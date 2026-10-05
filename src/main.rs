use axum::{
    extract::{
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use hollow_relay_sidecar::protocol::{now, Engine, Limits, Outbox, Outgoing};
use serde_json::{json, Value};
use std::{
    env,
    error::Error,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    sync::{mpsc, Notify, RwLock},
};
mod child_job;

struct App {
    engine: Mutex<Engine>,
    http: String,
    ws: String,
    token: String,
    started: u64,
    public: RwLock<Value>,
    registration: RwLock<Value>,
    enabled: AtomicBool,
    hosting_changed: Notify,
    refresh: Notify,
    shutdown: Notify,
}
fn config_number(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}
fn argument(name: &str) -> Option<String> {
    env::args()
        .skip(1)
        .collect::<Vec<_>>()
        .windows(2)
        .find(|a| a[0] == name)
        .map(|a| a[1].clone())
}
impl App {
    async fn status(&self) -> Value {
        let mut value = self.engine.lock().unwrap().snapshot();
        value["ok"] = json!(true);
        value["pid"] = json!(std::process::id());
        value["localHttpUrl"] = json!(self.http);
        value["localWsUrl"] = json!(self.ws);
        value["startedAt"] = json!(self.started);
        value["lastSeenAt"] = json!(now());
        value["healthy"] = json!(true);
        value["enabled"] = json!(self.enabled.load(Ordering::SeqCst));
        let public = self.public.read().await.clone();
        let visible = if public["running"] == true {
            public.clone()
        } else {
            Value::Null
        };
        value["publicUrl"] = visible
            .get("publicUrl")
            .cloned()
            .unwrap_or_else(|| json!(self.http));
        value["publicWsUrl"] = visible
            .get("publicWsUrl")
            .cloned()
            .unwrap_or_else(|| json!(self.ws));
        value["provider"] = json!(if public["running"] == true {
            "cloudflare"
        } else {
            "local"
        });
        value["tunnel"] = public;
        value["registration"] = self.registration.read().await.clone();
        value
    }
    fn authorized(&self, headers: &HeaderMap) -> bool {
        headers
            .get("x-hollow-relay-service-token")
            .and_then(|v| v.to_str().ok())
            == Some(self.token.as_str())
    }
}
async fn health(State(app): State<Arc<App>>) -> Json<Value> {
    let mut value = app.engine.lock().unwrap().snapshot();
    value["ok"] = json!(true);
    value["localHttpUrl"] = json!(app.http);
    value["localWsUrl"] = json!(app.ws);
    value["wsPath"] = json!("/relay");
    value["hosting"] = json!({"tunnel":app.public.read().await.clone(),"registered":app.registration.read().await["registered"] == true});
    Json(value)
}
async fn service(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !app.authorized(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"ok":false,"error":"Invalid relay service token"})),
        )
            .into_response();
    }
    Json(app.status().await).into_response()
}
async fn refresh(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !app.authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    app.engine.lock().unwrap().stop();
    *app.public.write().await = json!({"running":false,"phase":"resetting"});
    *app.registration.write().await = json!({"registered":false});
    app.hosting_changed.notify_one();
    app.refresh.notify_one();
    (StatusCode::ACCEPTED, Json(app.status().await)).into_response()
}
async fn set_enabled(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if !app.authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(enabled) = body["enabled"].as_bool() else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let changed = {
        let engine = app.engine.lock().unwrap();
        let changed = app.enabled.swap(enabled, Ordering::SeqCst) != enabled;
        if !enabled {
            engine.stop();
        }
        changed
    };
    if changed {
        *app.registration.write().await = json!({"registered":false});
        app.hosting_changed.notify_one();
    }
    Json(app.status().await).into_response()
}
async fn shutdown(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !app.authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    app.shutdown.notify_one();
    Json(json!({"ok":true})).into_response()
}
async fn upgrade(State(app): State<Arc<App>>, ws: WebSocketUpgrade) -> Response {
    let engine = app.engine.lock().unwrap();
    if engine.snapshot()["connections"].as_u64().unwrap_or(0) >= engine.limits.connections as u64 {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let limit = engine.limits.frame;
    drop(engine);
    ws.max_message_size(limit)
        .max_frame_size(limit)
        .on_upgrade(move |socket| connection(app, socket))
}
async fn connection(app: Arc<App>, socket: WebSocket) {
    let id = uuid::Uuid::new_v4().simple().to_string();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let out = Arc::new(Outbox {
        tx,
        pending: AtomicUsize::new(0),
        closing: AtomicBool::new(false),
    });
    let accepted = {
        let mut engine = app.engine.lock().unwrap();
        app.enabled
            .load(Ordering::SeqCst)
            .then(|| engine.connect(id.clone(), out.clone()))
    };
    let Some(accepted) = accepted else {
        let mut socket = socket;
        let _ = socket
            .send(Message::Text(
                json!(["HOLLOW_RELAY_PAUSED", {}]).to_string().into(),
            ))
            .await;
        let _ = socket
            .send(Message::Close(Some(CloseFrame {
                code: 1013,
                reason: "Relay paused".into(),
            })))
            .await;
        return;
    };
    let (mut sink, mut stream) = socket.split();
    let writer_out = out.clone();
    let mut writer = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            match message {
                Outgoing::Text(text) => {
                    let result = tokio::time::timeout(
                        Duration::from_secs(5),
                        sink.send(Message::Text(text.to_string().into())),
                    )
                    .await;
                    writer_out.pending.fetch_sub(text.len(), Ordering::Relaxed);
                    if !matches!(result, Ok(Ok(()))) {
                        break;
                    }
                }
                Outgoing::Close(code, reason) => {
                    let _ = sink
                        .send(Message::Close(Some(CloseFrame {
                            code,
                            reason: reason.into(),
                        })))
                        .await;
                    break;
                }
            }
        }
    });
    if accepted {
        loop {
            tokio::select! {
                _=&mut writer=>break,
                message=stream.next()=>match message {
                    Some(Ok(Message::Text(text)))=>{let mut engine=app.engine.lock().unwrap();if app.enabled.load(Ordering::SeqCst){engine.receive(&id,text.as_bytes());}},
                    Some(Ok(Message::Binary(bytes)))=>{let mut engine=app.engine.lock().unwrap();if app.enabled.load(Ordering::SeqCst){engine.receive(&id,&bytes);}},
                    Some(Ok(Message::Close(_)))|None|Some(Err(_))=>break,
                    _=>{},
                }
            }
        }
    }
    app.engine.lock().unwrap().disconnect(&id);
    writer.abort();
}

async fn tunnel(app: Arc<App>, helper: PathBuf, cloudflared: Option<PathBuf>, native: bool) {
    let mut native = native;
    loop {
        let binary = if native {
            &helper
        } else {
            cloudflared.as_ref().unwrap_or(&helper)
        };
        let mut command = Command::new(binary);
        if native {
            command.arg("--origin").arg(&app.ws);
        } else {
            command.args([
                "tunnel",
                "--url",
                &app.http,
                "--no-autoupdate",
                "--protocol",
                "http2",
            ]);
        }
        command
            .kill_on_drop(true)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(if native {
                std::process::Stdio::inherit()
            } else {
                std::process::Stdio::piped()
            });
        #[cfg(windows)]
        command.creation_flags(0x08000000 | 0x00000004);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                *app.public.write().await = json!({"running":false,"error":error.to_string()});
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue;
            }
        };
        #[cfg(windows)]
        let _job = match child_job::attach_and_resume(&child) {
            Ok(job) => job,
            Err(error) => {
                let _ = child.kill().await;
                *app.public.write().await =
                    json!({"running":false,"error":format!("Tunnel job setup failed: {error}")});
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue;
            }
        };
        let pipe: Box<dyn tokio::io::AsyncRead + Unpin + Send> = if native {
            Box::new(child.stdout.take().unwrap())
        } else {
            Box::new(child.stderr.take().unwrap())
        };
        let mut lines = BufReader::new(pipe).lines();
        let ready_future = tokio::time::timeout(Duration::from_secs(180), async {
            while let Ok(Some(line)) = lines.next_line().await {
                let value = if native {
                    let Ok(value) = serde_json::from_str::<Value>(&line) else {
                        continue;
                    };
                    value
                } else {
                    let Some(start) = line.find("https://") else {
                        continue;
                    };
                    let url: String = line[start..]
                        .chars()
                        .take_while(|c| {
                            c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | ':' | '/')
                        })
                        .collect();
                    if !url.ends_with(".trycloudflare.com") {
                        continue;
                    }
                    json!({"type":"ready","publicUrl":url})
                };
                if value["type"] == "progress" {
                    *app.public.write().await =
                        json!({"running":false,"phase":value["phase"],"since":now()});
                    continue;
                }
                if value["type"] == "error" {
                    return Err(value["message"].to_string());
                }
                if value["type"] == "ready" {
                    let url = value["publicUrl"]
                        .as_str()
                        .ok_or("Missing tunnel URL")?
                        .trim_end_matches('/')
                        .to_string();
                    if !url.starts_with("https://") {
                        return Err("Tunnel must use HTTPS".into());
                    }
                    let ws = format!("{}/relay", url.replacen("https:", "wss:", 1));
                    *app.public.write().await = json!({"running":false,"starting":true,"phase":"checking-public","candidateUrl":url,"since":now()});
                    // A helper's ready line is not proof that the public websocket works.
                    probe_public(&app, &ws).await?;
                    return Ok(
                        json!({"running":true,"healthy":true,"provider":"cloudflare","publicUrl":url,"publicWsUrl":ws,"startedAt":now()}),
                    );
                }
            }
            Err("Tunnel exited before ready".into())
        });
        let mut resetting = false;
        let ready = tokio::select! {
            value=ready_future=>value,
            _=app.refresh.notified()=>{
                resetting=true;
                Ok(Err("Tunnel reset requested".to_string()))
            }
        };
        match ready {
            Ok(Ok(value)) => {
                *app.public.write().await = value;
                app.hosting_changed.notify_one();
            }
            error => {
                eprintln!("[hollow-native-relay] tunnel not ready: {error:?}");
                *app.public.write().await = json!({"running":false,"error":format!("{error:?}")});
                let _ = child.kill().await;
                if !resetting && native && cloudflared.is_some() {
                    native = false;
                }
            }
        }
        let drain =
            tokio::spawn(async move { while matches!(lines.next_line().await, Ok(Some(_))) {} });
        if !resetting {
            tokio::select! {_=child.wait()=>{},_=app.refresh.notified()=>{resetting=true;let _=child.kill().await;}}
        } else {
            let _ = child.wait().await;
        }
        drain.abort();
        {
            let mut status = app.public.write().await;
            status["running"] = json!(false);
            status["healthy"] = json!(false);
        }
        *app.registration.write().await = json!({"registered":false});
        app.hosting_changed.notify_one();
        if !resetting {
            tokio::select! {_=tokio::time::sleep(Duration::from_secs(5))=>{},_=app.refresh.notified()=>{}}
        }
    }
}
async fn probe_public(app: &App, ws: &str) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            let mut probe = connect_public(ws).await?;
            let hello = probe
                .next()
                .await
                .ok_or("Tunnel closed")?
                .map_err(|e| e.to_string())?;
            let frame: Value = serde_json::from_str(hello.to_text().map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            let _ = probe.close(None).await;
            if frame[0] != "WELCOME" && frame[0] != "HOLLOW_RELAY_PAUSED" {
                return Err("Public relay did not welcome probe".to_string());
            }
            Ok(())
        })
        .await;
        if !matches!(&result, Ok(Ok(()))) {
            app.public.write().await["lastProbeError"] = json!(format!("{result:?}"));
        }
        if matches!(result, Ok(Ok(()))) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("Public tunnel probe failed: {result:?}"));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
async fn connect_public(
    ws: &str,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
> {
    let url = reqwest::Url::parse(ws).map_err(|e| e.to_string())?;
    let host = url.host_str().ok_or("Missing tunnel hostname")?;
    if url.scheme() != "wss" || !host.ends_with(".trycloudflare.com") {
        return tokio_tungstenite::connect_async(ws)
            .await
            .map(|(socket, _)| socket)
            .map_err(|e| e.to_string());
    }
    // Query authoritative-provider DNS first for fresh quick-tunnel names.
    // Looking them up before publication can poison the OS negative cache.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .map_err(|e| e.to_string())?;
    let answer: Value = client
        .get("https://cloudflare-dns.com/dns-query")
        .query(&[("name", host), ("type", "A")])
        .header("accept", "application/dns-json")
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    let mut last_error = "Tunnel DNS is not published yet".to_string();
    for address in public_addresses(&answer) {
        let tcp = match tokio::time::timeout(
            Duration::from_secs(3),
            tokio::net::TcpStream::connect((address, url.port_or_known_default().unwrap_or(443))),
        )
        .await
        {
            Ok(Ok(tcp)) => tcp,
            error => {
                last_error = format!("{error:?}");
                continue;
            }
        };
        // Keep the original URL: TLS verifies the hostname and sends its SNI,
        // and the websocket HTTP upgrade keeps the original Host header.
        match tokio_tungstenite::client_async_tls_with_config(ws, tcp, None, None).await {
            Ok((socket, _)) => return Ok(socket),
            Err(error) => last_error = error.to_string(),
        }
    }
    Err(last_error)
}
fn public_addresses(answer: &Value) -> Vec<std::net::Ipv4Addr> {
    if answer["Status"] != 0 {
        return Vec::new();
    }
    answer["Answer"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entry| entry["type"] == 1)
        .filter_map(|entry| entry["data"].as_str()?.parse::<std::net::Ipv4Addr>().ok())
        .filter(|ip| {
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_unspecified()
                && !ip.is_multicast()
        })
        .take(4)
        .collect()
}
#[cfg(test)]
mod tunnel_dns_tests {
    use super::*;
    #[test]
    fn accepts_only_successful_public_a_answers() {
        let data = json!({"Status":0,"Answer":[{"type":1,"data":"104.16.230.132"},{"type":5,"data":"alias.example"},{"type":1,"data":"127.0.0.1"},{"type":1,"data":"10.0.0.1"},{"type":1,"data":"invalid"}]});
        assert_eq!(
            public_addresses(&data),
            vec!["104.16.230.132".parse::<std::net::Ipv4Addr>().unwrap()]
        );
        assert!(public_addresses(
            &json!({"Status":3,"Answer":[{"type":1,"data":"104.16.230.132"}]})
        )
        .is_empty());
    }
}
async fn register(app: Arc<App>, upstream: String) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    loop {
        if !app.enabled.load(Ordering::SeqCst) || app.public.read().await["running"] != true {
            app.hosting_changed.notified().await;
            continue;
        }
        let mut registered_session = None;
        let session_future = async {
            let session: Value = client
                .post(format!(
                    "{}/plugins/hollow-relay/relays/sessions",
                    upstream.trim_end_matches('/')
                ))
                .json(&json!({"label":app.engine.lock().unwrap().label}))
                .send()
                .await
                .map_err(|e| e.to_string())?
                .error_for_status()
                .map_err(|e| e.to_string())?
                .json()
                .await
                .map_err(|e| e.to_string())?;
            registered_session = Some(session.clone());
            let advertised = session["relayWebSocketUrl"]
                .as_str()
                .ok_or("Missing registration websocket")?;
            let websocket = argument("--upstream-ws").unwrap_or_else(|| advertised.to_string());
            let (mut socket, _) = tokio_tungstenite::connect_async(&websocket)
                .await
                .map_err(|e| e.to_string())?;
            let mut interval = tokio::time::interval(Duration::from_secs(20));
            let mut registered = false;
            loop {
                tokio::select! {
                    _=interval.tick()=>{
                        let status=app.status().await;
                        let mut body=status.clone();body["sessionId"]=session["sessionId"].clone();body["sessionToken"]=session["sessionToken"].clone();
                        body["relayPublicKey"]=Value::Null;
                        let kind=if registered {"HOLLOW_WS_RELAY_HEARTBEAT"}else{"HOLLOW_WS_RELAY_REGISTER"};
                        socket.send(tokio_tungstenite::tungstenite::Message::Text(json!([kind,body]).to_string().into())).await.map_err(|e|e.to_string())?;
                    },
                    message=socket.next()=>{
                        let message=message.ok_or("Registration socket closed")?.map_err(|e|e.to_string())?;
                        if message.is_close(){return Err("Registration socket closed".to_string())}
                        if let Ok(text)=message.to_text() {if let Ok(frame)=serde_json::from_str::<Value>(text) {
                            if frame[0]=="HOLLOW_WS_RELAY_REGISTER_ERROR" {return Err("Upstream rejected relay registration".into())}
                            if frame[0]=="HOLLOW_WS_RELAY_REGISTERED" || frame[0]=="HOLLOW_WS_RELAY_REGISTER_OK" {registered=true;*app.registration.write().await=json!({"registered":true,"upstream":upstream});}
                        }}
                    }
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), String>(())
        };
        let result = tokio::select! {
            result=session_future=>Some(result),
            _=app.hosting_changed.notified()=>None,
        };
        *app.registration.write().await = json!({"registered":false});
        // Closing the registration websocket alone retains a fresh catalog entry.
        // Explicitly retire its authenticated session when pausing or resetting.
        if let Some(session) = registered_session {
            if let (Some(id), Some(token)) = (
                session["sessionId"].as_str(),
                session["sessionToken"].as_str(),
            ) {
                let _ = client
                    .delete(format!(
                        "{}/plugins/hollow-relay/relays/sessions/{id}",
                        upstream.trim_end_matches('/')
                    ))
                    .header("x-hollow-relay-session-token", token)
                    .timeout(Duration::from_secs(3))
                    .send()
                    .await;
            }
        }
        let Some(result) = result else {
            continue;
        };
        *app.registration.write().await = json!({"registered":false,"error":result.err()});
        tokio::select! {_=tokio::time::sleep(Duration::from_secs(10))=>{},_=app.hosting_changed.notified()=>{}}
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    if env::args().any(|a| a == "--help") {
        println!("hollow-relay-sidecar [--port 0] [--label NAME] [--state-file PATH] [--tunnel-helper PATH] [--cloudflared PATH] [--upstream https://relay.example] [--exit-on-stdin-close]\nLocal signed-room relay. No tunnel or registration unless explicitly configured. Control token: HOLLOW_RELAY_CONTROL_TOKEN or private state file.");
        return Ok(());
    }
    let port = argument("--port")
        .unwrap_or_else(|| "0".into())
        .parse::<u16>()?;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    let http = format!("http://{}", listener.local_addr()?);
    let ws = format!("{}/relay", http.replacen("http:", "ws:", 1));
    let limits = Limits {
        signed: env::var("HOLLOW_RELAY_REQUIRE_SIGNED_JOINS")
            .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
            .unwrap_or(true),
        connections: config_number("HOLLOW_RELAY_MAX_CONNECTIONS", 10_000),
        scopes: config_number("HOLLOW_RELAY_MAX_SCOPES_PER_CONNECTION", 8),
        members: config_number("HOLLOW_RELAY_MAX_MEMBERS_PER_ROOM", 5_000),
        frame: config_number("HOLLOW_RELAY_MAX_FRAME_BYTES", 65536),
        messages: config_number("HOLLOW_RELAY_MESSAGES_PER_SECOND", 240),
        bytes: config_number("HOLLOW_RELAY_INGRESS_BYTES_PER_SECOND", 768 * 1024),
        recipients: config_number("HOLLOW_RELAY_MAX_REALTIME_RECIPIENTS", 64),
    };
    let app = Arc::new(App {
        engine: Mutex::new(Engine::new(
            limits,
            argument("--label").unwrap_or_else(|| "Hollow Native Relay".into()),
        )),
        http,
        ws,
        token: env::var("HOLLOW_RELAY_CONTROL_TOKEN").unwrap_or_else(|_| {
            format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            )
        }),
        started: now(),
        public: RwLock::new(Value::Null),
        registration: RwLock::new(json!({"registered":false})),
        enabled: AtomicBool::new(!env::args().any(|a| a == "--start-paused")),
        hosting_changed: Notify::new(),
        refresh: Notify::new(),
        shutdown: Notify::new(),
    });
    if let Some(path) = argument("--state-file") {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?
        }
        let contents = json!({"pid":std::process::id(),"token":app.token,"controlHttpUrl":app.http,"localWsUrl":app.ws,"startedAt":app.started});
        #[cfg(unix)]
        {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&path)?;
            file.write_all(contents.to_string().as_bytes())?;
        }
        #[cfg(windows)]
        std::fs::write(&path, contents.to_string())?;
    }
    let native_helper = argument("--tunnel-helper");
    let cloudflared = argument("--cloudflared").map(PathBuf::from);
    let supervisor = native_helper
        .clone()
        .or_else(|| argument("--cloudflared"))
        .map(|path| {
            tokio::spawn(tunnel(
                app.clone(),
                PathBuf::from(path),
                cloudflared,
                native_helper.is_some(),
            ))
        });
    let registration =
        argument("--upstream").map(|upstream| tokio::spawn(register(app.clone(), upstream)));
    if env::args().any(|a| a == "--exit-on-stdin-close") {
        let parent = app.clone();
        std::thread::Builder::new()
            .name("hollow-parent-pipe".into())
            .spawn(move || {
                use std::io::Read;
                let mut input = std::io::stdin();
                let mut bytes = [0u8; 64];
                while matches!(input.read(&mut bytes),Ok(n) if n>0) {}
                parent.shutdown.notify_one();
            })?;
    }
    println!(
        "{}",
        json!({"type":"ready","pid":std::process::id(),"localHttpUrl":app.http,"localWsUrl":app.ws,"signedJoinsRequired":app.engine.lock().unwrap().limits.signed})
    );
    let stop = app.clone();
    let router = Router::new()
        .route("/relay", get(upgrade))
        .route("/health", get(health))
        .route("/", get(health))
        .route("/info", get(health))
        .route("/service", get(service))
        .route("/refresh", post(refresh))
        .route("/enabled", post(set_enabled))
        .route("/shutdown", post(shutdown))
        .with_state(app.clone());
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            tokio::select! {_=tokio::signal::ctrl_c()=>{},_=stop.shutdown.notified()=>{}};
            stop.engine.lock().unwrap().stop();
        })
        .await?;
    if let Some(task) = supervisor {
        task.abort();
        let _ = task.await;
    }
    if let Some(task) = registration {
        task.abort();
        let _ = task.await;
    }
    Ok(())
}
