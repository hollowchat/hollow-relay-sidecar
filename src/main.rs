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
    app.refresh.notify_one();
    (StatusCode::ACCEPTED, Json(app.status().await)).into_response()
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
    let accepted = app.engine.lock().unwrap().connect(id.clone(), out.clone());
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
                    Some(Ok(Message::Text(text)))=>app.engine.lock().unwrap().receive(&id,text.as_bytes()),
                    Some(Ok(Message::Binary(bytes)))=>app.engine.lock().unwrap().receive(&id,&bytes),
                    Some(Ok(Message::Close(_)))|None|Some(Err(_))=>break,
                    _=>{},
                }
            }
        }
    }
    app.engine.lock().unwrap().disconnect(&id);
    writer.abort();
}

async fn tunnel(
    app: Arc<App>,
    helper: PathBuf,
    upstream: Option<String>,
    cloudflared: Option<PathBuf>,
    native: bool,
) {
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
        let ready=tokio::time::timeout(Duration::from_secs(180),async {
            while let Ok(Some(line))=lines.next_line().await {
                let value = if native {let Ok(value)=serde_json::from_str::<Value>(&line) else {continue};value} else {
                    let Some(start) = line.find("https://") else {continue};
                    let url: String = line[start..].chars().take_while(|c|c.is_ascii_alphanumeric() || matches!(c,'-'|'.'|':'|'/')).collect();
                    if !url.ends_with(".trycloudflare.com") {continue}
                    json!({"type":"ready","publicUrl":url})
                };
                if value["type"]=="error" {return Err(value["message"].to_string())}
                if value["type"]=="ready" {
                    let url=value["publicUrl"].as_str().ok_or("Missing tunnel URL")?.trim_end_matches('/').to_string();
                    if !url.starts_with("https://") {return Err("Tunnel must use HTTPS".into())}
                    let ws=format!("{}/relay",url.replacen("https:","wss:",1));
                    *app.public.write().await = json!({"running":false,"starting":true,"candidateUrl":url});
                    // A helper's ready line is not proof that the public websocket works.
                    probe_public(&ws).await?;
                    return Ok(json!({"running":true,"healthy":true,"provider":"cloudflare","publicUrl":url,"publicWsUrl":ws,"startedAt":now()}));
                }
            }Err("Tunnel exited before ready".into())
        }).await;
        let mut registration = None;
        match ready {
            Ok(Ok(value)) => {
                *app.public.write().await = value;
                if let Some(upstream) = &upstream {
                    registration = Some(tokio::spawn(register(app.clone(), upstream.clone())));
                }
            }
            error => {
                eprintln!("[hollow-native-relay] tunnel not ready: {error:?}");
                *app.public.write().await = json!({"running":false,"error":format!("{error:?}")});
                let _ = child.kill().await;
                if native && cloudflared.is_some() {
                    native = false;
                }
            }
        }
        let drain =
            tokio::spawn(async move { while matches!(lines.next_line().await, Ok(Some(_))) {} });
        tokio::select! {_=child.wait()=>{},_=app.refresh.notified()=>{let _=child.kill().await;}}
        drain.abort();
        if let Some(task) = registration {
            task.abort();
        }
        {
            let mut status = app.public.write().await;
            status["running"] = json!(false);
            status["healthy"] = json!(false);
        }
        *app.registration.write().await = json!({"registered":false});
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
async fn probe_public(ws: &str) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            let (mut probe, _) = tokio_tungstenite::connect_async(ws)
                .await
                .map_err(|e| e.to_string())?;
            let hello = probe
                .next()
                .await
                .ok_or("Tunnel closed")?
                .map_err(|e| e.to_string())?;
            let frame: Value = serde_json::from_str(hello.to_text().map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            let _ = probe.close(None).await;
            if frame[0] != "WELCOME" {
                return Err("Public relay did not welcome probe".to_string());
            }
            Ok(())
        })
        .await;
        if matches!(result, Ok(Ok(()))) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("Public tunnel probe failed: {result:?}"));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
async fn register(app: Arc<App>, upstream: String) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    loop {
        let result=async {
            let session:Value=client.post(format!("{}/plugins/hollow-relay/relays/sessions",upstream.trim_end_matches('/'))).json(&json!({"label":app.engine.lock().unwrap().label})).send().await.map_err(|e|e.to_string())?.error_for_status().map_err(|e|e.to_string())?.json().await.map_err(|e|e.to_string())?;
            let advertised=session["relayWebSocketUrl"].as_str().ok_or("Missing registration websocket")?;
            let websocket = argument("--upstream-ws").unwrap_or_else(||advertised.to_string());
            let (mut socket,_)=tokio_tungstenite::connect_async(&websocket).await.map_err(|e|e.to_string())?;
            let mut interval=tokio::time::interval(Duration::from_secs(20));let mut registered=false;
            loop {tokio::select! {
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
            }}
            #[allow(unreachable_code)] Ok::<(),String>(())
        }.await;
        *app.registration.write().await = json!({"registered":false,"error":result.err()});
        tokio::time::sleep(Duration::from_secs(10)).await;
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
                argument("--upstream"),
                cloudflared,
                native_helper.is_some(),
            ))
        });
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
    Ok(())
}
