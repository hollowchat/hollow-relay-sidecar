use indexmap::IndexMap;
use secp256k1::{ecdsa::Signature, Message, PublicKey, Secp256k1};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;

pub const PROTOCOL: &str = "hollow-realtime/1";
pub const RELIABLE_BUFFER: usize = 2 * 1024 * 1024;
pub const LOSSY_BUFFER: usize = 256 * 1024;
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Clone)]
pub struct Limits {
    pub signed: bool,
    pub connections: usize,
    pub scopes: usize,
    pub members: usize,
    pub frame: usize,
    pub messages: usize,
    pub bytes: usize,
    pub recipients: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            signed: true,
            connections: 10_000,
            scopes: 8,
            members: 5_000,
            frame: 65536,
            messages: 240,
            bytes: 768 * 1024,
            recipients: 64,
        }
    }
}
pub enum Outgoing {
    Text(Arc<str>),
    Close(u16, &'static str),
}
pub struct Outbox {
    pub tx: mpsc::UnboundedSender<Outgoing>,
    pub pending: AtomicUsize,
    pub closing: AtomicBool,
}
impl Outbox {
    pub fn close(&self, code: u16, reason: &'static str) {
        if !self.closing.swap(true, Ordering::Relaxed) {
            let _ = self.tx.send(Outgoing::Close(code, reason));
        }
    }
    pub fn send(&self, text: Arc<str>, lossy: bool) -> bool {
        if self.closing.load(Ordering::Relaxed) {
            return false;
        }
        let limit = if lossy { LOSSY_BUFFER } else { RELIABLE_BUFFER };
        let len = text.len();
        if self
            .pending
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                n.checked_add(len).filter(|v| *v <= limit)
            })
            .is_err()
        {
            if !lossy {
                self.close(1013, "slow reliable consumer");
            }
            return false;
        }
        if self.tx.send(Outgoing::Text(text)).is_err() {
            self.pending.fetch_sub(len, Ordering::Relaxed);
            return false;
        }
        true
    }
}
fn frame(kind: &str, body: Value) -> Arc<str> {
    Arc::from(json!([kind, body]).to_string())
}
fn send(out: &Outbox, kind: &str, body: Value) {
    out.send(frame(kind, body), false);
}
#[derive(Clone)]
struct Join {
    peer: String,
    identity: String,
    metadata: Option<Value>,
}
#[derive(Clone)]
struct Member {
    connection: String,
    join: Join,
    position: Option<[f64; 3]>,
    out: Arc<Outbox>,
}
struct Connection {
    joins: IndexMap<String, Join>,
    out: Arc<Outbox>,
    tokens: f64,
    bytes: f64,
    last: Instant,
}
pub struct Engine {
    pub limits: Limits,
    pub relay_id: String,
    pub label: String,
    sockets: HashMap<String, Connection>,
    rooms: IndexMap<String, IndexMap<String, Member>>,
    nonces: HashMap<String, u64>,
    pub ingress: u64,
    pub egress: u64,
    pub dropped: u64,
}
impl Engine {
    pub fn new(limits: Limits, label: String) -> Self {
        Self {
            limits,
            label,
            relay_id: uuid::Uuid::new_v4().simple().to_string(),
            sockets: HashMap::new(),
            rooms: IndexMap::new(),
            nonces: HashMap::new(),
            ingress: 0,
            egress: 0,
            dropped: 0,
        }
    }
    pub fn connect(&mut self, id: String, out: Arc<Outbox>) -> bool {
        if self.sockets.len() >= self.limits.connections {
            out.close(1013, "relay at capacity");
            return false;
        }
        send(
            &out,
            "WELCOME",
            json!({"relayId":self.relay_id,"connectionId":id,"label":self.label,"protocol":PROTOCOL,"signedJoinsRequired":self.limits.signed}),
        );
        self.sockets.insert(
            id,
            Connection {
                joins: IndexMap::new(),
                out,
                tokens: self.limits.messages as f64,
                bytes: self.limits.bytes as f64,
                last: Instant::now(),
            },
        );
        true
    }
    pub fn snapshot(&self) -> Value {
        json!({"relayId":self.relay_id,"label":self.label,"rooms":self.rooms.len(),"connections":self.sockets.len(),"memberships":self.rooms.values().map(|r|r.len()).sum::<usize>(),"largestRoomMembers":self.rooms.values().map(|r|r.len()).max().unwrap_or(0),"realtimeIngressPackets":self.ingress,"realtimeEgressPackets":self.egress,"realtimeDroppedPackets":self.dropped,"protocol":PROTOCOL,"signedJoinsRequired":self.limits.signed,"maxRealtimeRecipients":self.limits.recipients})
    }
    pub fn stop(&self) {
        for state in self.sockets.values() {
            state.out.close(1001, "relay shutting down");
        }
    }
    pub fn disconnect(&mut self, id: &str) {
        let rooms = self
            .sockets
            .get(id)
            .map(|s| s.joins.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for room in rooms {
            self.leave(id, &room);
        }
        self.sockets.remove(id);
    }
    fn leave(&mut self, id: &str, room: &str) {
        let join = self
            .sockets
            .get_mut(id)
            .and_then(|s| s.joins.shift_remove(room));
        if let Some(join) = join {
            if let Some(members) = self.rooms.get_mut(room) {
                if members.get(&join.peer).is_some_and(|m| m.connection == id) {
                    members.shift_remove(&join.peer);
                    for member in members.values() {
                        send(
                            &member.out,
                            "PEER_LEFT",
                            json!({"roomId":room,"peerId":join.peer,"identityPublicKey":join.identity}),
                        );
                    }
                }
                if members.is_empty() {
                    self.rooms.shift_remove(room);
                }
            }
        }
    }
    pub fn receive(&mut self, id: &str, raw: &[u8]) {
        let result = self.handle(id, raw);
        if let Err(message) = result {
            if let Some(s) = self.sockets.get(id) {
                send(&s.out, "ERROR", json!({"message":message}));
            }
        }
    }
    fn handle(&mut self, id: &str, raw: &[u8]) -> Result<(), String> {
        let state = self.sockets.get_mut(id).ok_or("Connection is detached")?;
        if raw.len() > self.limits.frame {
            return Err("Room relay frame is too large".into());
        }
        let elapsed = state.last.elapsed().as_secs_f64();
        state.last = Instant::now();
        state.tokens =
            (state.tokens + elapsed * self.limits.messages as f64).min(self.limits.messages as f64);
        state.bytes =
            (state.bytes + elapsed * self.limits.bytes as f64).min(self.limits.bytes as f64);
        if state.tokens < 1. || state.bytes < raw.len() as f64 {
            return Err("Room relay rate limit exceeded".into());
        }
        state.tokens -= 1.;
        state.bytes -= raw.len() as f64;
        let value: Value = serde_json::from_slice(raw).map_err(|_| "Invalid room relay frame")?;
        let array = value.as_array().ok_or("Invalid room relay frame")?;
        let kind = array
            .first()
            .and_then(Value::as_str)
            .ok_or("Invalid room relay frame")?;
        let body = array.get(1).cloned().unwrap_or(Value::Null);
        let out = state.out.clone();
        match kind {
            "PING" => send(&out, "PONG", json!({"now":now()})),
            "JOIN" => {
                if let Err(message) = self.join(id, &body) {
                    send(
                        &out,
                        "ERROR",
                        json!({"roomId":text(&body,"roomId").trim(),"message":message}),
                    );
                }
            }
            "LEAVE" => {
                let room = text(&body, "roomId").trim();
                if room.is_empty() {
                    return Err("roomId is required".into());
                }
                self.leave(id, room);
            }
            "UPDATE" => {
                let room = text(&body, "roomId").trim();
                let metadata = metadata(&body)?;
                let join = self
                    .sockets
                    .get_mut(id)
                    .and_then(|s| s.joins.get_mut(room))
                    .ok_or_else(|| format!("Not joined to room {room}"))?;
                join.metadata = metadata.clone();
                let member = self
                    .rooms
                    .get_mut(room)
                    .and_then(|r| r.get_mut(&join.peer))
                    .ok_or("Not joined to room")?;
                member.join.metadata = metadata.clone();
                member.position = position(metadata.as_ref());
            }
            "DIRECT" | "BROADCAST" => {
                let room = text(&body, "roomId").trim();
                if room.is_empty() {
                    return Err("roomId is required".into());
                }
                let join = self
                    .sockets
                    .get(id)
                    .and_then(|s| s.joins.get(room))
                    .ok_or_else(|| format!("Not joined to room {room}"))?;
                let members = self.rooms.get(room).ok_or("Room does not exist")?;
                let content = json!({"roomId":room,"fromPeerId":join.peer,"fromIdentityPublicKey":join.identity,"payload":body.get("payload").unwrap_or(&Value::Null)});
                let encoded = frame(kind, content);
                if kind == "DIRECT" {
                    let to = text(&body, "toPeerId").trim();
                    if to.is_empty() {
                        return Err("roomId and toPeerId are required".into());
                    }
                    members
                        .get(to)
                        .ok_or_else(|| format!("Peer {to} is not in room {room}"))?
                        .out
                        .send(encoded, false);
                } else {
                    for member in members.values() {
                        if member.connection != id || body["includeSelf"] == true {
                            member.out.send(encoded.clone(), false);
                        }
                    }
                }
            }
            "REALTIME" => self.realtime(id, &body)?,
            "ROOMS" => {
                let rooms = self.sockets[id].joins.keys().map(|room|json!({"roomId":room,"peers":self.rooms.get(room).map(|members|members.values().map(peer).collect::<Vec<_>>()).unwrap_or_default()})).collect::<Vec<_>>();
                send(&out, "ROOMS", json!({"rooms":rooms}));
            }
            _ => return Err(format!("Unsupported room relay frame: {kind}")),
        }
        Ok(())
    }
    fn join(&mut self, id: &str, body: &Value) -> Result<(), String> {
        let room = text(body, "roomId").trim();
        if room.is_empty() || room.encode_utf16().count() > 192 {
            return Err("roomId is required".into());
        }
        let raw_peer = text(body, "peerId").trim();
        let peer_id = if raw_peer.is_empty() { id } else { raw_peer };
        if peer_id.encode_utf16().count() > 192 {
            return Err("peerId is too long".into());
        }
        let state = self.sockets.get(id).ok_or("Connection is detached")?;
        if !state.joins.contains_key(room) && state.joins.len() >= self.limits.scopes {
            return Err("Too many joined scopes".into());
        }
        let identity = verify_auth(
            &body["authorization"],
            room,
            peer_id,
            self.limits.signed,
            &mut self.nonces,
            now(),
        )?;
        let metadata = metadata(body)?;
        if let Some(members) = self.rooms.get(room) {
            if !members.contains_key(peer_id) && members.len() >= self.limits.members {
                return Err("Room is at capacity".into());
            }
            if let Some(previous) = members.get(peer_id).cloned() {
                if previous.connection != id {
                    if previous.join.identity != identity {
                        return Err("Peer id is already owned by another identity".into());
                    }
                    send(
                        &previous.out,
                        "DISCONNECTED",
                        json!({"roomId":room,"reason":"peer-replaced"}),
                    );
                    self.disconnect(&previous.connection);
                }
            }
        }
        let join = Join {
            peer: peer_id.to_string(),
            identity,
            metadata,
        };
        let out = self.sockets[id].out.clone();
        self.sockets
            .get_mut(id)
            .unwrap()
            .joins
            .insert(room.to_string(), join.clone());
        let members = self.rooms.entry(room.to_string()).or_default();
        members.insert(
            peer_id.to_string(),
            Member {
                connection: id.to_string(),
                position: position(join.metadata.as_ref()),
                join: join.clone(),
                out: out.clone(),
            },
        );
        let peers = members
            .values()
            .filter(|m| m.connection != id)
            .map(peer)
            .collect::<Vec<_>>();
        send(
            &out,
            "JOINED",
            json!({"roomId":room,"peerId":join.peer,"identityPublicKey":join.identity,"peers":peers}),
        );
        let mut joined = peer(members.get(peer_id).unwrap());
        joined["roomId"] = json!(room);
        for member in members.values() {
            if member.connection != id {
                send(&member.out, "PEER_JOINED", joined.clone());
            }
        }
        Ok(())
    }
    fn realtime(&mut self, id: &str, body: &Value) -> Result<(), String> {
        let room = text(body, "roomId").trim();
        let join = self
            .sockets
            .get(id)
            .and_then(|s| s.joins.get(room))
            .ok_or_else(|| format!("Not joined to room {room}"))?;
        let members = self.rooms.get(room).ok_or("Room does not exist")?;
        let source = members.get(&join.peer).ok_or("Not joined to room")?;
        if source.connection != id {
            return Err("Not joined to room".into());
        }
        let mut packet = body["packet"].clone();
        let lossy = validate_packet(&packet, room, now())?;
        self.ingress += 1;
        let destinations = body["destinationPeerIds"].as_array().map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .take(self.limits.recipients)
                .collect::<Vec<_>>()
        });
        let radius = body["radius"].as_f64().map(|n| n.clamp(0., 10_000.));
        let max = body["maxRecipients"]
            .as_f64()
            .map(|n| n.floor().clamp(1., self.limits.recipients as f64) as usize)
            .unwrap_or(self.limits.recipients);
        let mut candidates = members
            .values()
            .filter(|m| m.connection != id)
            .filter(|m| {
                destinations
                    .as_ref()
                    .is_none_or(|d| d.contains(&m.join.peer.as_str()))
            })
            .collect::<Vec<_>>();
        if let Some(radius) = radius {
            candidates.retain(|m| {
                source
                    .position
                    .zip(m.position)
                    .is_some_and(|(a, b)| distance(a, b) <= radius * radius)
            });
            candidates.sort_by(|a, b| {
                distance(source.position.unwrap(), a.position.unwrap())
                    .total_cmp(&distance(source.position.unwrap(), b.position.unwrap()))
            });
        }
        packet["scopeId"] = json!(room);
        packet["senderId"] = json!(join.peer);
        let encoded = frame(
            "REALTIME",
            json!({"roomId":room,"fromPeerId":join.peer,"fromIdentityPublicKey":join.identity,"packet":packet}),
        );
        for target in candidates.into_iter().take(max) {
            if target.out.send(encoded.clone(), lossy) {
                self.egress += 1;
            } else {
                self.dropped += 1;
            }
        }
        Ok(())
    }
}
fn text<'a>(v: &'a Value, key: &str) -> &'a str {
    v[key].as_str().unwrap_or("")
}
fn peer(m: &Member) -> Value {
    let mut value = json!({"peerId":m.join.peer,"identityPublicKey":m.join.identity});
    if let Some(metadata) = &m.join.metadata {
        value["metadata"] = metadata.clone();
    }
    value
}
fn metadata(body: &Value) -> Result<Option<Value>, String> {
    let Some(value) = body.get("metadata").filter(|v| v.is_object()) else {
        return Ok(None);
    };
    if value.to_string().len() > 8192 {
        return Err("Room metadata is too large".into());
    }
    Ok(value
        .as_object()
        .filter(|o| !o.is_empty())
        .map(|_| value.clone()))
}
fn position(metadata: Option<&Value>) -> Option<[f64; 3]> {
    let p = &metadata?["position"];
    let point = [p["x"].as_f64()?, p["y"].as_f64()?, p["z"].as_f64()?];
    point
        .iter()
        .all(|n| n.is_finite() && n.abs() <= 10_000_000.)
        .then_some(point)
}
fn distance(a: [f64; 3], b: [f64; 3]) -> f64 {
    (a[0] - b[0]).powi(2) + (a[1] - b[1]).powi(2) + (a[2] - b[2]).powi(2)
}
fn scope(v: &str) -> bool {
    !v.is_empty() && !v.chars().any(|c| c <= '\u{1f}' || c == '\u{7f}')
}
fn safe_integer(v: &Value) -> Option<u64> {
    v.as_u64().filter(|n| *n <= 9_007_199_254_740_991)
}
pub fn verify_auth(
    auth: &Value,
    room: &str,
    peer: &str,
    signed: bool,
    nonces: &mut HashMap<String, u64>,
    now: u64,
) -> Result<String, String> {
    if auth.is_null() && !signed {
        return Ok(format!("legacy:{peer}"));
    }
    let public = text(auth, "publicKey");
    let signature = text(auth, "signature");
    let nonce = text(auth, "nonce");
    let created = safe_integer(&auth["createdAt"])
        .ok_or("Signed room authorization is invalid or expired")?;
    let expires = safe_integer(&auth["expiresAt"])
        .ok_or("Signed room authorization is invalid or expired")?;
    if auth["version"] != 1
        || auth["action"] != "join"
        || auth["roomId"] != room
        || auth["peerId"] != peer
        || !scope(room)
        || !scope(peer)
        || !scope(nonce)
        || nonce.encode_utf16().count() > 128
        || public.len() != 66
        || signature.len() != 128
        || expires <= created
        || created > now + 10_000
        || expires <= now
        || expires > created + 60_000
    {
        return Err("Signed room authorization is invalid or expired".into());
    }
    nonces.retain(|_, expiry| *expiry > now);
    let identity = public.to_lowercase();
    let key = format!("{identity}:{nonce}");
    if nonces.contains_key(&key) {
        return Err("Signed room authorization was replayed".into());
    }
    let pubkey = PublicKey::from_slice(
        &hex::decode(public).map_err(|_| "Signed room authorization failed verification")?,
    )
    .map_err(|_| "Signed room authorization failed verification")?;
    let sig = Signature::from_compact(
        &hex::decode(signature).map_err(|_| "Signed room authorization failed verification")?,
    )
    .map_err(|_| "Signed room authorization failed verification")?;
    let mut canonical = sig;
    canonical.normalize_s();
    if sig != canonical {
        return Err("Signed room authorization failed verification".into());
    }
    let payload = format!(
        "hollow-room-auth-v1\njoin\n{room}\n{peer}\n{identity}\n{nonce}\n{created}\n{expires}"
    );
    let digest: [u8; 32] = Sha256::digest(payload.as_bytes()).into();
    Secp256k1::verification_only()
        .verify_ecdsa(Message::from_digest(digest), &sig, &pubkey)
        .map_err(|_| "Signed room authorization failed verification")?;
    nonces.insert(key, expires);
    Ok(identity)
}
pub fn validate_packet(p: &Value, room: &str, now: u64) -> Result<bool, String> {
    let lane = text(p, "lane");
    let (limit, deadline, lossy) = match lane {
        "voice" => (900, 140, true),
        "game-input" => (900, 100, true),
        "game-snapshot" => (900, 180, true),
        "game-critical" => (14336, 5000, false),
        "bulk" => (14336, 30000, false),
        _ => return Err("Unsupported realtime packet".into()),
    };
    if p["protocol"] != PROTOCOL {
        return Err("Unsupported realtime packet".into());
    }
    if p["scopeId"] != room
        || text(p, "sessionId").is_empty()
        || text(p, "sessionId").encode_utf16().count() > 192
    {
        return Err("Realtime scope is invalid".into());
    }
    if safe_integer(&p["sequence"]).is_none() || safe_integer(&p["epoch"]).is_none_or(|n| n < 1) {
        return Err("Realtime sequence is invalid".into());
    }
    let created = safe_integer(&p["createdAt"]).ok_or("Realtime timestamps are invalid")?;
    let expires = safe_integer(&p["expiresAt"]).ok_or("Realtime timestamps are invalid")?;
    if expires <= now || expires > created + deadline {
        return Err("Realtime packet is stale".into());
    }
    let data = p["payloadBase64"]
        .as_str()
        .ok_or("Realtime payload exceeds its lane budget")?;
    let padding = if data.ends_with("==") {
        2
    } else if data.ends_with('=') {
        1
    } else {
        0
    };
    if data.is_empty()
        || data.len() % 4 != 0
        || !data[..data.len().saturating_sub(padding)]
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'+' || c == b'/')
        || data.len() / 4 * 3 - padding > limit
    {
        return Err("Realtime payload exceeds its lane budget".into());
    }
    Ok(lossy)
}
