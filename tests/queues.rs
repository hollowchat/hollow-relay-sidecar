use hollow_relay_sidecar::protocol::{
    validate_packet, Outbox, Outgoing, LOSSY_BUFFER, RELIABLE_BUFFER,
};
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::mpsc;

#[test]
fn lossy_queue_drops_without_growing_or_closing() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let out = Outbox {
        tx,
        pending: AtomicUsize::new(0),
        closing: AtomicBool::new(false),
    };
    for _ in 0..10000 {
        out.send(Arc::from("x".repeat(900)), true);
    }
    assert!(out.pending.load(Ordering::Relaxed) <= LOSSY_BUFFER);
    assert!(!out.closing.load(Ordering::Relaxed));
    let mut count = 0;
    while rx.try_recv().is_ok() {
        count += 1;
    }
    assert!(count <= LOSSY_BUFFER / 900);
}

#[test]
fn reliable_queue_closes_once_and_stays_bounded() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let out = Outbox {
        tx,
        pending: AtomicUsize::new(0),
        closing: AtomicBool::new(false),
    };
    for _ in 0..10000 {
        out.send(Arc::from("x".repeat(16000)), false);
    }
    assert!(out.pending.load(Ordering::Relaxed) <= RELIABLE_BUFFER);
    assert!(out.closing.load(Ordering::Relaxed));
    let mut close = 0;
    while let Ok(value) = rx.try_recv() {
        if matches!(value, Outgoing::Close(1013, _)) {
            close += 1;
        }
    }
    assert_eq!(close, 1);
}

#[test]
fn malformed_base64_and_unknown_lanes_fail_closed() {
    let base = json!({"protocol":"hollow-realtime/1","scopeId":"room","sessionId":"s","sequence":0,"epoch":1,"createdAt":1000,"expiresAt":1100,"lane":"voice","payloadBase64":"AQ=="});
    assert!(validate_packet(&base, "room", 1000).is_ok());
    for bad in ["", "====", "!@#$", "AQ=", "你好"] {
        let mut value = base.clone();
        value["payloadBase64"] = json!(bad);
        assert!(validate_packet(&value, "room", 1000).is_err());
    }
}
