//! Official Socket.IO v4/v5 protocol compliance integration suite (`S3-T01`, `S3-T02`).

use engineio_core::{EngineConfig, HandshakePacket, TransportKind, UpgradeFsm, UpgradeState};
use socketio_adapters::{RecoveryManager, SessionRecoverySnapshot};
use socketio_parser::{
    decode_v5_packet, encode_event_simd, encode_v5_packet, BinaryAttachmentReconstructor,
    PacketType, SharedBytes, SocketIoPacket,
};
use socketio_server::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::test]
async fn e2e_engineio_v4_handshake_upgrade_and_socketio_v5_multiplexing() {
    // 1. Engine.IO v4 Open Handshake
    let cfg = EngineConfig::default();
    let hs = HandshakePacket {
        sid: "FSDjX-WRwSA4zTZMALqx".to_owned(),
        upgrades: vec![TransportKind::WebSocket],
        ping_interval: cfg.ping_interval_ms,
        ping_timeout: cfg.ping_timeout_ms,
        max_payload: cfg.max_payload_bytes,
    };
    assert!(hs.to_engineio_open_frame().starts_with("0{\"sid\":\"FSDjX-WRwSA4zTZMALqx\""));

    // 2. Engine.IO v4 Polling -> WebSocket Probe & Commit FSM
    let mut fsm = UpgradeFsm::new_upgrading_from_polling();
    assert_eq!(fsm.on_control_packet("2probe"), Ok(Some("3probe")));
    assert_eq!(fsm.on_control_packet("5"), Ok(None));
    assert_eq!(fsm.state, UpgradeState::Upgraded(TransportKind::WebSocket));

    // 3. Socket.IO v5 Namespace Multiplexing, Rooms & Zero-Copy Broadcast
    let io = SocketIo::new(4);
    let (tx, mut rx) = mpsc::channel(16);
    let sock = SocketRef::new(hs.sid.clone(), "/admin", Arc::clone(&io.rooms), tx);
    sock.join("ops");

    let payload = encode_event_simd("alert", &"high_load").unwrap();
    let frame = encode_v5_packet(&SocketIoPacket {
        packet_type: PacketType::Event,
        namespace: "/admin".to_owned(),
        ack_id: None,
        attachments: 0,
        payload,
    });
    assert_eq!(io.rooms.broadcast_zero_copy("ops", &frame), 1);

    let received = rx.recv().await.expect("socket should receive frame");
    let decoded = decode_v5_packet(received).expect("valid v5 frame");
    assert_eq!(decoded.namespace, "/admin");
    assert_eq!(decoded.packet_type, PacketType::Event);

    // 4. Binary Attachment Reconstruction (PacketType::BinaryEvent)
    let mut recon = BinaryAttachmentReconstructor::default();
    let bin_pkt = SocketIoPacket {
        packet_type: PacketType::BinaryEvent,
        namespace: "/admin".to_owned(),
        ack_id: Some(9),
        attachments: 1,
        payload: SharedBytes::from_slice(b"[\"blob\",{\"_placeholder\":true,\"num\":0}]"),
    };
    assert!(recon.start(bin_pkt).is_none());
    let (completed, bins) = recon
        .push_attachment(SharedBytes::from_slice(b"\x01\x02\x03\x04"))
        .unwrap();
    assert_eq!(completed.ack_id, Some(9));
    assert_eq!(bins[0].as_slice(), b"\x01\x02\x03\x04");

    // 5. Acknowledgement Correlation & Connection State Recovery
    let acks = AckTable::new(16);
    let (ack_id, ack_rx) = acks.register().unwrap();
    assert!(acks.resolve(ack_id, SharedBytes::from_slice(b"[\"ack_ok\"]")));
    let ack_res = acks
        .wait_with_timeout(ack_id, ack_rx, Duration::from_millis(50))
        .await
        .unwrap();
    assert_eq!(ack_res.as_slice(), b"[\"ack_ok\"]");

    let csr = RecoveryManager::new(64, 120_000);
    let o1 = csr.record_outbound("ops", frame.clone(), 1_000);
    csr.save_snapshot(SessionRecoverySnapshot {
        pid: "pid-e2e-001".to_owned(),
        sid: hs.sid.clone(),
        namespace: "/admin".to_owned(),
        rooms: vec!["ops".to_owned()],
        last_offset: o1,
        disconnected_at_ms: 1_200,
    });
    let _o2 = csr.record_outbound("ops", frame, 1_500);
    let outcome = csr.try_recover("pid-e2e-001", o1, 2_000).unwrap();
    assert!(outcome.recovered);
    assert_eq!(outcome.missed_frames.len(), 1);
}
