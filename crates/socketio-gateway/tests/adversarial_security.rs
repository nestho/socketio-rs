//! Cross-crate adversarial security test suite (`S5-T06`):
//! CVE-2026-59724 (crafted SID), CVE-2026-59725 (streaming POST exhaustion), and CSWSH Origin bypasses.

use engineio_core::{validate_sid, StreamingPayloadGuard};
use socketio_server::validate_origin;

#[test]
fn adversarial_cve_2026_59724_crafted_sids_rejected() {
    let malformed = [
        "",
        "short",
        "../../etc/passwd",
        "sid\0nullbyte1234567",
        "sid with spaces 123456",
        &"a".repeat(65),
    ];
    for sid in malformed {
        assert!(validate_sid(sid).is_err(), "SID should be rejected: {sid:?}");
    }
}

#[test]
fn adversarial_cve_2026_59725_streaming_payload_exhaustion_blocked() {
    let mut guard = StreamingPayloadGuard::new(1_000_000);
    for _ in 0..10 {
        assert!(guard.observe_chunk(100_000).is_ok());
    }
    assert!(guard.observe_chunk(1).is_err());
}

#[test]
fn adversarial_cswsh_origin_bypass_blocked() {
    let allowed = vec!["https://trusted.example.com".to_owned()];
    assert!(!validate_origin(
        Some("https://evil.example.com"),
        &allowed,
        true
    ));
    assert!(!validate_origin(None, &allowed, true));
}
