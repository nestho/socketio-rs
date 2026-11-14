//! Adversarial fuzzing regression suite for `socketio-parser` (`S5-T05`).

use socketio_parser::{
    compress_if_above_threshold, decode_v5_packet, decompress_bounded, verify_decompression_ratio,
    ParseError, SharedBytes,
};

#[test]
fn adversarial_fuzz_v5_parser_never_panics_on_malformed_bytes() {
    let mut seed: u64 = 0xDEAD_BEEF_CAFE_BABE;
    for len in 0..512 {
        let mut buf = Vec::with_capacity(len);
        for _ in 0..len {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            buf.push((seed >> 56) as u8);
        }
        // Must return Ok or Err cleanly — NEVER panic
        let _ = decode_v5_packet(SharedBytes::from_vec(buf));
    }
}

#[test]
fn adversarial_decompression_bomb_blocked() {
    let bomb_raw = vec![b'Z'; 10_240];
    let (compressed, _) = compress_if_above_threshold(&bomb_raw, 100).unwrap();
    let err = decompress_bounded(&compressed, 50, 1_000_000).unwrap_err();
    assert!(matches!(err, ParseError::DecompressionBombDetected { .. }));
    assert!(verify_decompression_ratio(10, 2_000, 100).is_err());
}
