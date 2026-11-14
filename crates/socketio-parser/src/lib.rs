#![forbid(unsafe_code)]
//! `socketio-parser` provides zero-copy `SharedBytes` (`Arc<[u8]>`) framing for
//! Engine.IO v4 and Socket.IO v5 packets, `sonic-rs` SIMD JSON event parsing,
//! MsgPack encoding/decoding (`msgpack` feature), binary attachment reconstruction,
//! and bounded `permessage-deflate` compression with 100:1 decompression-bomb guards.

use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;

/// Zero-copy reference-counted byte slice (`O(1)` atomic clone across broadcast subscribers).
#[derive(Clone, Eq)]
pub struct SharedBytes {
    buf: Arc<[u8]>,
    start: usize,
    end: usize,
}

impl SharedBytes {
    pub fn from_slice(data: &[u8]) -> Self {
        let arc: Arc<[u8]> = Arc::from(data);
        let len = arc.len();
        Self {
            buf: arc,
            start: 0,
            end: len,
        }
    }

    pub fn from_vec(vec: Vec<u8>) -> Self {
        let len = vec.len();
        Self {
            buf: Arc::from(vec),
            start: 0,
            end: len,
        }
    }

    pub fn slice(&self, start_offset: usize, end_offset: usize) -> Self {
        assert!(start_offset <= end_offset && self.start + end_offset <= self.end);
        Self {
            buf: Arc::clone(&self.buf),
            start: self.start + start_offset,
            end: self.start + end_offset,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf[self.start..self.end]
    }
}

impl PartialEq for SharedBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Deref for SharedBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl std::fmt::Debug for SharedBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match std::str::from_utf8(self.as_slice()) {
            Ok(s) => write!(f, "SharedBytes({s:?})"),
            Err(_) => write!(f, "SharedBytes({:?})", self.as_slice()),
        }
    }
}

/// Socket.IO v5 packet types (`0`..=`6`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    Connect = 0,
    Disconnect = 1,
    Event = 2,
    Ack = 3,
    ConnectError = 4,
    BinaryEvent = 5,
    BinaryAck = 6,
}

impl PacketType {
    pub fn from_ascii_digit(b: u8) -> Result<Self, ParseError> {
        match b {
            b'0' => Ok(Self::Connect),
            b'1' => Ok(Self::Disconnect),
            b'2' => Ok(Self::Event),
            b'3' => Ok(Self::Ack),
            b'4' => Ok(Self::ConnectError),
            b'5' => Ok(Self::BinaryEvent),
            b'6' => Ok(Self::BinaryAck),
            _ => Err(ParseError::InvalidPacketType(b)),
        }
    }
}

/// Zero-copy representation of a Socket.IO v5 frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketIoPacket {
    pub packet_type: PacketType,
    pub namespace: String,
    pub ack_id: Option<u64>,
    pub attachments: usize,
    pub payload: SharedBytes,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("empty packet frame")]
    EmptyFrame,
    #[error("invalid Socket.IO v5 packet type byte: {0}")]
    InvalidPacketType(u8),
    #[error("malformed packet header or UTF-8 sequence")]
    MalformedHeader,
    #[error("SIMD JSON / serde deserialization error: {0}")]
    JsonError(String),
    #[error("MsgPack serialization/deserialization error: {0}")]
    MsgPackError(String),
    #[error("decompression ratio ({ratio}:1) exceeds maximum allowed ({max_ratio}:1)")]
    DecompressionBombDetected { ratio: usize, max_ratio: usize },
    #[error("compression/decompression I/O failure")]
    CompressionFailed,
}

/// Validates compressed vs. decompressed frame sizes to prevent `permessage-deflate`
/// or `zstd` decompression bombs (default 100:1 limit).
pub fn verify_decompression_ratio(
    compressed_bytes: usize,
    decompressed_bytes: usize,
    max_ratio: usize,
) -> Result<(), ParseError> {
    if compressed_bytes == 0 {
        return Err(ParseError::EmptyFrame);
    }
    let ratio = decompressed_bytes / compressed_bytes;
    if ratio > max_ratio {
        return Err(ParseError::DecompressionBombDetected { ratio, max_ratio });
    }
    Ok(())
}

/// Compresses outbound payloads exceeding `threshold_bytes` (default 1 KiB) using
/// `permessage-deflate` (raw DEFLATE) to avoid CPU waste on small frames (`S2-T03`).
pub fn compress_if_above_threshold(
    payload: &[u8],
    threshold_bytes: usize,
) -> Result<(SharedBytes, bool), ParseError> {
    if payload.len() <= threshold_bytes {
        return Ok((SharedBytes::from_slice(payload), false));
    }
    let mut encoder = DeflateEncoder::new(Vec::with_capacity(payload.len() / 2), Compression::fast());
    encoder
        .write_all(payload)
        .map_err(|_| ParseError::CompressionFailed)?;
    let compressed = encoder.finish().map_err(|_| ParseError::CompressionFailed)?;
    Ok((SharedBytes::from_vec(compressed), true))
}

/// Decompresses a `permessage-deflate` frame while enforcing `max_ratio` (e.g., 100:1)
/// and `max_output_bytes` *during* streaming read to block zip bombs (`S2-T03`).
pub fn decompress_bounded(
    compressed: &[u8],
    max_ratio: usize,
    max_output_bytes: usize,
) -> Result<SharedBytes, ParseError> {
    if compressed.is_empty() {
        return Err(ParseError::EmptyFrame);
    }
    let hard_cap = compressed
        .len()
        .saturating_mul(max_ratio)
        .min(max_output_bytes);
    let mut decoder = DeflateDecoder::new(compressed);
    let mut out = Vec::with_capacity((compressed.len() * 2).min(hard_cap));
    let mut chunk = [0u8; 4096];

    loop {
        let n = decoder
            .read(&mut chunk)
            .map_err(|_| ParseError::CompressionFailed)?;
        if n == 0 {
            break;
        }
        if out.len().saturating_add(n) > hard_cap {
            let ratio = (out.len() + n) / compressed.len();
            return Err(ParseError::DecompressionBombDetected { ratio, max_ratio });
        }
        out.extend_from_slice(&chunk[..n]);
    }

    Ok(SharedBytes::from_vec(out))
}

/// Parses a Socket.IO JSON event array `["event_name", payload]` using `sonic-rs`
/// SIMD JSON acceleration (`S2-T01`), falling back cleanly for arbitrary values.
pub fn parse_event_simd<T: DeserializeOwned>(payload: &[u8]) -> Result<(String, T), ParseError> {
    let tuple: (String, T) =
        sonic_rs::from_slice(payload).map_err(|e| ParseError::JsonError(e.to_string()))?;
    Ok(tuple)
}

/// Parses an untyped dynamic JSON event array `["event_name", serde_json::Value]`.
pub fn parse_dynamic_event(payload: &[u8]) -> Result<(String, serde_json::Value), ParseError> {
    let val: serde_json::Value =
        serde_json::from_slice(payload).map_err(|e| ParseError::JsonError(e.to_string()))?;
    let arr = val
        .as_array()
        .ok_or_else(|| ParseError::JsonError("expected JSON array".to_owned()))?;
    let event = arr
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| ParseError::JsonError("missing event name string".to_owned()))?
        .to_owned();
    let data = arr.get(1).cloned().unwrap_or(serde_json::Value::Null);
    Ok((event, data))
}

/// Serializes a Socket.IO event `["event", data]` using `sonic-rs` SIMD JSON.
pub fn encode_event_simd<T: Serialize>(event: &str, data: &T) -> Result<SharedBytes, ParseError> {
    let bytes =
        sonic_rs::to_vec(&(event, data)).map_err(|e| ParseError::JsonError(e.to_string()))?;
    Ok(SharedBytes::from_vec(bytes))
}

/// Encodes and decodes Socket.IO packets in compact MsgPack format (`S2-T02`).
#[cfg(feature = "msgpack")]
pub mod msgpack {
    use super::*;

    pub fn encode_event<T: Serialize>(event: &str, data: &T) -> Result<SharedBytes, ParseError> {
        let bytes =
            rmp_serde::to_vec(&(event, data)).map_err(|e| ParseError::MsgPackError(e.to_string()))?;
        Ok(SharedBytes::from_vec(bytes))
    }

    pub fn decode_event<T: DeserializeOwned>(payload: &[u8]) -> Result<(String, T), ParseError> {
        rmp_serde::from_slice(payload).map_err(|e| ParseError::MsgPackError(e.to_string()))
    }
}

/// Reconstructs `BinaryEvent` (`5`) and `BinaryAck` (`6`) packets with their
/// trailing binary attachment frames (`S2-T02`).
#[derive(Debug, Default)]
pub struct BinaryAttachmentReconstructor {
    pending_packet: Option<SocketIoPacket>,
    collected: VecDeque<SharedBytes>,
}

impl BinaryAttachmentReconstructor {
    pub fn start(&mut self, packet: SocketIoPacket) -> Option<(SocketIoPacket, Vec<SharedBytes>)> {
        if packet.attachments == 0 {
            return Some((packet, Vec::new()));
        }
        self.collected.clear();
        self.pending_packet = Some(packet);
        None
    }

    pub fn push_attachment(
        &mut self,
        bin: SharedBytes,
    ) -> Option<(SocketIoPacket, Vec<SharedBytes>)> {
        let pkt = self.pending_packet.as_ref()?;
        self.collected.push_back(bin);
        if self.collected.len() >= pkt.attachments {
            let completed = self.pending_packet.take()?;
            let bins: Vec<SharedBytes> = self.collected.drain(..).collect();
            return Some((completed, bins));
        }
        None
    }
}

/// Encodes a Socket.IO v5 packet into an immutable, ref-counted `SharedBytes` buffer
/// suitable for O(1) zero-copy broadcast fan-out across thousands of room subscribers.
pub fn encode_v5_packet(packet: &SocketIoPacket) -> SharedBytes {
    let mut buf = Vec::with_capacity(32 + packet.payload.len());
    buf.push(b'0' + (packet.packet_type as u8));

    if matches!(
        packet.packet_type,
        PacketType::BinaryEvent | PacketType::BinaryAck
    ) {
        buf.extend_from_slice(packet.attachments.to_string().as_bytes());
        buf.push(b'-');
    }

    if !packet.namespace.is_empty() && packet.namespace != "/" {
        buf.extend_from_slice(packet.namespace.as_bytes());
        buf.push(b',');
    }

    if let Some(ack_id) = packet.ack_id {
        buf.extend_from_slice(ack_id.to_string().as_bytes());
    }

    buf.extend_from_slice(&packet.payload);
    SharedBytes::from_vec(buf)
}

/// Decodes a Socket.IO v5 text frame without copying the JSON payload slice.
pub fn decode_v5_packet(raw: SharedBytes) -> Result<SocketIoPacket, ParseError> {
    if raw.is_empty() {
        return Err(ParseError::EmptyFrame);
    }

    let packet_type = PacketType::from_ascii_digit(raw[0])?;
    let mut idx = 1;

    let mut attachments = 0usize;
    if matches!(
        packet_type,
        PacketType::BinaryEvent | PacketType::BinaryAck
    ) {
        let dash_pos = raw[idx..]
            .iter()
            .position(|&b| b == b'-')
            .ok_or(ParseError::MalformedHeader)?;
        let num_str =
            std::str::from_utf8(&raw[idx..idx + dash_pos]).map_err(|_| ParseError::MalformedHeader)?;
        attachments = num_str.parse().map_err(|_| ParseError::MalformedHeader)?;
        idx += dash_pos + 1;
    }

    let namespace = if idx < raw.len() && raw[idx] == b'/' {
        let comma_pos = raw[idx..]
            .iter()
            .position(|&b| b == b',')
            .unwrap_or(raw.len() - idx);
        let ns = std::str::from_utf8(&raw[idx..idx + comma_pos])
            .map_err(|_| ParseError::MalformedHeader)?
            .to_owned();
        idx += comma_pos;
        if idx < raw.len() && raw[idx] == b',' {
            idx += 1;
        }
        ns
    } else {
        "/".to_owned()
    };

    let ack_start = idx;
    while idx < raw.len() && raw[idx].is_ascii_digit() {
        idx += 1;
    }
    let ack_id = if idx > ack_start {
        let ack_str =
            std::str::from_utf8(&raw[ack_start..idx]).map_err(|_| ParseError::MalformedHeader)?;
        Some(ack_str.parse().map_err(|_| ParseError::MalformedHeader)?)
    } else {
        None
    };

    let payload = raw.slice(idx, raw.len());

    Ok(SocketIoPacket {
        packet_type,
        namespace,
        ack_id,
        attachments,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct ChatMsg {
        user: String,
        text: String,
    }

    #[test]
    fn test_roundtrip_v5_event_packet_zero_copy() {
        let pkt = SocketIoPacket {
            packet_type: PacketType::Event,
            namespace: "/chat".to_owned(),
            ack_id: Some(42),
            attachments: 0,
            payload: SharedBytes::from_slice(b"[\"message\",{\"hello\":\"world\"}]"),
        };

        let encoded = encode_v5_packet(&pkt);
        assert_eq!(&encoded[..], b"2/chat,42[\"message\",{\"hello\":\"world\"}]");

        let decoded = decode_v5_packet(encoded).expect("valid v5 frame");
        assert_eq!(decoded, pkt);
    }

    #[test]
    fn test_simd_json_event_roundtrip() {
        let msg = ChatMsg {
            user: "nestho".to_owned(),
            text: "rust fast".to_owned(),
        };
        let encoded = encode_event_simd("chat", &msg).expect("simd encode");
        let (event, decoded): (String, ChatMsg) =
            parse_event_simd(&encoded).expect("simd decode");
        assert_eq!(event, "chat");
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_msgpack_and_binary_attachment_reconstruction() {
        let msg = ChatMsg {
            user: "nestho".to_owned(),
            text: "bin".to_owned(),
        };
        let packed = msgpack::encode_event("bin_ev", &msg).expect("msgpack encode");
        let (ev, decoded): (String, ChatMsg) =
            msgpack::decode_event(&packed).expect("msgpack decode");
        assert_eq!(ev, "bin_ev");
        assert_eq!(decoded, msg);

        let mut recon = BinaryAttachmentReconstructor::default();
        let pkt = SocketIoPacket {
            packet_type: PacketType::BinaryEvent,
            namespace: "/".to_owned(),
            ack_id: None,
            attachments: 2,
            payload: SharedBytes::from_slice(b"[\"upload\",{\"_placeholder\":true,\"num\":0}]"),
        };
        assert!(recon.start(pkt).is_none());
        assert!(recon.push_attachment(SharedBytes::from_slice(b"chunk1")).is_none());
        let (_done_pkt, bins) = recon
            .push_attachment(SharedBytes::from_slice(b"chunk2"))
            .expect("2 attachments complete packet");
        assert_eq!(bins.len(), 2);
    }

    #[test]
    fn test_bounded_deflate_and_zip_bomb_protection() {
        let large_repetitive = vec![b'A'; 4096];
        let (compressed, did_compress) =
            compress_if_above_threshold(&large_repetitive, 1024).expect("compress");
        assert!(did_compress);
        assert!(compressed.len() < 200);

        // Normal ratio (e.g. 500x) fails when max_ratio is 100
        let err = decompress_bounded(&compressed, 100, 1_000_000).unwrap_err();
        assert!(matches!(err, ParseError::DecompressionBombDetected { .. }));

        // Higher ratio cap succeeds
        let restored = decompress_bounded(&compressed, 1000, 1_000_000).expect("decompress");
        assert_eq!(restored.as_slice(), &large_repetitive[..]);
    }
}
