#![forbid(unsafe_code)]
//! `socketio-parser` provides zero-copy `SharedBytes` (`Arc<[u8]>`) framing for
//! Engine.IO v4 and Socket.IO v5 packets, alongside strict decompression-bomb ratio guards.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

/// Zero-copy reference-counted byte slice (`O(1)` atomic clone across broadcast subscribers).
#[derive(Clone, Eq)]
pub struct SharedBytes {
    buf: Arc<[u8]>,
    start: usize,
    end: usize,
}

impl PartialEq for SharedBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
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

impl Deref for SharedBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl fmt::Debug for SharedBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    EmptyFrame,
    InvalidPacketType(u8),
    MalformedHeader,
    DecompressionBombDetected { ratio: usize, max_ratio: usize },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyFrame => write!(f, "empty packet frame"),
            Self::InvalidPacketType(b) => write!(f, "invalid Socket.IO v5 packet type byte: {b}"),
            Self::MalformedHeader => write!(f, "malformed packet header or UTF-8 sequence"),
            Self::DecompressionBombDetected { ratio, max_ratio } => write!(
                f,
                "decompression ratio ({ratio}:1) exceeds maximum allowed ({max_ratio}:1)"
            ),
        }
    }
}

impl std::error::Error for ParseError {}

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
    fn test_decompression_bomb_guard() {
        assert!(verify_decompression_ratio(100, 5_000, 100).is_ok());
        assert_eq!(
            verify_decompression_ratio(100, 15_000, 100),
            Err(ParseError::DecompressionBombDetected {
                ratio: 150,
                max_ratio: 100
            })
        );
    }
}
