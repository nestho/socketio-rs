//! Ergonomic Axum-style handler extractors (`Data<T>`, `TryData<T>`, `AckSender`, `Bin`, `State<S>`).

use serde::de::DeserializeOwned;
use serde::Serialize;
use socketio_parser::{encode_ack_simd, encode_v5_packet, PacketType, SharedBytes, SocketIoPacket};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Context passed into event handlers for extracting typed arguments.
#[derive(Debug, Clone)]
pub struct EventContext {
    pub namespace: String,
    pub ack_id: Option<u64>,
    pub payload: SharedBytes,
    pub binaries: Vec<SharedBytes>,
    pub outbound: mpsc::Sender<SharedBytes>,
}

/// Extracts and deserializes a typed payload `T` from a Socket.IO event (`S2-T06`).
#[derive(Debug, Clone)]
pub struct Data<T>(pub T);

/// Fallible extractor returning `Result<T, String>` instead of dropping malformed events.
#[derive(Debug, Clone)]
pub struct TryData<T>(pub Result<T, String>);

/// Extracts binary attachments from `BinaryEvent` (`5`) or `BinaryAck` (`6`) packets.
#[derive(Debug, Clone)]
pub struct Bin(pub Vec<SharedBytes>);

/// Extracts shared application state (`Arc<S>`).
#[derive(Debug, Clone)]
pub struct State<S>(pub Arc<S>);

/// Sends an acknowledgement packet back to the originating client.
#[derive(Debug, Clone)]
pub struct AckSender {
    namespace: String,
    ack_id: Option<u64>,
    outbound: mpsc::Sender<SharedBytes>,
}

impl AckSender {
    pub fn from_context(ctx: &EventContext) -> Self {
        Self {
            namespace: ctx.namespace.clone(),
            ack_id: ctx.ack_id,
            outbound: ctx.outbound.clone(),
        }
    }

    pub fn send<T: Serialize>(self, data: &T) -> bool {
        let Some(ack_id) = self.ack_id else {
            return false;
        };
        let Ok(payload) = encode_ack_simd(data) else {
            return false;
        };
        let pkt = SocketIoPacket {
            packet_type: PacketType::Ack,
            namespace: self.namespace,
            ack_id: Some(ack_id),
            attachments: 0,
            payload,
        };
        let frame = encode_v5_packet(&pkt);
        self.outbound.try_send(frame).is_ok()
    }
}

/// Trait implemented by all Axum-style event extractors.
pub trait FromEventContext: Sized {
    fn from_context(ctx: &EventContext) -> Result<Self, String>;
}

impl<T: DeserializeOwned> FromEventContext for Data<T> {
    fn from_context(ctx: &EventContext) -> Result<Self, String> {
        let (_ev, val): (String, T) =
            socketio_parser::parse_event_simd(&ctx.payload).map_err(|e| e.to_string())?;
        Ok(Data(val))
    }
}

impl<T: DeserializeOwned> FromEventContext for TryData<T> {
    fn from_context(ctx: &EventContext) -> Result<Self, String> {
        let parsed = socketio_parser::parse_event_simd::<T>(&ctx.payload)
            .map(|(_ev, v)| v)
            .map_err(|e| e.to_string());
        Ok(TryData(parsed))
    }
}

impl FromEventContext for Bin {
    fn from_context(ctx: &EventContext) -> Result<Self, String> {
        Ok(Bin(ctx.binaries.clone()))
    }
}

impl FromEventContext for AckSender {
    fn from_context(ctx: &EventContext) -> Result<Self, String> {
        Ok(AckSender::from_context(ctx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_and_try_data_extractors() {
        let (tx, _rx) = mpsc::channel(8);
        let ctx = EventContext {
            namespace: "/".to_owned(),
            ack_id: Some(7),
            payload: SharedBytes::from_slice(b"[\"ping\",{\"count\":42}]"),
            binaries: vec![],
            outbound: tx,
        };

        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Ping {
            count: u32,
        }

        let Data(ping) = Data::<Ping>::from_context(&ctx).expect("extract Data");
        assert_eq!(ping.count, 42);

        let TryData(res) = TryData::<Ping>::from_context(&ctx).expect("extract TryData");
        assert_eq!(res.unwrap().count, 42);
    }
}
