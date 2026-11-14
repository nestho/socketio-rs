#![forbid(unsafe_code)]
//! `socketio-gateway` is the standalone polyglot sidecar gateway binary.

use engineio_core::{AdaptiveDriver, EngineConfig, IoDriver, SocketTuningProfile};
use socketio_server::ShardedRoomIndex;

fn main() {
    let config = EngineConfig::default();
    let driver = AdaptiveDriver::probe(true, 0, SocketTuningProfile::default());
    let caps = driver.capabilities();
    let cores = driver.shard_count();
    let rooms = ShardedRoomIndex::new(cores);

    println!("==============================================================");
    println!("  socketio-rs Standalone Polyglot Gateway v0.1.0");
    println!("==============================================================");
    println!("  Selected IoDriver        : {}", driver.driver_name());
    println!(
        "  Driver Capabilities      : multishot_accept={}, buf_rings={}, notsent_lowat={}",
        caps.multishot_accept, caps.provided_buffer_rings, caps.tcp_notsent_lowat
    );
    println!("  Detected Worker Shards   : {} (64-byte cache-line aligned)", cores);
    println!(
        "  Write Watermarks         : {} KiB high / {} KiB low",
        config.write_high_watermark_bytes / 1024,
        config.write_low_watermark_bytes / 1024
    );
    println!("  Max Payload Guard        : {} bytes (streaming)", config.max_payload_bytes);
    println!("  Active Broadcast Count   : {}", rooms.total_broadcasts());
    println!("==============================================================");
}
