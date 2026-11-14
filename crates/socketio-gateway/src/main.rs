#![forbid(unsafe_code)]
//! `socketio-gateway` is the standalone polyglot sidecar gateway binary.

use engineio_core::{detect_optimal_driver, EngineConfig};
use socketio_server::ShardedRoomIndex;

fn main() {
    let config = EngineConfig::default();
    let driver = detect_optimal_driver(true);
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let rooms = ShardedRoomIndex::new(cores);

    println!("==============================================================");
    println!("  socketio-rs Standalone Polyglot Gateway v0.1.0");
    println!("==============================================================");
    println!("  Selected IoDriver        : {:?}", driver);
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
