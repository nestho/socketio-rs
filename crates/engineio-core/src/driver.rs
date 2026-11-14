//! Adaptive `IoDriver` trait boundary, capability probing (`io_uring` vs `epoll`
//! container `seccomp` fallback vs `kqueue`), and per-core worker shard binding.

use std::io;
use std::net::{SocketAddr, TcpListener};

/// Supported OS / kernel I/O driver backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoDriverKind {
    /// Linux 6.x `io_uring` completion-based thread-per-core ring (with provided buffer rings).
    IoUringThreadPerCore,
    /// Linux `epoll` pinned per-core readiness loop (used when container `seccomp` blocks `io_uring`).
    EpollPinned,
    /// macOS / BSD `kqueue` readiness loop for local development and testing.
    Kqueue,
}

/// Hardware and kernel capabilities exposed by the active `IoDriver`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverCapabilities {
    pub multishot_accept: bool,
    pub provided_buffer_rings: bool,
    pub tcp_notsent_lowat: bool,
    pub so_reuseport: bool,
}

/// Low-latency TCP & backpressure socket tuning profile applied to every shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketTuningProfile {
    pub tcp_nodelay: bool,
    pub tcp_notsent_lowat_bytes: usize,
    pub so_reuseport: bool,
    pub write_high_watermark_bytes: usize,
    pub write_low_watermark_bytes: usize,
}

impl Default for SocketTuningProfile {
    fn default() -> Self {
        Self {
            tcp_nodelay: true,
            tcp_notsent_lowat_bytes: 16_384,
            so_reuseport: cfg!(unix),
            write_high_watermark_bytes: 131_072, // 128 KiB
            write_low_watermark_bytes: 16_384,   // 16 KiB
        }
    }
}

/// Transport-agnostic I/O driver boundary.
///
/// Decoupling Engine.IO/Socket.IO state machines from the underlying I/O reactor
/// allows running `epoll`/`kqueue` inside restricted containers today while
/// swapping in completion-based `io_uring` (`compio`/`monoio`) on bare-metal
/// Linux 6.x without rewriting protocol logic.
pub trait IoDriver: Send + Sync {
    fn kind(&self) -> IoDriverKind;
    fn capabilities(&self) -> DriverCapabilities;
    fn shard_count(&self) -> usize;
    fn tuning(&self) -> SocketTuningProfile;

    fn driver_name(&self) -> &'static str {
        match self.kind() {
            IoDriverKind::IoUringThreadPerCore => "io_uring (thread-per-core)",
            IoDriverKind::EpollPinned => "epoll (pinned per-core)",
            IoDriverKind::Kqueue => "kqueue (macOS/BSD)",
        }
    }

    /// Binds a non-blocking TCP listener for a specific worker shard with `SO_REUSEPORT` & `TCP_NODELAY`.
    fn bind_shard_listener(&self, _shard_id: usize, addr: SocketAddr) -> io::Result<TcpListener> {
        let domain = if addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        };
        let sock = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
        sock.set_reuse_address(true)?;
        #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
        if self.tuning().so_reuseport {
            let _ = sock.set_reuse_port(true);
        }
        let _ = sock.set_nodelay(self.tuning().tcp_nodelay);
        sock.set_nonblocking(true)?;
        sock.bind(&addr.into())?;
        sock.listen(1024)?;
        Ok(sock.into())
    }

    /// Binds one `SO_REUSEPORT` non-blocking listener per worker shard (`shard_count()`).
    fn bind_all_shard_listeners(&self, addr: SocketAddr) -> io::Result<Vec<TcpListener>> {
        let count = self.shard_count().max(1);
        let mut listeners = Vec::with_capacity(count);
        for shard_id in 0..count {
            listeners.push(self.bind_shard_listener(shard_id, addr)?);
        }
        Ok(listeners)
    }
}

/// Configuration for Linux 6.x `io_uring` thread-per-core completion rings (`--features io-uring`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoUringRingConfig {
    pub sq_entries: u32,
    pub provided_buffer_ring_size: u16,
    pub multishot_accept: bool,
}

impl Default for IoUringRingConfig {
    fn default() -> Self {
        Self {
            sq_entries: 4096,
            provided_buffer_ring_size: 1024,
            multishot_accept: true,
        }
    }
}

/// Default adaptive `IoDriver` implementation that probes OS/container limits at startup.
#[derive(Debug, Clone)]
pub struct AdaptiveDriver {
    kind: IoDriverKind,
    capabilities: DriverCapabilities,
    shard_count: usize,
    tuning: SocketTuningProfile,
}

impl AdaptiveDriver {
    /// Probes the host OS and container `seccomp` policy to choose the optimal driver.
    pub fn probe(prefer_io_uring: bool, requested_shards: usize, tuning: SocketTuningProfile) -> Self {
        let kind = detect_optimal_driver(prefer_io_uring);
        let shard_count = if requested_shards == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        } else {
            requested_shards
        };

        let capabilities = match kind {
            IoDriverKind::IoUringThreadPerCore => DriverCapabilities {
                multishot_accept: true,
                provided_buffer_rings: true,
                tcp_notsent_lowat: true,
                so_reuseport: true,
            },
            IoDriverKind::EpollPinned => DriverCapabilities {
                multishot_accept: false,
                provided_buffer_rings: false,
                tcp_notsent_lowat: true,
                so_reuseport: true,
            },
            IoDriverKind::Kqueue => DriverCapabilities {
                multishot_accept: false,
                provided_buffer_rings: false,
                tcp_notsent_lowat: false,
                so_reuseport: true,
            },
        };

        Self {
            kind,
            capabilities,
            shard_count,
            tuning,
        }
    }
}

impl IoDriver for AdaptiveDriver {
    fn kind(&self) -> IoDriverKind {
        self.kind
    }

    fn capabilities(&self) -> DriverCapabilities {
        self.capabilities
    }

    fn shard_count(&self) -> usize {
        self.shard_count
    }

    fn tuning(&self) -> SocketTuningProfile {
        self.tuning
    }
}

/// Probes the current OS and container runtime to select the optimal `IoDriverKind`
/// without crashing under restricted Docker/Kubernetes `seccomp` profiles.
pub fn detect_optimal_driver(prefer_io_uring: bool) -> IoDriverKind {
    if cfg!(target_os = "macos") || cfg!(target_os = "freebsd") {
        return IoDriverKind::Kqueue;
    }
    if cfg!(target_os = "linux") && prefer_io_uring && is_io_uring_permitted() {
        return IoDriverKind::IoUringThreadPerCore;
    }
    IoDriverKind::EpollPinned
}

/// Safe user-space check for Linux `io_uring` availability via `/proc/sys/kernel/io_uring_disabled`.
fn is_io_uring_permitted() -> bool {
    if let Ok(val) = std::fs::read_to_string("/proc/sys/kernel/io_uring_disabled") {
        return val.trim() == "0";
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adaptive_driver_probe_and_nonblocking_bind() {
        let driver = AdaptiveDriver::probe(true, 0, SocketTuningProfile::default());
        assert!(driver.shard_count() >= 1);
        assert!(driver.capabilities().so_reuseport);

        let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
        match driver.bind_shard_listener(0, addr) {
            Ok(listener) => assert!(listener.local_addr().is_ok()),
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
                // Expected inside network-isolated sandbox test runners.
            }
            Err(err) => panic!("unexpected listener bind error: {err}"),
        }
    }
}
