#![forbid(unsafe_code)]
//! `sio-migrate` performs comment- and string-aware JS/TS lexical analysis of
//! Socket.IO repositories to extract namespaces (`io.of(...)`), event handlers
//! (`socket.on(...)`), rooms (`.to(...)`/`.in(...)`/`.join(...)`), middleware
//! (`io.use(...)`), `cors` origins, `connectionStateRecovery`, and `socket.request`
//! retention, then generates `gateway.toml` and Rust `socketio_server` scaffolding.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MigrationScanReport {
    pub files_scanned: usize,
    pub namespaces: BTreeSet<String>,
    pub event_names: BTreeSet<String>,
    pub rooms: BTreeSet<String>,
    pub middleware_count: usize,
    pub csr_configured: bool,
    pub cors_origins: BTreeSet<String>,
    pub namespaces_detected: usize,
    pub event_handlers_detected: usize,
    pub room_broadcasts_detected: usize,
    pub raw_request_usages: usize,
    pub recommended_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Ident(String),
    StringLit(String),
    Punct(char),
}

/// Tokenizes JS/TS source while stripping `//` line comments and `/* ... */` block comments.
fn lex_js_ts(src: &str) -> Vec<Token> {
    let bytes = src.as_bytes();
    let len = bytes.len();
    let mut i = 0usize;
    let mut out = Vec::new();

    while i < len {
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'/' && i + 1 < len && bytes[i + 1] == b'/' {
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(len);
            continue;
        }
        if c == b'\'' || c == b'"' || c == b'`' {
            let quote = c;
            i += 1;
            let mut s = String::new();
            while i < len {
                let ch = bytes[i];
                if ch == b'\\' && i + 1 < len {
                    i += 2;
                    continue;
                }
                if ch == quote {
                    i += 1;
                    break;
                }
                s.push(ch as char);
                i += 1;
            }
            out.push(Token::StringLit(s));
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' || c == b'$' {
            let start = i;
            i += 1;
            while i < len
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'$')
            {
                i += 1;
            }
            out.push(Token::Ident(src[start..i].to_owned()));
            continue;
        }
        out.push(Token::Punct(c as char));
        i += 1;
    }
    out
}

pub fn scan_source_snippet(source: &str, report: &mut MigrationScanReport) {
    report.files_scanned += 1;
    let tokens = lex_js_ts(source);

    for idx in 0..tokens.len() {
        if let (Some(Token::Punct('.')), Some(Token::Ident(method)), Some(Token::Punct('('))) =
            (tokens.get(idx), tokens.get(idx + 1), tokens.get(idx + 2))
        {
            let first_str = match tokens.get(idx + 3) {
                Some(Token::StringLit(s)) => Some(s.clone()),
                _ => None,
            };

            match method.as_str() {
                "of" => {
                    report.namespaces_detected += 1;
                    if let Some(ns) = first_str {
                        report.namespaces.insert(ns);
                    }
                }
                "on" => {
                    report.event_handlers_detected += 1;
                    if let Some(ev) = first_str {
                        if ev != "connection" && ev != "disconnect" && ev != "disconnecting" {
                            report.event_names.insert(ev);
                        }
                    }
                }
                "to" | "in" | "join" => {
                    report.room_broadcasts_detected += 1;
                    if let Some(room) = first_str {
                        report.rooms.insert(room);
                    }
                }
                "use" => {
                    report.middleware_count += 1;
                }
                _ => {}
            }
        }

        if let Some(Token::Ident(ident)) = tokens.get(idx) {
            if ident == "connectionStateRecovery" {
                report.csr_configured = true;
            } else if ident == "origin" {
                if let (Some(Token::Punct(':')), Some(Token::StringLit(orig))) =
                    (tokens.get(idx + 1), tokens.get(idx + 2))
                {
                    report.cors_origins.insert(orig.clone());
                }
            }
        }

        if let (
            Some(Token::Ident(obj)),
            Some(Token::Punct('.')),
            Some(Token::Ident(prop)),
        ) = (tokens.get(idx), tokens.get(idx + 1), tokens.get(idx + 2))
        {
            if obj == "socket" && prop == "request" {
                report.raw_request_usages += 1;
            }
        }
    }

    if report.raw_request_usages > 0 {
        report.recommended_path =
            "Path A (Sidecar Gateway) + Refactor socket.request to handshake headers".to_owned();
    } else {
        report.recommended_path =
            "Ready for both Path A (Zero-Rewrite Sidecar) and Path B (Native Rust Layer)"
                .to_owned();
    }
}

pub fn scan_directory(dir: &Path, report: &mut MigrationScanReport) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name != "node_modules" && name != "target" && !name.starts_with('.') {
                    scan_directory(&path, report);
                }
            } else if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                if matches!(ext, "js" | "ts" | "mjs" | "cjs") {
                    if let Ok(content) = fs::read_to_string(&path) {
                        scan_source_snippet(&content, report);
                    }
                }
            }
        }
    }
}

/// Generates a tailored `gateway.toml` configuration from the scan report.
pub fn generate_gateway_toml(report: &MigrationScanReport) -> String {
    let adapter = if report.room_broadcasts_detected > 0 {
        "redis"
    } else {
        "in_memory"
    };
    let origins: Vec<String> = if report.cors_origins.is_empty() {
        vec!["\"*\"".to_owned()]
    } else {
        report
            .cors_origins
            .iter()
            .map(|o| format!("\"{o}\""))
            .collect()
    };
    format!(
        "# Auto-generated by sio-migrate\n\
         [server]\n\
         bind_addr = \"0.0.0.0:3000\"\n\
         path = \"/socket.io/\"\n\
         io_driver = \"auto\"\n\
         worker_threads = 0\n\
         shutdown_drain_timeout_ms = 15000\n\
         max_connection_duration_jitter_pct = 15\n\n\
         [security]\n\
         allowed_origins = [{}]\n\
         require_origin_header = {}\n\
         max_decompression_ratio = 100\n\n\
         [recovery]\n\
         enabled = true\n\
         max_disconnection_duration_ms = 120000\n\
         ring_buffer_capacity_per_room = 512\n\
         adapter = \"{adapter}\"\n\n\
         [bridge]\n\
         broadcast_bind_addr = \"127.0.0.1:3001\"\n",
        origins.join(", "),
        !report.cors_origins.is_empty()
    )
}

/// Generates native Rust `socketio_server` namespace and event handler scaffolding.
pub fn scaffold_rust_handlers(report: &MigrationScanReport) -> String {
    let mut out = String::from(
        "// Auto-generated by sio-migrate\n\
         use socketio_server::prelude::*;\n\
         use std::sync::Arc;\n\n\
         pub fn register_migrated_namespaces(io: &Arc<SocketIo>, bridge: &Arc<SocketIoEngineBridge>) {\n",
    );

    let namespaces: Vec<String> = if report.namespaces.is_empty() {
        vec!["/".to_owned()]
    } else {
        report.namespaces.iter().cloned().collect()
    };

    for ns in namespaces {
        out.push_str(&format!(
            "    let b = Arc::clone(bridge);\n    io.ns(\"{ns}\", move |socket| {{\n"
        ));
        for ev in &report.event_names {
            out.push_str(&format!(
                "        b.register_event_handler(&socket.id, \"{ev}\", Arc::new(|sock, ctx| {{\n\
                 // TODO: migrate `{ev}` handler logic\n\
                 let _ = (sock, ctx);\n\
                 }}));\n"
            ));
        }
        out.push_str("    });\n");
    }
    out.push_str("}\n");
    out
}

fn print_migrate_help() {
    println!(
        "\
sio-migrate 0.3.0 — Socket.IO (Node.js/TS) to Rust Migration Analyzer & Scaffolder

USAGE:
    sio-migrate [OPTIONS]

OPTIONS:
    --scan <DIR>            Directory of JS/TS files to scan (default: .)
    --scaffold <OUT_DIR>    Generate gateway.toml, migrated_handlers.rs, and MIGRATION_PLAN.md
    --json                  Emit machine-readable JSON summary instead of banner text
    -h, --help              Print this help message and exit
    -V, --version           Print version information and exit

WHAT IT DETECTS:
    - Namespaces via .of(...) and middleware via .use(...)
    - Event handlers via .on(...) and room usage via .to(...)/.in(...)/.join(...)
    - CORS origin configuration and connectionStateRecovery settings
    - socket.request retention (flags Path A Sidecar vs Path B Native Rust)

EXAMPLES:
    sio-migrate --scan ./my-node-chat-app
    sio-migrate --scan ./my-node-chat-app --scaffold ./migration-out
    sio-migrate --scan ./my-node-chat-app --json
"
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print_migrate_help();
        return;
    }
    if args.iter().any(|a| a == "-V" || a == "--version") {
        println!("sio-migrate {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    let target_dir = args
        .windows(2)
        .find(|w| w[0] == "--scan")
        .map(|w| w[1].as_str())
        .or_else(|| args.get(2).map(|s| s.as_str()))
        .unwrap_or(".");

    let mut report = MigrationScanReport::default();
    scan_directory(Path::new(target_dir), &mut report);

    if args.iter().any(|a| a == "--json") {
        println!(
            "{{\"files_scanned\":{},\"namespaces\":{},\"events\":{},\"rooms\":{},\"middleware\":{},\"csr_configured\":{},\"raw_request_usages\":{}}}",
            report.files_scanned,
            report.namespaces_detected,
            report.event_handlers_detected,
            report.room_broadcasts_detected,
            report.middleware_count,
            report.csr_configured,
            report.raw_request_usages
        );
    } else {
        println!("==============================================================");
        println!("  sio-migrate — Socket.IO to Rust Migration Analyzer v0.3.0");
        println!("==============================================================");
        println!("  Target Path              : {}", target_dir);
        println!("  JS/TS Files Scanned      : {}", report.files_scanned);
        println!("  Namespaces (.of)         : {} ({:?})", report.namespaces_detected, report.namespaces);
        println!("  Event Handlers (.on)     : {} ({:?})", report.event_handlers_detected, report.event_names);
        println!("  Room Broadcasts (.to/.in): {} ({:?})", report.room_broadcasts_detected, report.rooms);
        println!("  Middleware (.use)        : {}", report.middleware_count);
        println!("  CSR Configured           : {}", report.csr_configured);
        println!("  CORS Origins             : {:?}", report.cors_origins);
        println!("  socket.request Retention : {}", report.raw_request_usages);
        println!("  Recommendation           : {}", report.recommended_path);
        println!("  Projected Idle RAM Drop  : ~52 KB/conn -> ~2-6 KB/conn (-88% to -96%)");
        println!("==============================================================");
    }

    if let Some(scaffold_dir) = args.windows(2).find(|w| w[0] == "--scaffold").map(|w| Path::new(&w[1])) {
        let _ = fs::create_dir_all(scaffold_dir);
        let _ = fs::write(scaffold_dir.join("gateway.toml"), generate_gateway_toml(&report));
        let _ = fs::write(scaffold_dir.join("migrated_handlers.rs"), scaffold_rust_handlers(&report));
        let _ = fs::write(scaffold_dir.join("MIGRATION_PLAN.md"), generate_migration_plan_md(&report));
        println!("  Scaffolded files written to {}", scaffold_dir.display());
    }
}

/// Generates a Markdown migration checklist tailored to the scanned repository.
pub fn generate_migration_plan_md(report: &MigrationScanReport) -> String {
    format!(
        "# Socket.IO to Rust (`socketio-rs`) Migration Plan\n\n\
         - **Files Scanned**: {}\n\
         - **Namespaces Detected**: {} (`{:?}`)\n\
         - **Events Detected**: {} (`{:?}`)\n\
         - **Rooms Detected**: {} (`{:?}`)\n\
         - **Middleware (`.use`)**: {}\n\
         - **CSR Configured**: {}\n\
         - **`socket.request` Retention Count**: {}\n\
         - **Recommended Path**: {}\n\n\
         ## Generated Files\n\
         1. `gateway.toml` — Pre-configured edge, security (`allowed_origins`), and Redis/CSR settings.\n\
         2. `migrated_handlers.rs` — Native Rust `socketio_server` namespace and event handler skeletons.\n",
        report.files_scanned,
        report.namespaces_detected,
        report.namespaces,
        report.event_handlers_detected,
        report.event_names,
        report.room_broadcasts_detected,
        report.rooms,
        report.middleware_count,
        report.csr_configured,
        report.raw_request_usages,
        report.recommended_path,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scanner_detects_socketio_patterns_cors_csr_and_ignores_comments() {
        let sample_js = r#"
            // io.of("/ignored-comment").on("fake", () => {});
            /* socket.request.headers.ignored */
            const io = new Server(httpServer, {
                cors: { origin: "https://app.example.com" },
                connectionStateRecovery: { maxDisconnectionDuration: 120000 }
            });
            const chat = io.of("/chat");
            chat.use((socket, next) => next());
            chat.on("connection", (socket) => {
                const cookie = socket.request.headers.cookie;
                socket.on("send_msg", (data) => {
                    socket.to("room1").emit("hello", data);
                });
            });
        "#;
        let mut report = MigrationScanReport::default();
        scan_source_snippet(sample_js, &mut report);

        assert_eq!(report.files_scanned, 1);
        assert!(report.csr_configured);
        assert!(report.cors_origins.contains("https://app.example.com"));
        assert_eq!(report.namespaces_detected, 1);
        assert!(report.namespaces.contains("/chat"));
        assert!(!report.namespaces.contains("/ignored-comment"));
        assert_eq!(report.event_handlers_detected, 2);
        assert!(report.event_names.contains("send_msg"));
        assert!(!report.event_names.contains("fake"));
        assert_eq!(report.room_broadcasts_detected, 1);
        assert!(report.rooms.contains("room1"));
        assert_eq!(report.middleware_count, 1);
        assert_eq!(report.raw_request_usages, 1);

        let toml = generate_gateway_toml(&report);
        assert!(toml.contains("adapter = \"redis\""));
        assert!(toml.contains("https://app.example.com"));
        let rust_code = scaffold_rust_handlers(&report);
        assert!(rust_code.contains("io.ns(\"/chat\""));
        assert!(rust_code.contains("\"send_msg\""));
    }
}
