import { spawn } from "node:child_process";
import { io } from "socket.io-client";
import assert from "node:assert/strict";
import http from "node:http";

const EDGE_PORT = 3920;
const CONTROL_PORT = 3921;
const EDGE_URL = `http://127.0.0.1:${EDGE_PORT}`;
const CONTROL_URL = `http://127.0.0.1:${CONTROL_PORT}`;

function postJson(urlPath, data) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(data);
    const req = http.request(
      `${CONTROL_URL}${urlPath}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(payload),
        },
      },
      (res) => {
        let body = "";
        res.on("data", (chunk) => (body += chunk));
        res.on("end", () => {
          try {
            resolve({ status: res.statusCode, body: JSON.parse(body) });
          } catch {
            resolve({ status: res.statusCode, body });
          }
        });
      }
    );
    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

function getJson(urlPath) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      `${CONTROL_URL}${urlPath}`,
      { method: "GET" },
      (res) => {
        let body = "";
        res.on("data", (chunk) => (body += chunk));
        res.on("end", () => {
          try {
            resolve({ status: res.statusCode, body: JSON.parse(body) });
          } catch {
            resolve({ status: res.statusCode, body });
          }
        });
      }
    );
    req.on("error", reject);
    req.end();
  });
}

async function waitForHealth(retries = 30) {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await getJson("/healthz");
      if (res.status === 200 && res.body?.status === "ok") {
        return true;
      }
    } catch {
      // wait and retry
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error("Gateway failed to become healthy in time");
}

async function runTests() {
  console.log("============================================================");
  console.log("  Starting socketio-rs Node.js Official Client E2E Suite     ");
  console.log("============================================================");

  const binPath = "../../target/debug/socketio-gateway";
  const gwProcess = spawn(
    binPath,
    [
      "--serve",
      "--bind",
      `127.0.0.1:${EDGE_PORT}`,
      "--control-bind",
      `127.0.0.1:${CONTROL_PORT}`,
    ],
    { stdio: "inherit" }
  );

  const cleanup = () => {
    try {
      gwProcess.kill("SIGTERM");
    } catch {}
  };
  process.on("exit", cleanup);
  process.on("SIGINT", cleanup);
  process.on("SIGTERM", cleanup);

  try {
    console.log(`[1/8] Waiting for gateway health on ${CONTROL_URL}/healthz ...`);
    await waitForHealth();
    console.log("      ✓ Gateway is healthy.");

    // Test 1: Polling-only connection
    console.log("[2/8] Testing HTTP Long-Polling transport connection...");
    await new Promise((resolve, reject) => {
      const socket = io(EDGE_URL, {
        transports: ["polling"],
        reconnection: false,
        timeout: 5000,
      });
      socket.on("connect", () => {
        assert.ok(socket.id, "socket.id must exist on connect");
        console.log(`      ✓ Polling connected with sid: ${socket.id}`);
        socket.disconnect();
        resolve();
      });
      socket.on("connect_error", reject);
    });

    // Test 2: WebSocket direct connection
    console.log("[3/8] Testing Direct WebSocket connection...");
    const wsSocket = await new Promise((resolve, reject) => {
      const socket = io(EDGE_URL, {
        transports: ["websocket"],
        reconnection: false,
        timeout: 5000,
      });
      socket.on("connect", () => {
        assert.ok(socket.id, "socket.id must exist on connect");
        console.log(`      ✓ WebSocket connected with sid: ${socket.id}`);
        resolve(socket);
      });
      socket.on("connect_error", reject);
    });

    // Test 3: Request-Response ACK Roundtrip over WebSocket
    console.log("[4/8] Testing Request-Response ACK (ping -> pong)...");
    await new Promise((resolve, reject) => {
      wsSocket.timeout(3000).emit("ping", (err, response) => {
        if (err) return reject(err);
        assert.strictEqual(response, "pong", "Server must reply with pong ack");
        console.log("      ✓ ACK roundtrip received: 'pong'");
        resolve();
      });
    });

    // Test 4: Dynamic Event Echo Roundtrip
    console.log("[5/8] Testing Inbound event & reply (echo -> echo_reply)...");
    await new Promise((resolve, reject) => {
      const testMsg = { text: "Wire-compatible pure Rust engine", seq: 42 };
      wsSocket.on("echo_reply", (data) => {
        assert.deepStrictEqual(data, testMsg, "Echo payload must match");
        console.log("      ✓ Echo reply received and verified");
        resolve();
      });
      wsSocket.emit("echo", testMsg);
      setTimeout(() => reject(new Error("Timeout waiting for echo_reply")), 4000);
    });

    // Test 5: Sidecar Control API Room Broadcast
    console.log("[6/8] Testing Sidecar Control API Room Broadcast (/_rooms/join & /_broadcast)...");
    const testRoom = "test-node-room";
    const joinRes = await postJson("/_rooms/join", {
      sid: wsSocket.id,
      room: testRoom,
    });
    assert.strictEqual(joinRes.status, 200);
    assert.strictEqual(joinRes.body?.ok, true);

    await new Promise((resolve, reject) => {
      const broadcastMsg = { alert: "Zero-copy broadcast", ts: Date.now() };
      wsSocket.on("system_alert", (data) => {
        assert.deepStrictEqual(data, broadcastMsg);
        console.log("      ✓ Broadcast event received by room subscriber");
        resolve();
      });

      postJson("/_broadcast", {
        namespace: "/",
        room: testRoom,
        event: "system_alert",
        data: broadcastMsg,
      }).catch(reject);

      setTimeout(() => reject(new Error("Timeout waiting for broadcast_event")), 4000);
    });

    // Test 6: Polling to WebSocket Upgrade Handshake
    console.log("[7/8] Testing Transport Upgrade (polling -> 2probe -> 3probe -> 5 -> websocket)...");
    await new Promise((resolve, reject) => {
      const socket = io(EDGE_URL, {
        transports: ["polling", "websocket"],
        upgrade: true,
        reconnection: false,
        timeout: 5000,
      });
      socket.on("connect", () => {
        // Wait briefly for upgrade probe cycle to complete
        setTimeout(() => {
          assert.strictEqual(
            socket.io.engine.transport.name,
            "websocket",
            "Transport must have upgraded to websocket"
          );
          console.log("      ✓ Successfully upgraded from polling to websocket");
          socket.disconnect();
          resolve();
        }, 600);
      });
      socket.on("connect_error", reject);
    });

    // Test 7: Prometheus & Stats API check
    console.log("[8/8] Testing Prometheus exposition & Stats counters...");
    const statsRes = await getJson("/_stats");
    assert.strictEqual(statsRes.status, 200);
    assert.ok(statsRes.body?.active_rooms !== undefined);

    const metricsRes = await getJson("/metrics");
    assert.strictEqual(metricsRes.status, 200);
    assert.ok(typeof metricsRes.body === "string" && metricsRes.body.includes("socketio_"));
    console.log("      ✓ Stats and Prometheus endpoints validated");

    wsSocket.disconnect();
    console.log("\n============================================================");
    console.log("  ALL 8 OFFICIAL NODE.JS CLIENT E2E TESTS PASSED (100%)      ");
    console.log("============================================================\n");
  } finally {
    cleanup();
  }
}

runTests().catch((err) => {
  console.error("Test Suite Failed:", err);
  process.exit(1);
});
