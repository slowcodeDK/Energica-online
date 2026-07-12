import { createServer } from "http";
import { readFile } from "fs/promises";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { defineSignals, record } from "./can/signals.ts";
import { SIGNALS } from "./can/registry.ts";
import { bringUpCan, openChannel } from "./can/socket.ts";
import { decodeFrame, STREAM_IDS } from "./can/decode.ts";
import { initObd, isObdResponse, handleResponse, startObdPoller } from "./can/obd.ts";
import { setupWs } from "./ws.ts";
import type { RawChannel } from "socketcan";

// Thin orchestrator: wire CAN decode/OBD + HTTP/WS together.
// See obd-garage/INTEGRATION_PLAN.md.

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const PORT = 8080;
const CAN_IFACE = "can0";

// Config (env overrides):
//   CAN_ENABLED=0 → skip CAN entirely
//   OBD_ENABLED=0 → passive/listen-only: decode broadcasts but don't TX OBD polls
const CAN_ENABLED = process.env.CAN_ENABLED !== "0";
const OBD_ENABLED = process.env.OBD_ENABLED !== "0";

// --- DB + signal registry ---// Thin orchestrator: wire DB + CAN decode/OBD + HTTP/WS together.
defineSignals(SIGNALS);

// --- Coolant probes (MAX31865) DELETED ---

// --- CAN: broadcast decode + OBD-II polling ---
let channel: RawChannel | undefined;
let stopObd: (() => void) | undefined;

if (CAN_ENABLED) {
  try {
    await bringUpCan(CAN_IFACE, OBD_ENABLED); // ACTIVE only when we intend to TX OBD reads
    channel = openChannel(CAN_IFACE);

    try {
      channel.setRxFilters([
        ...STREAM_IDS.map(id => ({ id, mask: 0x7ff })),
        { id: 0x7e0, mask: 0x7f0 }, // OBD responses 0x7E0–0x7EF
      ]);
    } catch (err) {
      console.warn("can: setRxFilters failed, accepting all frames:", err);
    }

    channel.addListener("onMessage", msg => {
      const data = msg.data;
      if (isObdResponse(msg.id)) {
        handleResponse(msg.id, data);
        return;
      }
      for (const { key, value } of decodeFrame(msg.id, data)) {
        record(key, value);
      }
    });
    channel.start();
    console.log("can: channel started, decoding broadcasts");

    if (OBD_ENABLED) {
      initObd(channel);
      stopObd = startObdPoller(500);
      console.log("obd: polling @2Hz (speed/rpm/temps/load/distance)");
    } else {
      console.log("obd: disabled (OBD_ENABLED=0) — passive decode only");
    }
  } catch (err) {
    console.error("can: init failed — continuing without CAN:", err);
  }
} else {
  console.log("can: disabled (CAN_ENABLED=0)");
}

// --- HTTP + WebSocket server ---
const indexHtml = await readFile(join(ROOT, "public", "index.html"), "utf-8");

const server = createServer(async (req, res) => {
  res.writeHead(200, { "Content-Type": "text/html" });
  res.end(indexHtml);
});

const ws = setupWs(server);

server.listen(PORT, () => {
  console.log(`HTTP + WebSocket server on http://0.0.0.0:${PORT}`);
});

// --- Graceful shutdown ---
let shuttingDown = false;
function shutdown(): void {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log("\nShutting down…");
  stopObd?.();
  try {
    channel?.stop();
  } catch {
    // ignore
  }
  ws.stop();
  server.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
