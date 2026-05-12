import express from "express";
import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import crypto from "crypto";

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 10000;
const BASE_DOMAIN = process.env.BASE_DOMAIN || "obsio.tech";

const tunnels = new Map();

app.use(express.raw({ type: "*/*", limit: "20mb" }));

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    tunnels: tunnels.size
  });
});

function getTunnelIdFromHost(host) {
  if (!host) return null;

  const cleanHost = host.split(":")[0];

  if (cleanHost.endsWith(`.${BASE_DOMAIN}`)) {
    return cleanHost.replace(`.${BASE_DOMAIN}`, "");
  }

  if (cleanHost.endsWith(".localhost")) {
    return cleanHost.replace(".localhost", "");
  }

  return null;
}

// Express 5 fix: use app.use instead of app.all("*")
app.use(async (req, res) => {
  const tunnelId = getTunnelIdFromHost(req.headers.host);

  if (!tunnelId) {
    return res.status(400).send("Invalid tunnel host");
  }

  const ws = tunnels.get(tunnelId);

  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return res.status(404).send("Tunnel not found or disconnected");
  }

  const requestId = crypto.randomUUID();

  const bodyBase64 = req.body?.length ? req.body.toString("base64") : "";

  const payload = {
    type: "request",
    requestId,
    method: req.method,
    path: req.originalUrl,
    headers: req.headers,
    bodyBase64
  };

  const timeout = setTimeout(() => {
    if (!res.headersSent) {
      res.status(504).send("Tunnel request timeout");
    }

    ws.pendingRequests?.delete(requestId);
  }, 30000);

  ws.pendingRequests ??= new Map();

  ws.pendingRequests.set(requestId, (response) => {
    clearTimeout(timeout);

    if (res.headersSent) return;

    res.status(response.status || 502);

    if (response.headers) {
      for (const [key, value] of Object.entries(response.headers)) {
        const lowerKey = key.toLowerCase();

        if (
          lowerKey === "content-encoding" ||
          lowerKey === "transfer-encoding" ||
          lowerKey === "connection"
        ) {
          continue;
        }

        try {
          res.setHeader(key, value);
        } catch {}
      }
    }

    const responseBuffer = Buffer.from(response.bodyBase64 || "", "base64");
    res.send(responseBuffer);
  });

  ws.send(JSON.stringify(payload));
});

wss.on("connection", (ws) => {
  ws.pendingRequests = new Map();
  let tunnelId = null;

  ws.on("message", (message) => {
    try {
      const data = JSON.parse(message.toString());

      if (data.type === "register") {
        tunnelId = data.tunnelId;
        tunnels.set(tunnelId, ws);

        ws.send(
          JSON.stringify({
            type: "registered",
            tunnelId
          })
        );

        console.log(`Tunnel registered: ${tunnelId}`);
      }

      if (data.type === "response") {
        const callback = ws.pendingRequests.get(data.requestId);

        if (callback) {
          callback(data);
          ws.pendingRequests.delete(data.requestId);
        }
      }
    } catch (error) {
      console.error("WS message error:", error.message);
    }
  });

  ws.on("close", () => {
    if (tunnelId) {
      tunnels.delete(tunnelId);
      console.log(`Tunnel removed: ${tunnelId}`);
    }
  });

  ws.on("error", () => {
    if (tunnelId) {
      tunnels.delete(tunnelId);
    }
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Tunnel server running on port ${PORT}`);
});