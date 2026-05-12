import express from "express";
import http from "http";
import { WebSocketServer, WebSocket } from "ws";
import crypto from "crypto";

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

const PORT = process.env.PORT || 10000;
const REQUEST_TIMEOUT_MS = 120000;
const MAX_BODY_SIZE = "50mb";

const tunnels = new Map();

app.use(express.raw({ type: "*/*", limit: MAX_BODY_SIZE }));

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    tunnels: tunnels.size
  });
});

function parseCookies(cookieHeader = "") {
  return Object.fromEntries(
    cookieHeader
      .split(";")
      .map((v) => v.trim().split("="))
      .filter(([key, value]) => key && value)
  );
}

function getTunnelIdFromRequest(req) {
  const urlMatch = req.originalUrl.match(/^\/t\/([^/]+)(\/.*)?$/);
  if (urlMatch) return urlMatch[1];

  const cookies = parseCookies(req.headers.cookie);
  if (cookies.__devjk_tunnel) return cookies.__devjk_tunnel;

  const referer = req.headers.referer;
  if (referer) {
    try {
      const refererUrl = new URL(referer);
      const refererMatch = refererUrl.pathname.match(/^\/t\/([^/]+)/);
      if (refererMatch) return refererMatch[1];
    } catch {}
  }

  return null;
}

function getForwardPath(req) {
  const match = req.originalUrl.match(/^\/t\/[^/]+(\/.*)?$/);
  if (match) return match[1] || "/";

  return req.originalUrl || "/";
}

function cleanRequestHeaders(headers, tunnelId) {
  const cleaned = { ...headers };

  delete cleaned.host;
  delete cleaned.connection;
  delete cleaned["content-length"];
  delete cleaned["accept-encoding"];
  delete cleaned["cf-connecting-ip"];
  delete cleaned["cf-ray"];
  delete cleaned["x-forwarded-for"];
  delete cleaned["x-forwarded-host"];
  delete cleaned["x-forwarded-proto"];

  cleaned["x-devjk-tunnel-id"] = tunnelId;

  return cleaned;
}

function cleanResponseHeaders(headers = {}) {
  const cleaned = {};

  for (const [key, value] of Object.entries(headers)) {
    const lowerKey = key.toLowerCase();

    if (
      lowerKey === "content-encoding" ||
      lowerKey === "transfer-encoding" ||
      lowerKey === "connection" ||
      lowerKey === "content-length"
    ) {
      continue;
    }

    cleaned[key] = value;
  }

  return cleaned;
}

function rewriteHtml(html, tunnelId) {
  const prefix = `/t/${tunnelId}`;

  return html
    .replace(/(<head[^>]*>)/i, `$1<base href="${prefix}/">`)
    .replaceAll(`src="/`, `src="${prefix}/`)
    .replaceAll(`href="/`, `href="${prefix}/`)
    .replaceAll(`action="/`, `action="${prefix}/`)
    .replaceAll(`url("/`, `url("${prefix}/`)
    .replaceAll(`url('/`, `url('${prefix}/`);
}

app.use(async (req, res) => {
  const tunnelId = getTunnelIdFromRequest(req);
  const forwardPath = getForwardPath(req);

  console.log("Forward:", tunnelId, req.method, req.originalUrl, "=>", forwardPath);

  if (!tunnelId) {
    return res.status(400).send("Invalid tunnel path. Use /t/:tunnelId");
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
    path: forwardPath,
    headers: cleanRequestHeaders(req.headers, tunnelId),
    bodyBase64
  };

  const timeout = setTimeout(() => {
    if (!res.headersSent) {
      res.status(504).send("Tunnel request timeout");
    }

    ws.pendingRequests?.delete(requestId);
  }, REQUEST_TIMEOUT_MS);

  ws.pendingRequests ??= new Map();

  ws.pendingRequests.set(requestId, (response) => {
    clearTimeout(timeout);

    if (res.headersSent) return;

    const status = response.status || 502;
    const headers = cleanResponseHeaders(response.headers || {});
    const responseBuffer = Buffer.from(response.bodyBase64 || "", "base64");

    res.status(status);

    for (const [key, value] of Object.entries(headers)) {
      try {
        res.setHeader(key, value);
      } catch {}
    }

    res.setHeader("Set-Cookie", `__devjk_tunnel=${tunnelId}; Path=/; SameSite=Lax`);
    res.setHeader("X-DevJK-Tunnel", tunnelId);

    const contentType = String(headers["content-type"] || headers["Content-Type"] || "");

    if (contentType.includes("text/html")) {
      const html = responseBuffer.toString("utf8");
      return res.send(rewriteHtml(html, tunnelId));
    }

    return res.send(responseBuffer);
  });

  try {
    ws.send(JSON.stringify(payload));
  } catch {
    ws.pendingRequests.delete(requestId);
    return res.status(502).send("Failed to forward request to tunnel client");
  }
});

server.on("upgrade", (req, socket, head) => {
  const pathname = req.url || "";

  if (pathname.startsWith("/t/")) {
    socket.write("HTTP/1.1 426 Upgrade Required\r\n\r\nWebSocket proxy is not supported yet");
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit("connection", ws, req);
  });
});

wss.on("connection", (ws) => {
  ws.pendingRequests = new Map();
  let tunnelId = null;
  let heartbeat = null;

  ws.isAlive = true;

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  heartbeat = setInterval(() => {
    if (ws.isAlive === false) {
      if (tunnelId) tunnels.delete(tunnelId);
      ws.terminate();
      return;
    }

    ws.isAlive = false;
    ws.ping();
  }, 30000);

  ws.on("message", (message) => {
    try {
      const data = JSON.parse(message.toString());

      if (data.type === "register") {
        tunnelId = data.tunnelId;

        const oldSocket = tunnels.get(tunnelId);
        if (oldSocket && oldSocket !== ws) {
          oldSocket.close();
        }

        tunnels.set(tunnelId, ws);

        ws.send(
          JSON.stringify({
            type: "registered",
            tunnelId
          })
        );

        console.log(`Tunnel registered: ${tunnelId}`);
        return;
      }

      if (data.type === "response") {
        const callback = ws.pendingRequests.get(data.requestId);

        if (callback) {
          callback(data);
          ws.pendingRequests.delete(data.requestId);
        }

        return;
      }
    } catch (error) {
      console.error("WS message error:", error.message);
    }
  });

  ws.on("close", () => {
    clearInterval(heartbeat);

    if (tunnelId) {
      tunnels.delete(tunnelId);
      console.log(`Tunnel removed: ${tunnelId}`);
    }

    for (const callback of ws.pendingRequests.values()) {
      callback({
        status: 502,
        headers: {
          "content-type": "text/plain"
        },
        bodyBase64: Buffer.from("Tunnel client disconnected").toString("base64")
      });
    }

    ws.pendingRequests.clear();
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