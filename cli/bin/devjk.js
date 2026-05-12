#!/usr/bin/env node

import { Command } from "commander";
import WebSocket from "ws";
import readline from "readline";
import crypto from "crypto";

const program = new Command();

program
  .name("devjk")
  .description("Temporary localhost tunnel")
  .option("-p, --port <port>", "Local port")
  .option(
    "-s, --server <url>",
    "Tunnel WebSocket server URL",
    "wss://YOUR_RENDER_APP_URL"
  )
  .parse(process.argv);

const options = program.opts();

function askQuestion(question) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      rl.close();
      resolve(answer);
    });
  });
}

function createTunnelId() {
  return crypto.randomBytes(4).toString("hex");
}

async function main() {
  let port = options.port;

  if (!port) {
    port = await askQuestion("Enter local port: ");
  }

  port = Number(port);

  if (!port || Number.isNaN(port) || port < 1 || port > 65535) {
    console.error("Invalid port. Example: npx devjk --port 5173");
    process.exit(1);
  }

  const tunnelId = createTunnelId();
  const serverUrl = options.server;

  if (serverUrl.includes("YOUR_RENDER_APP_URL")) {
    console.error("Please update server URL first.");
    console.error("Example:");
    console.error("npx devjk --port 5173 --server wss://devjk-server.onrender.com");
    process.exit(1);
  }

  const ws = new WebSocket(serverUrl);

  ws.on("open", () => {
    ws.send(
      JSON.stringify({
        type: "register",
        tunnelId
      })
    );
  });

  ws.on("message", async (message) => {
    const data = JSON.parse(message.toString());

    if (data.type === "registered") {
      console.log("");
      console.log("Tunnel ready:");
      console.log(`Local:  http://localhost:${port}`);
      console.log(`Public: https://${tunnelId}.obsio.tech`);
      console.log("");
      console.log("Press Ctrl + C to stop tunnel");
    }

    if (data.type === "request") {
      try {
        const targetUrl = `http://localhost:${port}${data.path}`;

        const headers = { ...data.headers };

        delete headers.host;
        delete headers.connection;
        delete headers["content-length"];
        delete headers["accept-encoding"];

        const bodyBuffer = data.bodyBase64
          ? Buffer.from(data.bodyBase64, "base64")
          : undefined;

        const response = await fetch(targetUrl, {
          method: data.method,
          headers,
          body:
            data.method === "GET" || data.method === "HEAD"
              ? undefined
              : bodyBuffer
        });

        const arrayBuffer = await response.arrayBuffer();
        const responseBodyBase64 = Buffer.from(arrayBuffer).toString("base64");

        const responseHeaders = {};
        response.headers.forEach((value, key) => {
          responseHeaders[key] = value;
        });

        ws.send(
          JSON.stringify({
            type: "response",
            requestId: data.requestId,
            status: response.status,
            headers: responseHeaders,
            bodyBase64: responseBodyBase64
          })
        );
      } catch (error) {
        ws.send(
          JSON.stringify({
            type: "response",
            requestId: data.requestId,
            status: 502,
            headers: {
              "content-type": "text/plain"
            },
            bodyBase64: Buffer.from(
              `Local app error: ${error.message}`
            ).toString("base64")
          })
        );
      }
    }
  });

  ws.on("close", () => {
    console.log("Tunnel server disconnected");
    process.exit(0);
  });

  ws.on("error", (error) => {
    console.error("Tunnel error:", error.message);
    process.exit(1);
  });

  process.on("SIGINT", () => {
    console.log("\nClosing tunnel...");
    ws.close();
    process.exit(0);
  });
}

main();