// WebSocket client for tests/inapp.sh (Node 22+, built-in WebSocket).
// Logs every frame (and the close) as one JSON line. After READ_AFTER notifications it sends
// {"type":"read"}. usage: node inapp_client.mjs URL LOG_FILE [READ_AFTER]
import { appendFileSync } from "node:fs";

const [url, log, readAfter] = process.argv.slice(2);
const write = (o) => appendFileSync(log, JSON.stringify(o) + "\n");
let notes = 0;
const ws = new WebSocket(url);
ws.onopen = () => write({ type: "_open" });
ws.onmessage = (e) => {
  const m = JSON.parse(e.data);
  write(m);
  if (m.type === "notification" && ++notes === Number(readAfter || 0)) ws.send(JSON.stringify({ type: "read" }));
};
ws.onclose = (e) => { write({ type: "_close", code: e.code, reason: e.reason }); process.exit(0); };
ws.onerror = () => write({ type: "_error" });
