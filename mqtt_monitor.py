#!/usr/bin/env python3
"""Smart reader MQTT monitor.

Subscribes to the reader topics, keeps only RFID tag reads (heartbeat/"alive"
responses are ignored), and serves a page where each reader position lights up
on a tag read and then fades out. Tag reads are pushed to the browser with
Server-Sent Events, so the page updates as soon as a message arrives.

Run:
    python3 mqtt_monitor.py
    python3 mqtt_monitor.py --http-port 8080 --fade 3

The MQTT broker defaults to 192.168.1.2:1883 without authentication.
EMQX's default ACL rejects subscriptions to a bare "#" from non-local clients,
so the default filter is "/row/#", which matches the reader topics.
"""

from __future__ import annotations

import argparse
import json
import queue
import re
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover - exercised when starting without deps
    mqtt = None  # type: ignore[assignment]


# This is the topic shape emitted by generate_gw_config.py:
# /row/{row}/column/{col}/id/{uid}/reader/response
ROW_COL_IN_TOPIC = re.compile(
    r"(?:^|/)row/(?P<row>\d+)/(?:column|col)/(?P<col>\d+)(?:/|$)",
    re.IGNORECASE,
)

# Reader responses look like 010308 + tag id + trailer, e.g.
#   01030803843D3C242173050000  -> tag 03843D3C242173
#   01030800000000000000050000  -> all-zero tag id: alive, no tag present
# One payload can carry several responses separated by CRLF.
TAG_RESPONSE = re.compile(r"^010308(?P<tag>[0-9A-F]{14})")


def is_tag_read(payload: bytes) -> bool:
    for line in payload.decode("utf-8", errors="ignore").upper().split():
        match = TAG_RESPONSE.match(line)
        if match and match.group("tag").strip("0"):
            return True
    return False


def extract_coordinates(topic: str) -> Optional[tuple[int, int]]:
    match = ROW_COL_IN_TOPIC.search(topic)
    if match:
        # The gateway config has row and column swapped in the topic, so the
        # display is transposed here instead of changing the subscriptions.
        return int(match.group("col")), int(match.group("row"))
    return None


class TagHub:
    """Last tag-read time per (row, col), fanned out to SSE subscribers."""

    def __init__(self) -> None:
        self._last_seen: dict[tuple[int, int], float] = {}
        self._subscribers: set[queue.Queue[tuple[int, int, float]]] = set()
        self._lock = threading.Lock()

    def hit(self, row: int, col: int) -> None:
        now = time.time()
        with self._lock:
            self._last_seen[(row, col)] = now
            subscribers = list(self._subscribers)
        for q in subscribers:
            q.put((row, col, now))

    def subscribe(self) -> tuple[queue.Queue[tuple[int, int, float]], list[dict[str, Any]]]:
        """Register a subscriber and return it with the current ages."""
        q: queue.Queue[tuple[int, int, float]] = queue.Queue()
        now = time.time()
        with self._lock:
            self._subscribers.add(q)
            snapshot = [
                {"row": row, "col": col, "age": now - seen}
                for (row, col), seen in self._last_seen.items()
            ]
        return q, snapshot

    def unsubscribe(self, q: queue.Queue[tuple[int, int, float]]) -> None:
        with self._lock:
            self._subscribers.discard(q)


HUB = TagHub()


def _mqtt_client(broker: str, port: int, subscribe_topic: str) -> Any:
    if mqtt is None:
        raise SystemExit(
            "缺少 MQTT 依赖，请先执行: python3 -m pip install -r requirements.txt"
        )
    # paho-mqtt 1.x and 2.x use slightly different constructors.
    try:
        # MQTT 3.1.1 is supported by the simple brokers normally used by the
        # gateway and avoids requiring MQTT 5 settings on the broker.
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv311)
    except (AttributeError, TypeError):
        client = mqtt.Client(protocol=mqtt.MQTTv311)

    def on_connect(client: mqtt.Client, userdata: Any, flags: Any, rc: Any, *args: Any) -> None:
        code = getattr(rc, "value", rc)
        if code == 0:
            client.subscribe(subscribe_topic)
            print(f"MQTT connected, subscribed to {subscribe_topic}")
        else:
            print(f"MQTT connect failed (code={code})")

    def on_subscribe(client: mqtt.Client, userdata: Any, mid: Any, reason_codes: Any = None, *args: Any) -> None:
        codes = reason_codes if isinstance(reason_codes, (list, tuple)) else [reason_codes]
        values = [getattr(code, "value", code) for code in codes]
        if any(value is None or value >= 128 for value in values):
            print(f"MQTT subscribe to {subscribe_topic} rejected by broker (codes={values}); check the broker ACL")

    def on_message(client: mqtt.Client, userdata: Any, message: Any) -> None:
        if not is_tag_read(message.payload):
            return
        coordinates = extract_coordinates(message.topic)
        if not coordinates:
            return
        row, col = coordinates
        if row < 1 or col < 1 or row > 12 or col > 12:
            return
        HUB.hit(row, col)

    def on_disconnect(client: mqtt.Client, userdata: Any, disconnect_flags: Any = None, rc: Any = None, *args: Any) -> None:
        code = getattr(rc, "value", rc)
        print(f"MQTT disconnected (code={code}); waiting to reconnect...")

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.connect_async(broker, port, keepalive=60)
    return client


PAGE = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>读卡器状态监控</title>
<style>
:root { color-scheme: dark; font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
body { margin:0; background:#10141c; color:#e7ecf4; }
main { max-width:1100px; margin:0 auto; padding:24px; }
h1 { margin:0 0 8px; font-size:26px; }
.sub { color:#96a2b5; margin-bottom:20px; }
.toolbar { display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin-bottom:18px; }
button { color:#dce6f5; background:#202a3a; border:1px solid #3a4a62; border-radius:7px; padding:8px 14px; cursor:pointer; }
button.active { background:#2667b2; border-color:#4b9bff; }
#conn { margin-left:auto; color:#96a2b5; font-size:13px; }
#conn.error { color:#ff9e9e; }
.grid { display:grid; gap:6px; }
.cell { position:relative; overflow:hidden; min-height:58px; border:1px solid #293548; border-radius:7px; background:#171e2a; }
.cell .glow { position:absolute; inset:0; background:#35d07f; opacity:0; }
.cell .pos { position:relative; padding:6px; font-size:12px; color:#8491a4; }
@media (max-width:650px) { main { padding:14px; } .cell { min-height:40px; } .cell .pos { padding:3px; font-size:10px; } }
</style>
</head>
<body><main>
<h1>读卡器状态监控</h1>
<div class="sub">读到 tag 时对应位置点亮，随后渐隐。</div>
<div class="toolbar">
  <button data-mode="lite">Lite · 6×6</button><button data-mode="pro">Pro · 12×12</button>
  <span id="conn">连接中…</span>
</div>
<div id="grid" class="grid"></div>
<script>
const params = new URLSearchParams(location.search);
let mode = params.get('mode') === 'pro' ? 'pro' : 'lite';
const fadeMs = (parseFloat(params.get('fade')) || __FADE__) * 1000;
const lastHit = new Map();  // "row-col" -> performance.now() of the last tag read
const buttons = [...document.querySelectorAll('button')];
buttons.forEach(b => b.onclick = () => {
  mode = b.dataset.mode; params.set('mode', mode); history.replaceState(null, '', '?' + params); render();
});
function glow(key) {
  const el = document.getElementById('glow-' + key), at = lastHit.get(key);
  if (!el || at === undefined) return;
  el.getAnimations().forEach(a => a.cancel());
  // A negative delay resumes the fade part-way through, e.g. after a mode switch.
  el.animate([{opacity: 1}, {opacity: 0}], {duration: fadeMs, delay: at - performance.now(), easing: 'cubic-bezier(0.2, 0.8, 0.4, 1)', fill: 'forwards'});
}
function render() {
  buttons.forEach(b => b.classList.toggle('active', b.dataset.mode === mode));
  const n = mode === 'pro' ? 12 : 6;
  const grid = document.getElementById('grid'); grid.style.gridTemplateColumns = `repeat(${n}, minmax(0, 1fr))`;
  const cells = []; for (let row=1; row<=n; row++) for (let col=1; col<=n; col++) cells.push(`<div class="cell"><div class="glow" id="glow-${row}-${col}"></div><div class="pos">R${row} · C${col}</div></div>`);
  grid.innerHTML = cells.join('');
  lastHit.forEach((_, key) => glow(key));
}
function hit(row, col, ageMs) {
  if (ageMs >= fadeMs) return;
  const key = `${row}-${col}`, at = performance.now() - ageMs;
  if (at < (lastHit.get(key) ?? -Infinity)) return;
  lastHit.set(key, at); glow(key);
}
// Events carry the server's read time. The smallest (client clock - server time)
// seen so far is the normal transit time; anything above it is time the event
// spent stuck in the network, so a burst after a stall shows as already faded.
let minLag = Infinity;
function ageOf(serverTs) { const lag = Date.now() - serverTs * 1000; minLag = Math.min(minLag, lag); return lag - minLag; }
function connect() {
  const conn = document.getElementById('conn'), es = new EventSource('/events');
  es.addEventListener('snapshot', e => JSON.parse(e.data).forEach(x => x.age * 1000 < fadeMs && hit(x.row, x.col, x.age * 1000)));
  es.addEventListener('tag', e => { const [row, col, ts] = JSON.parse(e.data); hit(row, col, ageOf(ts)); });
  es.onopen = () => { conn.textContent = '实时'; conn.className = ''; };
  es.onerror = () => { conn.textContent = '连接断开，重连中…'; conn.className = 'error'; };
}
render(); connect();
</script></main></body></html>"""


class Handler(BaseHTTPRequestHandler):
    fade_seconds = 3.0

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/events":
            self._stream_events()
            return
        if path in ("/", "/index.html"):
            body = PAGE.replace("__FADE__", repr(self.fade_seconds)).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
        else:
            body = b"Not found"
            self.send_response(HTTPStatus.NOT_FOUND)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _stream_events(self) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        q, snapshot = HUB.subscribe()
        try:
            self._send("snapshot", snapshot)
            while True:
                try:
                    self._send("tag", q.get(timeout=15))
                except queue.Empty:
                    # Comment line keeps idle connections (and tunnels) open.
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
        except OSError:
            pass  # browser closed the page
        finally:
            HUB.unsubscribe(q)

    def _send(self, event: str, data: Any) -> None:
        self.wfile.write(f"event: {event}\ndata: {json.dumps(data)}\n\n".encode("utf-8"))
        self.wfile.flush()

    def log_message(self, format: str, *args: Any) -> None:
        # Keep the terminal focused on MQTT activity.
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Smart reader MQTT status monitor")
    parser.add_argument("--broker", default="192.168.1.2", help="MQTT broker host (default: 192.168.1.2)")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port (default: 1883)")
    parser.add_argument("--subscribe", default="/row/#", help="MQTT topic filter (default: /row/#)")
    parser.add_argument("--bind", default="0.0.0.0", help="Web bind address (default: 0.0.0.0)")
    parser.add_argument("--http-port", type=int, default=8080, help="Web port (default: 8080)")
    parser.add_argument("--fade", type=float, default=3.0, help="Seconds for a lit cell to fade out (default: 3)")
    args = parser.parse_args()

    Handler.fade_seconds = args.fade
    client = _mqtt_client(args.broker, args.port, args.subscribe)
    client.loop_start()
    server = ThreadingHTTPServer((args.bind, args.http_port), Handler)
    server.daemon_threads = True
    print(f"Web UI: http://127.0.0.1:{args.http_port}/?mode=lite")
    print(f"Web UI: http://127.0.0.1:{args.http_port}/?mode=pro")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        server.server_close()
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
