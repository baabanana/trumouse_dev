#!/usr/bin/env python3
"""Smart reader MQTT monitor.

Subscribes to all MQTT topics, extracts a reader's row/column from the topic or
JSON payload, and serves a small status page.

Run:
    python3 mqtt_monitor.py
    python3 mqtt_monitor.py --mode lite --http-port 8080

The MQTT broker defaults to 192.168.1.2:1883 without authentication.
"""

from __future__ import annotations

import argparse
import json
import re
import threading
import time
from dataclasses import dataclass
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
ROW_COL_TEXT = re.compile(
    r"\brow\s*[/=:_-]?\s*(\d+)\D+(?:column|col)\s*[/=:_-]?\s*(\d+)\b",
    re.IGNORECASE,
)


@dataclass
class ReaderState:
    row: int
    col: int
    last_seen: float
    topic: str
    payload_preview: str
    message_count: int = 1

    def as_dict(self, now: float) -> dict[str, Any]:
        age = max(0.0, now - self.last_seen)
        return {
            "row": self.row,
            "col": self.col,
            "last_seen": self.last_seen,
            "age_seconds": round(age, 1),
            "status": "online" if age <= 90 else "stale",
            "topic": self.topic,
            "payload": self.payload_preview,
            "message_count": self.message_count,
        }


class ReaderStore:
    """Thread-safe in-memory last-seen state, keyed by (row, col)."""

    def __init__(self) -> None:
        self._states: dict[tuple[int, int], ReaderState] = {}
        self._lock = threading.Lock()

    def update(self, row: int, col: int, topic: str, payload: bytes) -> None:
        now = time.time()
        preview = payload.decode("utf-8", errors="replace")[:240]
        with self._lock:
            old = self._states.get((row, col))
            self._states[(row, col)] = ReaderState(
                row=row,
                col=col,
                last_seen=now,
                topic=topic,
                payload_preview=preview,
                message_count=(old.message_count + 1 if old else 1),
            )

    def snapshot(self) -> list[dict[str, Any]]:
        now = time.time()
        with self._lock:
            return [state.as_dict(now) for state in self._states.values()]


STORE = ReaderStore()


def _as_int(value: Any) -> Optional[int]:
    # bool is an int subclass but is never a valid coordinate here.
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _coordinates_from_object(value: Any) -> Optional[tuple[int, int]]:
    """Find row/col in nested JSON objects used as MQTT message bodies."""
    if isinstance(value, dict):
        row = _as_int(value.get("row"))
        col = _as_int(value.get("col", value.get("column")))
        if row is not None and col is not None:
            return row, col
        # Some devices wrap this as sender/source/device/reader.
        for child in value.values():
            found = _coordinates_from_object(child)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _coordinates_from_object(child)
            if found:
                return found
    elif isinstance(value, str):
        found = ROW_COL_IN_TOPIC.search(value) or ROW_COL_TEXT.search(value)
        if found:
            return int(found.group("row") if "row" in found.groupdict() else found.group(1)), int(
                found.group("col") if "col" in found.groupdict() else found.group(2)
            )
    return None


def extract_coordinates(topic: str, payload: bytes) -> Optional[tuple[int, int]]:
    """Extract (row, col), preferring the explicit topic path."""
    match = ROW_COL_IN_TOPIC.search(topic)
    if match:
        return int(match.group("row")), int(match.group("col"))

    text = payload.decode("utf-8", errors="replace")
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError):
        parsed = None
    found = _coordinates_from_object(parsed)
    if found:
        return found

    match = ROW_COL_TEXT.search(text)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None


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

    def on_message(client: mqtt.Client, userdata: Any, message: Any) -> None:
        coordinates = extract_coordinates(message.topic, message.payload)
        if not coordinates:
            return
        row, col = coordinates
        if row < 1 or col < 1 or row > 12 or col > 12:
            return
        STORE.update(row, col, message.topic, message.payload)
        print(f"reader row={row} col={col} topic={message.topic}")

    def on_disconnect(client: mqtt.Client, userdata: Any, disconnect_flags: Any = None, rc: Any = None, *args: Any) -> None:
        code = getattr(rc, "value", rc)
        print(f"MQTT disconnected (code={code}); waiting to reconnect...")

    client.on_connect = on_connect
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
.legend { margin-left:auto; color:#96a2b5; font-size:13px; }
.dot { display:inline-block; width:9px; height:9px; border-radius:50%; margin:0 5px 0 12px; }
.online { background:#35d07f; } .stale { background:#f2ad42; } .empty { background:#526074; }
#summary { color:#b8c4d7; margin-bottom:12px; }
.grid { display:grid; gap:6px; }
.cell { min-height:58px; border:1px solid #293548; border-radius:7px; padding:7px; background:#171e2a; box-sizing:border-box; }
.cell.online { border-color:#278e61; background:#142b25; }
.cell.stale { border-color:#8a672d; background:#2b2416; }
.cell .pos { font-weight:600; font-size:14px; } .cell .state { font-size:12px; color:#aab7ca; margin-top:6px; }
.cell .age { font-size:11px; color:#8491a4; margin-top:3px; }
.error { color:#ff9e9e; }
@media (max-width:650px) { main { padding:14px; } .cell { min-height:50px; padding:5px; } .cell .state { font-size:11px; } }
</style>
</head>
<body><main>
<h1>读卡器状态监控</h1>
<div class="sub">收到 MQTT 消息后，按发送方的 row / col 点亮对应位置。90 秒未收到消息会标为“超时”。</div>
<div class="toolbar">
  <button data-mode="lite">Lite · 6×6</button><button data-mode="pro">Pro · 12×12</button>
  <span class="legend"><i class="dot online"></i>在线 <i class="dot stale"></i>超时 <i class="dot empty"></i>未收到</span>
</div>
<div id="summary">正在读取状态…</div><div id="grid" class="grid"></div>
<script>
let mode = new URLSearchParams(location.search).get('mode') === 'pro' ? 'pro' : 'lite';
const buttons = [...document.querySelectorAll('button')];
buttons.forEach(b => b.onclick = () => { mode = b.dataset.mode; history.replaceState(null, '', '?mode=' + mode); render(); });
function render() {
  buttons.forEach(b => b.classList.toggle('active', b.dataset.mode === mode));
  const n = mode === 'pro' ? 12 : 6;
  const grid = document.getElementById('grid'); grid.style.gridTemplateColumns = `repeat(${n}, minmax(0, 1fr))`;
  const cells = []; for (let row=1; row<=n; row++) for (let col=1; col<=n; col++) cells.push(`<div class="cell" id="cell-${row}-${col}"><div class="pos">R${row} · C${col}</div><div class="state">未收到</div></div>`);
  grid.innerHTML = cells.join('');
}
function update(state) {
  const n = mode === 'pro' ? 12 : 6, map = new Map(state.map(x => [`${x.row}-${x.col}`, x]));
  let online=0, stale=0, received=0;
  for (let row=1; row<=n; row++) for (let col=1; col<=n; col++) {
    const el = document.getElementById(`cell-${row}-${col}`), item = map.get(`${row}-${col}`);
    if (!el || !item) continue;
    received++; el.className = 'cell ' + item.status;
    el.querySelector('.state').textContent = item.status === 'online' ? '在线' : '超时';
    el.querySelector('.age')?.remove();
    const age = document.createElement('div'); age.className='age'; age.textContent = `${Math.round(item.age_seconds)} 秒前 · ${item.message_count} 条`;
    el.appendChild(age); item.status === 'online' ? online++ : stale++;
  }
  const total=n*n; document.getElementById('summary').textContent = `${mode === 'pro' ? 'Pro' : 'Lite'}：${online} 在线，${stale} 超时，${total-received} 个位置未收到消息 · ${new Date().toLocaleTimeString()}`;
}
async function poll() { try { const r=await fetch('/api/state', {cache:'no-store'}); if (!r.ok) throw Error(r.status); update(await r.json()); } catch(e) { document.getElementById('summary').innerHTML='<span class="error">无法读取监控状态</span>'; } }
render(); poll(); setInterval(poll, 2000);
</script></main></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path.split("?", 1)[0] == "/api/state":
            body = json.dumps(STORE.snapshot(), ensure_ascii=False).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
        elif self.path.split("?", 1)[0] in ("/", "/index.html"):
            body = PAGE.encode("utf-8")
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

    def log_message(self, format: str, *args: Any) -> None:
        # Keep the terminal focused on MQTT activity.
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Smart reader MQTT status monitor")
    parser.add_argument("--broker", default="192.168.1.2", help="MQTT broker host (default: 192.168.1.2)")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port (default: 1883)")
    parser.add_argument("--subscribe", default="#", help="MQTT topic filter (default: #)")
    parser.add_argument("--bind", default="0.0.0.0", help="Web bind address (default: 0.0.0.0)")
    parser.add_argument("--http-port", type=int, default=8080, help="Web port (default: 8080)")
    args = parser.parse_args()

    client = _mqtt_client(args.broker, args.port, args.subscribe)
    client.loop_start()
    server = ThreadingHTTPServer((args.bind, args.http_port), Handler)
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
