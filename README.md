# Smart Reader MQTT Monitor

这是一个简单的 MQTT 读卡器状态页面。程序监听 MQTT 消息，解析发送方的 `row` / `col`，并在 Lite（6×6）或 Pro（12×12）网格上显示最后一次收到消息的时间。

## 启动

```bash
python3 -m pip install -r requirements.txt
python3 mqtt_monitor.py
```

默认连接 `192.168.1.2:1883`，无用户名和密码，订阅 `#`。浏览器打开：

- `http://127.0.0.1:8080/?mode=lite`
- `http://127.0.0.1:8080/?mode=pro`

也可以覆盖参数：

```bash
python3 mqtt_monitor.py --broker 192.168.1.2 --port 1883 --subscribe '#'
```

## 位置解析

优先从 topic 解析，例如：

```text
/row/3/column/5/id/LA9/reader/response
```

也支持 payload 中的 JSON 字段（包括嵌套的 `sender`、`source`、`device`、`reader` 等对象）：

```json
{"sender": {"row": 3, "col": 5}, "event": "heartbeat"}
```

没有 `row` / `col` 的 MQTT 消息会被忽略。收到消息后位置显示“在线”；超过 90 秒没有新消息显示“超时”。状态只保存在进程内存中，程序重启后会重新等待消息。
