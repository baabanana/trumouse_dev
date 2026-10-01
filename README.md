# Smart Reader MQTT Monitor

这是一个简单的 MQTT 读卡器状态页面。程序监听读卡器的 MQTT 消息，只保留 tag 读卡数据（忽略 alive 心跳），在 Lite（6×6）或 Pro（12×12）网格上把对应位置点亮，然后渐隐。

## 启动

```bash
python3 -m pip install -r requirements.txt
python3 mqtt_monitor.py
```

默认连接 `192.168.1.2:1883`，无用户名和密码，订阅 `/row/#`（EMQX 默认 ACL 会拒绝非本机客户端订阅 `#`）。浏览器打开：

- `http://127.0.0.1:8080/?mode=lite`
- `http://127.0.0.1:8080/?mode=pro`

也可以覆盖参数：

```bash
python3 mqtt_monitor.py --broker 192.168.1.2 --port 1883 --subscribe '/row/#' --fade 3
```

## 识别规则

位置从 topic 解析：

```text
/row/3/column/5/id/LA9/reader/response
```

注意：网关配置里 topic 的 row 和 column 写反了，页面显示时已转置，上例显示在 R5 · C3。

payload 形如 `010308` + 14 位 tag 号 + 尾部（如 `01030803843D3C242173050000`）。tag 号不全为 0 时视为读到 tag；全 0（`01030800000000000000050000`）是 alive，直接忽略。一条 payload 里可能用换行拼了多条响应，逐行判断。

读到 tag 后，页面通过 Server-Sent Events 实时收到推送，对应格子立即点亮并在 `--fade` 秒（默认 3 秒）内渐隐；也可以在 URL 上加 `&fade=5` 临时调整。状态只保存在进程内存中。
