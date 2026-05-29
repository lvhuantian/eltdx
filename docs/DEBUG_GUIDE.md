# 排错指南

这份文档用来定位三类问题：连不上主站、数据返回异常、协议字段需要核对。

## 连不上

先确认 TCP 连接状态：

```python
from eltdx.hosts import DEFAULT_HOSTS, probe_hosts

results = probe_hosts(DEFAULT_HOSTS[:10], timeout=1.2)
for item in results:
    print(item.host, item.ok, item.latency_ms, item.error)
```

如果大部分主站都连不上，优先检查网络、防火墙、代理和当前环境是否允许直连 `7709` TCP 端口。

如果只是个别主站慢，可以打开测速排序：

```python
from eltdx import TdxClient

with TdxClient(probe_hosts=True, timeout=3) as client:
    print(client.transport.hosts[:5])
    print(client.get_quote("sz000001")[0].last_price)
```

也可以手动指定主站：

```python
with TdxClient(host="116.205.183.150:7709", timeout=3) as client:
    print(client.get_count("sz"))
```

## 主站列表

默认主站来自包内 `tdx_server.json`：

```python
from eltdx.hosts import DEFAULT_HOSTS, load_server_config

print(load_server_config()["schema_version"])
print(DEFAULT_HOSTS[:3])
```

如果包内 JSON 缺失或格式坏了，客户端会退回代码内置主站列表。

## 长时间空闲

真实 socket 默认每 30 秒发一次 `0x0004` 心跳。普通短脚本不用处理。

需要调整：

```python
from eltdx import TdxClient

client = TdxClient(heartbeat_interval=60)
```

需要关闭：

```python
client = TdxClient(heartbeat_interval=None)
```

程序需要创建较多客户端实例时，使用 `with TdxClient(...) as client:` 自动关闭连接和后台线程。

## 请求超时

超时通常有三种原因：

| 现象 | 处理 |
| --- | --- |
| 第一次请求就超时 | 换主站或打开 `probe_hosts=True` |
| 偶发超时 | 增大 `timeout`，例如 `timeout=8` |
| 长时间运行后超时 | 确认没有忘记关闭旧客户端，必要时降低连接池数量 |

## 字段对不上

需要看原始 payload 时，打开 `include_raw=True`：

```python
from eltdx import TdxClient

with TdxClient(timeout=3) as client:
    kline = client.get_kline("day", "sz000001", count=5, include_raw=True)

print(kline.raw_payload.hex())
print(kline.bars[0].record_hex)
```

常见 raw 字段：

| 字段 | 意思 |
| --- | --- |
| `raw_payload` | 当前响应 payload 原始 bytes |
| `record_hex` | 单条记录原始十六进制 |
| `decoded_payload` | 少数 XOR 编码接口解码后的 payload |

这些字段主要用于抓包对照和排查解析问题。

## 推送队列

部分接口会有服务端主动推送帧。真实 transport 会把没匹配到请求的帧放进本地队列：

```python
from eltdx import TdxClient

with TdxClient(timeout=3) as client:
    client.get_quote("sz000001")
    print(client.transport.pending_push_count)
    print(client.transport.drain_pushes(parse=True))
```

主动查询可以直接使用 `client.quotes.refresh()`，服务端主动推送帧可以通过 `poll_push()` / `drain_pushes()` 读取。
