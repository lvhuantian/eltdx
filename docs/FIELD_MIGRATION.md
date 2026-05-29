# 历史字段对照

这份文档给从历史版本接入 `eltdx 1.0` 的用户看。当前版本优先保持字段语义清楚。

## raw 调试字段

旧版常见写法：

```python
data = client.get_kline("day", "sz000001", include_raw=True)
print(data.raw_frame_hex)
print(data.raw_payload_hex)
```

新版模型主要保留 payload 和单条记录原文：

```python
data = client.get_kline("day", "sz000001", include_raw=True)
print(data.raw_payload.hex())
print(data.bars[0].record_hex)
```

对照：

| 旧字段 | 新字段 / 新写法 | 说明 |
| --- | --- | --- |
| `raw_payload_hex` | `raw_payload.hex()` | payload 原始十六进制 |
| `items[].raw_hex` | `bars[].record_hex` / `points[].record_hex` / `ticks[].record_hex` / `records[].record_hex` | 单条记录原始十六进制 |
| `raw_frame_hex` | 查看 transport 返回帧或抓包样本 | 业务模型默认保留 payload；完整 TCP 响应帧更适合在 transport / 抓包层查看 |

常规字段排查优先看 `raw_payload` 和单条 `record_hex`；需要完整帧时，在 transport / 抓包层取更清楚。

## 返回集合字段

旧版很多响应统一叫 `items`。新版按业务换成更直观的名字。

| 旧字段 | 新字段 |
| --- | --- |
| K 线 `items` | `bars` |
| 分时 `items` | `points` |
| 成交明细 `items` | `ticks` |
| 股本变迁 `items` | `records`，同时保留 `items` 属性 |
| 分类行情 `items` | `records` |

## 价格和涨跌幅

| 旧字段习惯 | 新字段 / 新写法 |
| --- | --- |
| `last_price` | `last_price` |
| `last_close_price` | `pre_close_price` |
| `change_percent` | `change_pct` |
| `amount` | `amount` |
| `volume` | 快照用 `total_hand`，成交明细用 `volume`，K 线用 `volume_lots` |

## 代码字段

| 旧字段习惯 | 新字段 / 新写法 |
| --- | --- |
| `code` 带市场前缀 | `full_code` |
| 市场 | `exchange` |
| 六位代码 | `code` |

新版更推荐内部拆开保存：

```python
item.exchange   # "sz"
item.code       # "000001"
item.full_code  # "sz000001"
```

## 复权

旧版普通复权更多依赖本地因子计算。新版优先使用 `0x052d` 服务端复权参数：

```python
client.get_adjusted_kline("day", "sz000001", adjust="qfq")
client.get_adjusted_kline("day", "sz000001", adjust="hfq")
```

本地复权因子仍保留：

```python
client.get_factors("sz000001")
client.get_local_adjusted_kline_all("day", "sz000001", adjust="qfq")
```

## 缓存

新版会缓存低频数据：

| 数据 | 默认缓存 |
| --- | --- |
| 代码数量 | 是 |
| 全量代码表 | 是 |
| 股本变迁 / GBBQ | 是，不缓存 `include_raw=True` 的结果 |
| 财务批量完整结果 | 是 |
| 行情快照、分时、成交明细、K 线 | 否 |

需要强制重新请求：

```python
client.get_codes_all("sz", refresh=True)
client.get_gbbq("sz000001", refresh=True)
client.get_finance_batch(["sz000001"], refresh=True)
```

需要清空全部缓存：

```python
client.clear_cache()
```
