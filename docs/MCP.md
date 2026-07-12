# MCP 工具

`eltdx` 提供一个 MCP stdio 服务，方便在支持 MCP 的客户端里直接调用行情、K 线、F10 和题材相关工具。

## 启动

安装后运行：

```bash
pip install "eltdx[mcp]"
eltdx-mcp
```

也可以在源码目录运行：

```bash
pip install -e ".[mcp]"
eltdx-mcp
```

未安装到当前 Python 环境时，需要让 Python 找到 `src/` 目录：

PowerShell：

```powershell
$env:PYTHONPATH = "src"
python -m eltdx.mcp
```

bash：

```bash
PYTHONPATH=src python -m eltdx.mcp
```

MCP 服务默认走 stdio，不额外开启 HTTP 端口。

## 工具列表 {#mcp-tools}

| 工具 | 作用 |
| --- | --- |
| `eltdx_quote` | 查询一个或多个股票的行情快照 |
| `eltdx_kline` | 查询 K 线 / 周期线，支持复权参数 |
| `eltdx_stock_profile` | 汇总股票表头信息，合并行情、代码表和财务基础信息 |
| `eltdx_stock_topics` | 查询某只股票的全部题材 / 概念板块 |
| `eltdx_topic_stocks` | 查询某个题材 / 概念板块里的股票 |
| `eltdx_company_profile` | 查询 F10 公司概况 |
| `eltdx_hot_topics` | 查询 F10 热点题材明细 |
| `eltdx_auction_0925` | 查询指定日期 09:25 竞价成交快照 |
| `eltdx_docs_index` | 返回项目主要文档入口 |

## 调用示例

查询行情：

```json
{
  "codes": ["sz000001", "sh600000"],
  "timeout": 3
}
```

查询 K 线：

```json
{
  "code": "sz000001",
  "period": "day",
  "count": 120,
  "adjust": "qfq"
}
```

查询个股题材：

```json
{
  "code": "000034",
  "timeout": 3
}
```

查询题材成分股：

```json
{
  "seed_code": "000034",
  "topic_name": "存储芯片",
  "sort_by": "zdf"
}
```

## 连接参数

| 参数 | 说明 |
| --- | --- |
| `timeout` | 请求超时时间，默认 `8.0` 秒 |
| `host` | 指定单个 `7709` 主站，例如 `"116.205.183.150:7709"` |

F10 工具走 `7615/TQLEX` HTTP 网关，不需要 7709 握手。
