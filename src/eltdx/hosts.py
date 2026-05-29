"""Default 7709 quote server hosts."""

from __future__ import annotations

import json
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from importlib import resources
from typing import Any

SERVER_FILE = "tdx_server.json"
DEFAULT_PROBE_TIMEOUT = 1.2
DEFAULT_PROBE_WORKERS = 32


@dataclass(frozen=True, slots=True)
class HostProbeResult:
    host: str
    ok: bool
    latency_ms: float | None = None
    error: str | None = None


FALLBACK_HOSTS: tuple[str, ...] = (
    "116.205.183.150:7709",
    "116.205.171.132:7709",
    "111.230.186.52:7709",
    "129.204.230.128:7709",
    "116.205.163.254:7709",
    "110.41.2.72:7709",
    "159.75.29.111:7709",
    "43.139.95.83:7709",
    "175.178.128.227:7709",
    "110.41.147.114:7709",
    "124.71.9.153:7709",
    "81.71.32.47:7709",
    "43.139.18.171:7709",
    "119.97.185.59:7709",
    "123.60.70.228:7709",
    "123.60.73.44:7709",
    "124.70.199.56:7709",
    "175.178.112.197:7709",
    "101.33.225.16:7709",
    "124.71.187.122:7709",
    "124.71.187.72:7709",
    "111.229.247.189:7709",
    "121.36.225.169:7709",
    "150.158.160.2:7709",
    "123.60.164.122:7709",
    "49.232.15.141:7709",
    "122.51.120.217:7709",
    "111.231.113.208:7709",
    "124.223.163.242:7709",
    "62.234.50.143:7709",
    "101.35.121.35:7709",
    "101.42.240.54:7709",
    "101.43.159.194:7709",
    "81.70.151.186:7709",
    "82.156.174.84:7709",
    "123.60.84.66:7709",
    "120.53.8.251:7709",
    "124.70.133.119:7709",
    "118.25.98.114:7709",
    "122.51.232.182:7709",
    "101.42.164.241:7709",
    "152.136.191.169:7709",
    "82.156.214.79:7709",
)


def normalize_host(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    host = value.strip()
    if not host or ":" not in host:
        return None
    address, port = host.rsplit(":", 1)
    address = address.strip()
    port = port.strip()
    if not address or not port.isdigit():
        return None
    return f"{address}:{int(port)}"


def unique_hosts(values: list[Any] | tuple[Any, ...]) -> list[str]:
    hosts: list[str] = []
    for value in values:
        host = normalize_host(value)
        if host is not None and host not in hosts:
            hosts.append(host)
    return hosts


def load_server_config() -> dict[str, Any]:
    """Load packaged 7709 host configuration.

    The file is deliberately optional at runtime. If a downstream package strips
    it out, the client still falls back to the built-in host list below.
    """

    try:
        content = resources.files("eltdx").joinpath(SERVER_FILE).read_text(encoding="utf-8")
        data = json.loads(content)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return data if isinstance(data, dict) else {}


def load_server_hosts() -> list[str]:
    """Return normalized hosts from the packaged server config."""

    data = load_server_config()
    hosts = data.get("hosts")
    if isinstance(hosts, list):
        return unique_hosts(hosts)

    values: list[Any] = [data.get("current_host")]
    for key in ("manual_hosts", "imported_hosts"):
        item = data.get(key, [])
        if isinstance(item, list):
            values.extend(item)
    return unique_hosts(values)


def probe_host(host: str, *, timeout: float = DEFAULT_PROBE_TIMEOUT) -> HostProbeResult:
    """Measure whether a 7709 host accepts TCP connections."""

    normalized = normalize_host(host)
    if normalized is None:
        return HostProbeResult(host=str(host), ok=False, error="invalid host")

    address, port_text = normalized.rsplit(":", 1)
    started = time.perf_counter()
    try:
        with socket.create_connection((address, int(port_text)), timeout=timeout):
            latency_ms = round((time.perf_counter() - started) * 1000.0, 2)
            return HostProbeResult(host=normalized, ok=True, latency_ms=latency_ms)
    except OSError as exc:
        return HostProbeResult(host=normalized, ok=False, error=type(exc).__name__)


def probe_hosts(
    hosts: list[str] | tuple[str, ...],
    *,
    timeout: float = DEFAULT_PROBE_TIMEOUT,
    max_workers: int = DEFAULT_PROBE_WORKERS,
) -> list[HostProbeResult]:
    """Probe many hosts concurrently."""

    candidates = unique_hosts(list(hosts))
    if not candidates:
        return []

    worker_count = min(max(1, int(max_workers)), len(candidates))
    results: list[HostProbeResult] = []
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="eltdx-probe") as executor:
        futures = [executor.submit(probe_host, host, timeout=timeout) for host in candidates]
        for future in as_completed(futures):
            results.append(future.result())
    return results


def sort_hosts_by_latency(
    hosts: list[str] | tuple[str, ...],
    *,
    timeout: float = DEFAULT_PROBE_TIMEOUT,
    max_workers: int = DEFAULT_PROBE_WORKERS,
) -> list[str]:
    """Return reachable hosts first, ordered by TCP latency."""

    candidates = unique_hosts(list(hosts))
    results = probe_hosts(candidates, timeout=timeout, max_workers=max_workers)
    reachable = sorted(
        (result for result in results if result.ok),
        key=lambda result: (result.latency_ms if result.latency_ms is not None else float("inf"), candidates.index(result.host)),
    )
    reachable_hosts = {result.host for result in reachable}
    unreachable = [host for host in candidates if host not in reachable_hosts]
    return [result.host for result in reachable] + unreachable


DEFAULT_HOSTS: tuple[str, ...] = tuple(load_server_hosts() or FALLBACK_HOSTS)
