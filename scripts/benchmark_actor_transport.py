"""Reproducible loopback benchmark for old and Actor 7709 transports."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import socket
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = Path(os.environ.get("ELTDX_SOURCE_ROOT", ROOT)).resolve()
sys.path.insert(0, str(SOURCE_ROOT / "src"))

from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_SECURITY_COUNT  # noqa: E402
from eltdx.transport import PooledSocketTransport  # noqa: E402


class BenchmarkServer:
    def __init__(self, response_delay: float) -> None:
        self._delay = response_delay
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._closing = threading.Event()
        self._thread: threading.Thread | None = None
        self._workers: list[threading.Thread] = []
        self._lock = threading.Lock()
        self._active = 0
        self.max_active = 0
        self.requests = 0
        self.errors: list[str] = []
        self.host = ""

    def __enter__(self) -> BenchmarkServer:
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen()
        address, port = self._listener.getsockname()
        self.host = f"{address}:{port}"
        self._thread = threading.Thread(target=self._accept, name="eltdx-benchmark-server", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._closing.set()
        self._listener.close()
        if self._thread is not None:
            self._thread.join(timeout=2)
        for worker in self._workers:
            worker.join(timeout=2)
        if exc_type is None and self.errors:
            raise RuntimeError(f"benchmark server errors: {self.errors!r}")

    def _accept(self) -> None:
        while not self._closing.is_set():
            try:
                conn, _ = self._listener.accept()
            except OSError:
                return
            worker = threading.Thread(target=self._serve, args=(conn,), daemon=True)
            self._workers.append(worker)
            worker.start()

    def _serve(self, conn: socket.socket) -> None:
        try:
            with conn:
                while not self._closing.is_set():
                    msg_id, msg_type, _ = _read_request(conn)
                    if msg_type == TYPE_HANDSHAKE:
                        payload = _handshake_payload()
                    elif msg_type == TYPE_SECURITY_COUNT:
                        payload = (23285).to_bytes(2, "little")
                    else:
                        raise RuntimeError(f"unexpected benchmark command: {msg_type:#x}")
                    with self._lock:
                        self._active += 1
                        self.max_active = max(self.max_active, self._active)
                    try:
                        if self._delay:
                            time.sleep(self._delay)
                        conn.sendall(_response(msg_id, msg_type, payload))
                    finally:
                        with self._lock:
                            self._active -= 1
                            self.requests += 1
        except (EOFError, OSError):
            return
        except BaseException as exc:
            self.errors.append(f"{type(exc).__name__}: {exc}")


def _read_exact(conn: socket.socket, size: int) -> bytes:
    output = bytearray()
    while len(output) < size:
        chunk = conn.recv(size - len(output))
        if not chunk:
            raise EOFError
        output.extend(chunk)
    return bytes(output)


def _read_request(conn: socket.socket) -> tuple[int, int, bytes]:
    header = _read_exact(conn, 12)
    length = int.from_bytes(header[6:8], "little")
    return int.from_bytes(header[1:5], "little"), int.from_bytes(header[10:12], "little"), _read_exact(conn, length - 2)


def _response(msg_id: int, msg_type: int, payload: bytes) -> bytes:
    return b"\xb1\xcb\x74\x00\x00" + msg_id.to_bytes(4, "little") + b"\x00" + msg_type.to_bytes(2, "little") + len(payload).to_bytes(2, "little") * 2 + payload


def _handshake_payload() -> bytes:
    payload = bytearray(189)
    payload[1:3] = (2026).to_bytes(2, "little")
    payload[3:9] = bytes((27, 5, 30, 10, 0, 0))
    payload[42:46] = (20260527).to_bytes(4, "little")
    payload[50:54] = (20260527).to_bytes(4, "little")
    return bytes(payload)


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def run_case(pool_size: int, concurrency: int, requests: int, delay: float) -> dict[str, Any]:
    with BenchmarkServer(delay) as server:
        transport = PooledSocketTransport(hosts=[server.host], timeout=10, pool_size=pool_size, heartbeat_interval=None)
        transport.connect()
        for _ in range(pool_size):
            assert transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 23285

        latencies: list[float] = []
        latency_lock = threading.Lock()

        def execute(_: int) -> None:
            started = time.perf_counter()
            value = transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"})
            elapsed = time.perf_counter() - started
            if value != 23285:
                raise AssertionError(value)
            with latency_lock:
                latencies.append(elapsed)

        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            list(executor.map(execute, range(requests)))
        elapsed = time.perf_counter() - started
        transport.close()

        return {
            "pool_size": pool_size,
            "concurrency": concurrency,
            "requests": requests,
            "seconds": round(elapsed, 6),
            "throughput_rps": round(requests / elapsed, 3),
            "latency_p50_ms": round(statistics.median(latencies) * 1000, 4),
            "latency_p99_ms": round(_percentile(latencies, 0.99) * 1000, 4),
            "server_max_active": server.max_active,
        }


def _git_identity() -> dict[str, Any]:
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=SOURCE_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=SOURCE_ROOT,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        )
    except (OSError, subprocess.CalledProcessError):
        return {"implementation_sha": None, "implementation_dirty": None}
    return {"implementation_sha": sha, "implementation_dirty": dirty}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--delay-ms", type=float, default=1.0)
    parser.add_argument("--acceptance", action="store_true")
    parser.add_argument("--sequential-requests", type=int, default=10_000)
    parser.add_argument("--concurrent-requests", type=int, default=100_000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    source = Path(__file__).read_bytes()
    result = {
        "schema": 2,
        "label": args.label,
        "workload_sha256": hashlib.sha256(source).hexdigest(),
        "source_root": str(SOURCE_ROOT),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "delay_ms": args.delay_ms,
        **_git_identity(),
    }
    if args.acceptance:
        result["cases"] = [
            run_case(1, 1, args.sequential_requests, args.delay_ms / 1000),
            run_case(4, 100, args.concurrent_requests, args.delay_ms / 1000),
        ]
    else:
        result["requests_per_case"] = args.requests
        result["cases"] = [
            run_case(pool_size, concurrency, args.requests, args.delay_ms / 1000)
            for pool_size in (1, 2, 4)
            for concurrency in (1, 10, 100)
        ]
    encoded = json.dumps(result, ensure_ascii=True, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")


if __name__ == "__main__":
    main()
