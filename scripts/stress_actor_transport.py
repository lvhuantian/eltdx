"""Deterministic loopback stress, soak, and resource measurements."""

from __future__ import annotations

import argparse
import ctypes
import gc
from ctypes import wintypes
import hashlib
import json
import os
import platform
import socket
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from eltdx.exceptions import PushOverflowError  # noqa: E402
from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_HEARTBEAT, TYPE_REFRESH_STREAM, TYPE_SECURITY_COUNT  # noqa: E402
from eltdx.transport import PooledSocketTransport, SocketTransport  # noqa: E402


class StressServer:
    def __init__(self, *, push_every: int = 0, close_every: int = 0, response_delay: float = 0.0) -> None:
        self.push_every = push_every
        self.close_every = close_every
        self.response_delay = response_delay
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._stop = threading.Event()
        self._condition = threading.Condition()
        self._thread: threading.Thread | None = None
        self._workers: list[threading.Thread] = []
        self._connections: set[socket.socket] = set()
        self.accept_count = 0
        self.business_requests = 0
        self.push_frames = 0
        self.heartbeat_requests = 0
        self.active_business = 0
        self.max_business_active = 0
        self.errors: list[str] = []
        self.host = ""

    def __enter__(self) -> StressServer:
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen()
        address, port = self._listener.getsockname()
        self.host = f"{address}:{port}"
        self._thread = threading.Thread(target=self._accept, name="eltdx-stress-server", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._stop.set()
        self._listener.close()
        with self._condition:
            connections = tuple(self._connections)
        for conn in connections:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                conn.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2)
        for worker in self._workers:
            worker.join(timeout=2)
        alive = [worker.name for worker in self._workers if worker.is_alive()]
        if exc_type is None and alive:
            raise RuntimeError(f"stress server workers did not stop: {alive!r}")
        if exc_type is None and self.errors:
            raise RuntimeError(f"stress server errors: {self.errors!r}")
        self._workers.clear()
        self._connections.clear()
        self._thread = None

    def wait_for_business(self, count: int, timeout: float = 2.0) -> bool:
        with self._condition:
            return self._condition.wait_for(lambda: self.business_requests >= count, timeout=timeout)

    def _accept(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._listener.accept()
            except OSError:
                return
            conn.settimeout(5)
            with self._condition:
                self.accept_count += 1
                self._connections.add(conn)
                index = self.accept_count
                self._condition.notify_all()
            worker = threading.Thread(target=self._serve, args=(conn,), name=f"eltdx-stress-conn-{index}", daemon=True)
            self._workers.append(worker)
            worker.start()

    def _serve(self, conn: socket.socket) -> None:
        try:
            with conn:
                while not self._stop.is_set():
                    msg_id, msg_type, _ = _read_request(conn)
                    if msg_type == TYPE_HANDSHAKE:
                        conn.sendall(_response(msg_id, msg_type, _handshake_payload()))
                        continue
                    if msg_type == TYPE_HEARTBEAT:
                        with self._condition:
                            self.heartbeat_requests += 1
                        conn.sendall(_response(msg_id, msg_type, bytes.fromhex("0000000000008f173501")))
                        continue
                    if msg_type != TYPE_SECURITY_COUNT:
                        raise RuntimeError(f"unexpected stress command: {msg_type:#x}")
                    with self._condition:
                        self.business_requests += 1
                        sequence = self.business_requests
                        self.active_business += 1
                        self.max_business_active = max(self.max_business_active, self.active_business)
                        self._condition.notify_all()
                    try:
                        if self.response_delay:
                            time.sleep(self.response_delay)
                        if self.push_every and sequence % self.push_every == 0:
                            conn.sendall(_response(0xF0000000 | (sequence & 0x0FFFFFFF), TYPE_REFRESH_STREAM, b"\x93\x93"))
                            with self._condition:
                                self.push_frames += 1
                        conn.sendall(_response(msg_id, msg_type, (23285).to_bytes(2, "little")))
                    finally:
                        with self._condition:
                            self.active_business -= 1
                    if self.close_every and sequence % self.close_every == 0:
                        return
        except (EOFError, OSError, TimeoutError):
            return
        except BaseException as exc:
            self.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            with self._condition:
                self._connections.discard(conn)


def run_generation_stress(count: int) -> dict[str, Any]:
    before_threads = _actor_threads()
    before_resources = _resource_count()
    with StressServer(close_every=1) as server:
        transport = SocketTransport(hosts=[server.host], timeout=5, heartbeat_interval=None)
        started = time.perf_counter()
        actor_identity = None
        for _ in range(count):
            if transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) != 23285:
                raise AssertionError("generation stress response mismatch")
            runtime = transport._runtime
            if runtime is None or runtime.actor_thread is None:
                raise AssertionError("Actor runtime disappeared")
            if actor_identity is None:
                actor_identity = runtime.actor_thread.ident
            elif runtime.actor_thread.ident != actor_identity:
                raise AssertionError("Actor thread identity changed across generations")
        elapsed = time.perf_counter() - started
        diagnostics = transport.diagnostics
        generation_counter = diagnostics.actor.tcp_generation if diagnostics.actor is not None else 0
        stale_events = diagnostics.actor.stale_event_count if diagnostics.actor is not None else -1
        transport.close()
        result = {
            "requests": count,
            "seconds": round(elapsed, 6),
            "throughput_rps": round(count / elapsed, 3),
            "actor_identity": actor_identity,
            "generation_counter": generation_counter,
            "server_accepts": server.accept_count,
            "stale_events": stale_events,
        }
    del server
    result.update(
        resource_before=before_resources,
        resource_after=_resource_after_gc(),
        actor_threads_before=len(before_threads),
        actor_threads_after=len(_actor_threads()),
    )
    return result


def run_mixed_stress(
    requests: int,
    *,
    pool_size: int = 4,
    concurrency: int = 100,
    push_every: int = 97,
    close_every: int = 1000,
    response_delay: float = 0.0005,
) -> dict[str, Any]:
    before_resources = _resource_count()
    with StressServer(push_every=push_every, close_every=close_every, response_delay=response_delay) as server:
        pool = PooledSocketTransport(
            hosts=[server.host],
            timeout=10,
            pool_size=pool_size,
            heartbeat_interval=None,
            max_pending_requests=max(concurrency * 2, 256),
            push_queue_size=1024,
            push_queue_bytes=8 * 1024 * 1024,
        )

        def execute(_: int) -> int:
            return pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"})

        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            values = list(executor.map(execute, range(requests)))
        elapsed = time.perf_counter() - started
        del executor
        if any(value != 23285 for value in values):
            raise AssertionError("mixed stress response mismatch")
        diagnostics = pool.diagnostics
        gap_reported = False
        try:
            pool.drain_pushes()
        except PushOverflowError:
            gap_reported = True
            pool.drain_pushes()
        actor_generations = [actor.tcp_generation for actor in diagnostics.actors]
        stale_events = sum(actor.stale_event_count for actor in diagnostics.actors)
        broker = diagnostics.broker
        pool.close()
        result = {
            "requests": requests,
            "pool_size": pool_size,
            "concurrency": concurrency,
            "seconds": round(elapsed, 6),
            "throughput_rps": round(requests / elapsed, 3),
            "server_requests": server.business_requests,
            "server_accepts": server.accept_count,
            "server_max_active": server.max_business_active,
            "push_frames_sent": server.push_frames,
            "push_dropped": diagnostics.push_dropped,
            "push_gap_reported": gap_reported,
            "actor_generations": actor_generations,
            "stale_events": stale_events,
            "broker_waiters": broker.waiter_count if broker is not None else -1,
            "broker_leases": broker.active_leases if broker is not None else -1,
            "resource_before": before_resources,
            "actor_threads_after": len(_actor_threads()),
        }
    del server
    result["resource_after"] = _resource_after_gc()
    return result


def run_close_samples(samples: int, *, pool_size: int = 4, loaded: bool = False) -> dict[str, Any]:
    latencies: list[float] = []
    with StressServer(response_delay=0.05 if loaded else 0.0) as server:
        handled = 0
        for _ in range(samples):
            pool = PooledSocketTransport(hosts=[server.host], timeout=2, pool_size=pool_size, heartbeat_interval=None)
            pool.connect()
            futures = []
            executor = None
            if loaded:
                executor = ThreadPoolExecutor(max_workers=pool_size)
                futures = [executor.submit(pool.execute, TYPE_SECURITY_COUNT, {"market": "sz"}) for _ in range(pool_size)]
                handled += pool_size
                if not server.wait_for_business(handled, timeout=2):
                    raise AssertionError("loaded close requests did not reach server")
            started = time.perf_counter()
            pool.close()
            latencies.append((time.perf_counter() - started) * 1000)
            if executor is not None:
                for future in futures:
                    try:
                        future.result(timeout=2)
                    except BaseException:
                        pass
                executor.shutdown()
    ordered = sorted(latencies)
    return {
        "samples": samples,
        "loaded": loaded,
        "p50_ms": round(statistics.median(ordered), 4),
        "p95_ms": round(_percentile(ordered, 0.95), 4),
        "p99_ms": round(_percentile(ordered, 0.99), 4),
        "max_ms": round(max(ordered), 4),
    }


def run_heartbeat_impact(requests: int, *, trials: int = 3) -> dict[str, Any]:
    if trials < 3 or trials % 2 == 0:
        raise ValueError("heartbeat impact trials must be an odd number >= 3")

    def run(interval: float | None) -> dict[str, Any]:
        with StressServer(response_delay=0.001) as server:
            pool = PooledSocketTransport(
                hosts=[server.host], timeout=10, pool_size=4, heartbeat_interval=interval, max_pending_requests=256
            )
            pool.connect()
            with ThreadPoolExecutor(max_workers=100) as executor:
                warmup = list(
                    executor.map(
                        lambda _: pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"}),
                        range(min(500, max(100, requests // 10))),
                    )
                )
                heartbeat_before = server.heartbeat_requests
                started = time.perf_counter()
                values = list(executor.map(lambda _: pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"}), range(requests)))
                elapsed = time.perf_counter() - started
            if any(value != 23285 for value in warmup) or any(value != 23285 for value in values):
                raise AssertionError("heartbeat workload response mismatch")
            pool.close()
            return {
                "seconds": round(elapsed, 6),
                "throughput_rps": round(requests / elapsed, 3),
                "heartbeat_requests": server.heartbeat_requests - heartbeat_before,
            }

    samples: dict[str, list[dict[str, Any]]] = {"without_heartbeat": [], "with_heartbeat": []}
    for trial in range(trials):
        intervals = (None, 0.01) if trial % 2 == 0 else (0.01, None)
        for interval in intervals:
            key = "without_heartbeat" if interval is None else "with_heartbeat"
            samples[key].append(run(interval))

    def summarize(values: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "seconds": round(statistics.median(item["seconds"] for item in values), 6),
            "throughput_rps": round(statistics.median(item["throughput_rps"] for item in values), 3),
            "heartbeat_requests": max(item["heartbeat_requests"] for item in values),
            "heartbeat_requests_total": sum(item["heartbeat_requests"] for item in values),
            "samples": values,
        }

    baseline = summarize(samples["without_heartbeat"])
    active = summarize(samples["with_heartbeat"])
    return {
        "requests": requests,
        "trials": trials,
        "without_heartbeat": baseline,
        "with_heartbeat": active,
        "throughput_ratio": round(active["throughput_rps"] / baseline["throughput_rps"], 6),
    }


def run_idle_cpu_sample(seconds: float = 0.5) -> dict[str, Any]:
    transport = SocketTransport(hosts=["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    transport._ensure_runtime()
    started_cpu = time.process_time()
    started = time.perf_counter()
    threading.Event().wait(seconds)
    elapsed = time.perf_counter() - started
    cpu = time.process_time() - started_cpu
    transport.close()
    return {"seconds": round(elapsed, 6), "process_cpu_seconds": round(cpu, 6), "cpu_ratio": round(cpu / elapsed, 6)}


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


def _actor_threads() -> list[threading.Thread]:
    return [thread for thread in threading.enumerate() if thread.name.startswith("eltdx-7709-actor-")]


def _resource_count() -> int | None:
    if os.name == "nt":
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        get_count = kernel32.GetProcessHandleCount
        get_count.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
        get_count.restype = wintypes.BOOL
        count = wintypes.DWORD()
        if get_count(kernel32.GetCurrentProcess(), ctypes.byref(count)):
            return int(count.value)
        return None
    fd_path = Path("/proc/self/fd")
    return len(tuple(fd_path.iterdir())) if fd_path.exists() else None


def _resource_after_gc() -> int | None:
    gc.collect()
    return _resource_count()


def _percentile(values: list[float], fraction: float) -> float:
    return values[min(len(values) - 1, int((len(values) - 1) * fraction))]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--generations", type=int, default=0)
    parser.add_argument("--requests", type=int, default=0)
    parser.add_argument("--pool-size", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--close-samples", type=int, default=0)
    parser.add_argument("--heartbeat-requests", type=int, default=0)
    parser.add_argument("--idle-seconds", type=float, default=0.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    source_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    result: dict[str, Any] = {
        "schema": 1,
        "workload_sha256": source_hash,
        "platform": platform.platform(),
        "python": platform.python_version(),
    }
    if args.generations:
        result["generation_stress"] = run_generation_stress(args.generations)
    if args.requests:
        result["mixed_stress"] = run_mixed_stress(
            args.requests, pool_size=args.pool_size, concurrency=args.concurrency
        )
    if args.close_samples:
        result["idle_close"] = run_close_samples(args.close_samples, pool_size=args.pool_size)
        result["loaded_close"] = run_close_samples(args.close_samples, pool_size=args.pool_size, loaded=True)
    if args.heartbeat_requests:
        result["heartbeat_impact"] = run_heartbeat_impact(args.heartbeat_requests)
    if args.idle_seconds:
        result["idle_cpu"] = run_idle_cpu_sample(args.idle_seconds)
    encoded = json.dumps(result, ensure_ascii=True, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")


if __name__ == "__main__":
    main()
