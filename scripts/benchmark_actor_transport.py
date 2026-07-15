"""Reproducible loopback benchmark for old and Actor 7709 transports."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time
from concurrent.futures import ALL_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = Path(os.environ.get("ELTDX_SOURCE_ROOT", ROOT)).resolve()
sys.path.insert(0, str(SOURCE_ROOT / "src"))

from eltdx.models import FileContentChunk  # noqa: E402
from eltdx.protocol.constants import TYPE_FILE_CONTENT, TYPE_HANDSHAKE  # noqa: E402
from eltdx.transport import PooledSocketTransport  # noqa: E402


FIFO_PROTOCOL = "fifo-v2"
_BENCHMARK_PATH = "eltdx-performance.bin"
_BENCHMARK_VALUE = struct.Struct("<IIII")
_COMPLETION_RECORD = struct.Struct("<IIIIIIIII")
_SETUP_TOKEN_BASE = 0xE0000000
_WARMUP_TOKEN_BASE = 0xC0000000


def fifo_v2_config() -> dict[str, Any]:
    return {
        "delay_ns": 5_000_000,
        "response_contract": {
            "command": "file_content",
            "path": _BENCHMARK_PATH,
            "content_fields": [
                "request_token",
                "measurement_epoch",
                "connection_id",
                "attempt_sequence",
            ],
            "completion_record_fields": [
                "requested_token",
                "snapshot_token",
                "echoed_token",
                "response_epoch",
                "response_connection_id",
                "response_attempt_sequence",
                "expected_epoch",
                "expected_connection_id",
                "expected_attempt_sequence",
            ],
        },
        "cases": {
            "sequential": {
                "mode": "saturated",
                "pool_size": 1,
                "concurrency": 1,
                "requests": 10_000,
                "warmup_requests": 1_000,
                "latency_metric": "call_latency_ns",
            },
            "saturated": {
                "mode": "saturated",
                "pool_size": 4,
                "concurrency": 100,
                "requests": 100_000,
                "warmup_requests": 1_000,
                "latency_metric": "call_latency_ns",
            },
            "no_backlog": {
                "mode": "fixed_cohort",
                "pool_size": 4,
                "cohort_size": 4,
                "measured_cohorts": 2_500,
                "warmup_cohorts": 100,
                "requests": 10_000,
                "latency_metric": "call_latency_ns",
            },
            "contended_wave": {
                "mode": "fixed_cohort",
                "pool_size": 4,
                "cohort_size": 100,
                "measured_cohorts": 50,
                "warmup_cohorts": 10,
                "requests": 5_000,
                "latency_metric": "cohort_latency_ns",
            },
        },
    }


class BenchmarkServer:
    def __init__(self, response_delay: float) -> None:
        self._delay = response_delay
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._closing = threading.Event()
        self._thread: threading.Thread | None = None
        self._workers: list[threading.Thread] = []
        self._lock = threading.Lock()
        self._idle = threading.Condition(self._lock)
        self._connections: set[socket.socket] = set()
        self._active = 0
        self.max_active = 0
        self.requests = 0
        self.accept_count = 0
        self.measurement_epoch = 0
        self.attempt_sequence = 0
        self.expected_provenance: dict[int, tuple[int, int, int]] = {}
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
        self.abort()
        if self._thread is not None:
            self._thread.join(timeout=2)
            if self._thread.is_alive():
                self.errors.append("benchmark accept thread still alive")
        for worker in self._workers:
            worker.join(timeout=2)
        alive = [worker.name for worker in self._workers if worker.is_alive()]
        if alive:
            self.errors.append(f"benchmark server workers still alive: {alive!r}")
        if exc_type is None and self.errors:
            raise RuntimeError(f"benchmark server errors: {self.errors!r}")

    def abort(self) -> None:
        self._closing.set()
        try:
            self._listener.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._listener.close()
        with self._lock:
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

    def reset_measurement(self) -> None:
        with self._idle:
            if not self._idle.wait_for(lambda: self._active == 0, timeout=2.0):
                raise TimeoutError(f"benchmark server retained {self._active} active requests")
            self.max_active = 0
            self.requests = 0
            self.measurement_epoch += 1
            self.attempt_sequence = 0
            self.expected_provenance.clear()

    def wait_for_idle(self, timeout: float = 2.0) -> None:
        with self._idle:
            if not self._idle.wait_for(lambda: self._active == 0, timeout=timeout):
                raise TimeoutError(f"benchmark server retained {self._active} active requests")

    def provenance_snapshot(self) -> tuple[int, int, dict[int, tuple[int, int, int]]]:
        with self._idle:
            return self.accept_count, self.attempt_sequence, dict(self.expected_provenance)

    def _accept(self) -> None:
        while not self._closing.is_set():
            try:
                conn, _ = self._listener.accept()
            except OSError:
                return
            with self._lock:
                if self._closing.is_set():
                    conn.close()
                    return
                self._connections.add(conn)
                self.accept_count += 1
                connection_id = self.accept_count
            worker = threading.Thread(target=self._serve, args=(conn, connection_id), daemon=True)
            self._workers.append(worker)
            worker.start()

    def _serve(self, conn: socket.socket, connection_id: int) -> None:
        try:
            with conn:
                while not self._closing.is_set():
                    msg_id, msg_type, request = _read_request(conn)
                    if msg_type == TYPE_HANDSHAKE:
                        conn.sendall(_response(msg_id, msg_type, _handshake_payload()))
                        continue
                    if msg_type != TYPE_FILE_CONTENT:
                        raise RuntimeError(f"unexpected benchmark command: {msg_type:#x}")
                    token = _parse_benchmark_request(request)
                    with self._idle:
                        if token in self.expected_provenance:
                            raise RuntimeError(f"duplicate benchmark request token: {token}")
                        self._active += 1
                        self.max_active = max(self.max_active, self._active)
                        self.attempt_sequence += 1
                        provenance = (self.measurement_epoch, connection_id, self.attempt_sequence)
                        self.expected_provenance[token] = provenance
                    try:
                        if self._delay:
                            time.sleep(self._delay)
                    finally:
                        with self._idle:
                            self._active -= 1
                            self.requests += 1
                            if self._active == 0:
                                self._idle.notify_all()
                    content = _BENCHMARK_VALUE.pack(token, *provenance)
                    payload = len(content).to_bytes(4, "little") + content
                    conn.sendall(_response(msg_id, msg_type, payload))
        except (EOFError, OSError):
            return
        except BaseException as exc:
            self.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            with self._lock:
                self._connections.discard(conn)


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


def _parse_benchmark_request(request: bytes) -> int:
    if len(request) < 8:
        raise RuntimeError("truncated benchmark file-content request")
    token = int.from_bytes(request[:4], "little")
    requested_size = int.from_bytes(request[4:8], "little")
    path = request[8:].split(b"\x00", 1)[0]
    if requested_size != _BENCHMARK_VALUE.size or path != _BENCHMARK_PATH.encode("ascii"):
        raise RuntimeError("benchmark file-content request contract mismatch")
    return token


def _response(msg_id: int, msg_type: int, payload: bytes) -> bytes:
    return b"\xb1\xcb\x74\x00\x00" + msg_id.to_bytes(4, "little") + b"\x00" + msg_type.to_bytes(2, "little") + len(payload).to_bytes(2, "little") * 2 + payload


def _handshake_payload() -> bytes:
    payload = bytearray(189)
    payload[1:3] = (2026).to_bytes(2, "little")
    payload[3:9] = bytes((27, 5, 30, 10, 0, 0))
    payload[42:46] = (20260527).to_bytes(4, "little")
    payload[50:54] = (20260527).to_bytes(4, "little")
    return bytes(payload)


_EXECUTOR_START_TIMEOUT = 30.0
_COHORT_TIMEOUT = 30.0


def _percentile_ns(values: list[int], numerator: int, denominator: int) -> int:
    if not values:
        raise ValueError("latency sample is empty")
    ordered = sorted(values)
    index = ((len(ordered) - 1) * numerator) // denominator
    return ordered[index]


def _summary_fields(latencies_ns: list[int], elapsed_ns: int, requests: int) -> dict[str, Any]:
    median_ns = statistics.median(latencies_ns)
    p99_ns = _percentile_ns(latencies_ns, 99, 100)
    return {
        "elapsed_ns": elapsed_ns,
        "seconds": round(elapsed_ns / 1_000_000_000, 6),
        "throughput_rps": round(requests * 1_000_000_000 / elapsed_ns, 3),
        "latency_p50_ns": median_ns,
        "latency_p99_ns": p99_ns,
        "latency_p50_ms": round(float(median_ns) / 1_000_000, 4),
        "latency_p99_ms": round(p99_ns / 1_000_000, 4),
    }


def _prestart_executor(executor: ThreadPoolExecutor, workers: int) -> tuple[int, ...]:
    ready = threading.Barrier(workers + 1, timeout=_EXECUTOR_START_TIMEOUT)

    def prestart() -> int:
        thread_id = threading.get_ident()
        ready.wait()
        return thread_id

    futures: list[Future[int]] = []
    try:
        for _ in range(workers):
            futures.append(executor.submit(prestart))
        ready.wait()
        thread_ids = tuple(future.result(timeout=_EXECUTOR_START_TIMEOUT) for future in futures)
    except BaseException:
        ready.abort()
        for future in futures:
            future.cancel()
        wait(futures, timeout=_EXECUTOR_START_TIMEOUT, return_when=ALL_COMPLETED)
        raise
    if len(set(thread_ids)) != workers:
        raise RuntimeError(f"executor started {len(set(thread_ids))} of {workers} workers")
    return thread_ids


def _execute_timed(transport: Any, token: int) -> dict[str, int]:
    started_ns = time.perf_counter_ns()
    value = transport.execute(
        TYPE_FILE_CONTENT,
        {"path": _BENCHMARK_PATH, "offset": token, "size": _BENCHMARK_VALUE.size},
    )
    elapsed_ns = time.perf_counter_ns() - started_ns
    if not isinstance(value, FileContentChunk):
        raise AssertionError(f"benchmark response is not FileContentChunk: {value!r}")
    if (
        value.path != _BENCHMARK_PATH
        or value.offset != token
        or value.request_size != _BENCHMARK_VALUE.size
        or value.chunk_len != _BENCHMARK_VALUE.size
        or len(value.content) != _BENCHMARK_VALUE.size
    ):
        raise AssertionError(f"benchmark response snapshot mismatch: {value!r}")
    echoed_token, measurement_epoch, connection_id, attempt_sequence = _BENCHMARK_VALUE.unpack(value.content)
    return {
        "requested_token": token,
        "snapshot_token": value.offset,
        "echoed_token": echoed_token,
        "measurement_epoch": measurement_epoch,
        "connection_id": connection_id,
        "attempt_sequence": attempt_sequence,
        "call_latency_ns": elapsed_ns,
    }


def _completion_summary(records: Sequence[Mapping[str, int]], server: BenchmarkServer) -> dict[str, Any]:
    requested = [record["requested_token"] for record in records]
    echoed = [record["echoed_token"] for record in records]
    expected_tokens = set(requested)
    returned_tokens = set(echoed)
    connections, attempts, expected_provenance = server.provenance_snapshot()
    cross_request = sum(
        record["snapshot_token"] != record["requested_token"]
        or record["echoed_token"] != record["requested_token"]
        for record in records
    )
    digest = hashlib.sha256()
    completion_records: list[list[int]] = []
    for record in records:
        expected = expected_provenance.get(record["requested_token"], (0, 0, 0))
        completion_record = [
            record["requested_token"],
            record["snapshot_token"],
            record["echoed_token"],
            record["measurement_epoch"],
            record["connection_id"],
            record["attempt_sequence"],
            *expected,
        ]
        completion_records.append(completion_record)
        digest.update(_COMPLETION_RECORD.pack(*completion_record))
    cross_generation = sum(record[3:6] != record[6:9] for record in completion_records)
    return {
        "request_token_start": min(requested) if requested else None,
        "request_token_end": max(requested) if requested else None,
        "request_token_sum": sum(requested),
        "server_connections": connections,
        "server_attempts": attempts,
        "unique_responses": len(returned_tokens),
        "duplicate_responses": len(echoed) - len(returned_tokens),
        "missing_responses": len(expected_tokens - returned_tokens),
        "unexpected_responses": len(returned_tokens - expected_tokens),
        "cross_request_completions": cross_request,
        "cross_generation_completions": cross_generation,
        "completion_sha256": digest.hexdigest(),
        "completion_records": completion_records,
    }


def _close_after_failure(
    server: BenchmarkServer,
    transport: Any,
    executor: ThreadPoolExecutor,
    futures: Sequence[Future[Any]],
) -> list[str]:
    cleanup_errors: list[str] = []
    server.abort()
    try:
        transport.close()
    except BaseException as exc:
        cleanup_errors.append(f"transport close failed: {type(exc).__name__}: {exc}")
    for future in futures:
        future.cancel()
    _, pending = wait(futures, timeout=_COHORT_TIMEOUT, return_when=ALL_COMPLETED)
    if pending:
        cleanup_errors.append(f"{len(pending)} executor futures remained pending after cleanup")
    executor.shutdown(wait=not pending, cancel_futures=True)
    return cleanup_errors


def _submit_requests(
    executor: ThreadPoolExecutor,
    transport: Any,
    tokens: Sequence[int],
    futures: list[Future[dict[str, int]]],
) -> None:
    for token in tokens:
        futures.append(executor.submit(_execute_timed, transport, token))


def run_case(
    pool_size: int,
    concurrency: int,
    requests: int,
    delay: float,
    *,
    warmup_requests: int = 0,
    include_latencies: bool = False,
) -> dict[str, Any]:
    with BenchmarkServer(delay) as server:
        transport = PooledSocketTransport(
            hosts=[server.host],
            timeout=10,
            pool_size=pool_size,
            heartbeat_interval=None,
        )
        executor = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="eltdx-benchmark")
        active_futures: list[Future[dict[str, int]]] = []
        try:
            transport.connect()
            for index in range(pool_size):
                _execute_timed(transport, _SETUP_TOKEN_BASE + index)
            worker_ids = _prestart_executor(executor, concurrency)
            if warmup_requests:
                _submit_requests(
                    executor,
                    transport,
                    range(_WARMUP_TOKEN_BASE, _WARMUP_TOKEN_BASE + warmup_requests),
                    active_futures,
                )
                for future in active_futures:
                    future.result()
                active_futures.clear()
            server.reset_measurement()
            started_ns = time.perf_counter_ns()
            _submit_requests(executor, transport, range(requests), active_futures)
            records = [future.result() for future in active_futures]
            elapsed_ns = time.perf_counter_ns() - started_ns
            latencies_ns = [record["call_latency_ns"] for record in records]
            active_futures.clear()
            server.wait_for_idle()
        except BaseException as exc:
            cleanup_errors = _close_after_failure(server, transport, executor, active_futures)
            if cleanup_errors:
                raise RuntimeError(f"{type(exc).__name__}: {exc}; {'; '.join(cleanup_errors)}") from exc
            raise
        executor.shutdown(wait=True)
        transport.close()

        result = {
            "mode": "saturated",
            "pool_size": pool_size,
            "concurrency": concurrency,
            "requests": requests,
            "warmup_requests": warmup_requests,
            "successes": len(latencies_ns),
            "errors": [],
            "worker_threads": len(worker_ids),
            "server_max_active": server.max_active,
            "server_requests": server.requests,
            **_completion_summary(records, server),
            **_summary_fields(latencies_ns, elapsed_ns, requests),
        }
        if include_latencies:
            result["latency_metric"] = "call_latency_ns"
            result["latency_ns"] = latencies_ns
        return result


def _run_cohort_wave(
    executor: ThreadPoolExecutor,
    transport: Any,
    tokens: Sequence[int],
    active_futures: list[Future[dict[str, int]]],
) -> list[dict[str, int]]:
    width = len(tokens)
    if width <= 0:
        raise ValueError("cohort token list is empty")
    epoch_ns = 0

    def release() -> None:
        nonlocal epoch_ns
        epoch_ns = time.perf_counter_ns()

    barrier = threading.Barrier(width + 1, action=release, timeout=_COHORT_TIMEOUT)

    def execute(worker_id: int, token: int) -> dict[str, int]:
        barrier.wait()
        record = _execute_timed(transport, token)
        record["worker_id"] = worker_id
        record["cohort_latency_ns"] = time.perf_counter_ns() - epoch_ns
        return record

    active_futures.clear()
    try:
        for worker_id, token in enumerate(tokens):
            active_futures.append(executor.submit(execute, worker_id, token))
        barrier.wait()
        _, pending = wait(active_futures, timeout=_COHORT_TIMEOUT, return_when=ALL_COMPLETED)
        if pending:
            raise TimeoutError(f"cohort did not settle within {_COHORT_TIMEOUT:.0f}s")
    except BaseException:
        barrier.abort()
        for future in active_futures:
            future.cancel()
        raise

    records: list[dict[str, int]] = []
    errors: list[BaseException] = []
    for future in active_futures:
        try:
            records.append(future.result())
        except BaseException as exc:
            errors.append(exc)
    if errors:
        raise errors[0]
    active_futures.clear()
    return records


def _broker_boundary(transport: Any, pool_size: int) -> dict[str, Any] | None:
    broker = getattr(transport, "_broker", None)
    if broker is None:
        return None
    snapshot = broker.snapshot()
    clean = (
        snapshot.idle_slots == pool_size
        and snapshot.waiter_count == 0
        and snapshot.pin_waiter_count == 0
        and snapshot.active_leases == 0
        and not snapshot.closed
    )
    return {
        "idle_slots": snapshot.idle_slots,
        "waiters": snapshot.waiter_count,
        "pin_waiters": snapshot.pin_waiter_count,
        "active_leases": snapshot.active_leases,
        "closed": snapshot.closed,
        "clean": clean,
    }


def run_fixed_cohort_case(
    pool_size: int,
    cohort_size: int,
    measured_cohorts: int,
    delay: float,
    *,
    warmup_cohorts: int,
    latency_metric: str,
) -> dict[str, Any]:
    if latency_metric not in {"call_latency_ns", "cohort_latency_ns"}:
        raise ValueError(f"unsupported cohort latency metric: {latency_metric}")
    with BenchmarkServer(delay) as server:
        transport = PooledSocketTransport(
            hosts=[server.host],
            timeout=10,
            pool_size=pool_size,
            heartbeat_interval=None,
        )
        executor = ThreadPoolExecutor(max_workers=cohort_size, thread_name_prefix="eltdx-cohort")
        call_latencies_ns: list[int] = []
        cohort_latencies_ns: list[int] = []
        wave_makespans_ns: list[int] = []
        boundary_checks = 0
        boundary_supported = False
        active_futures: list[Future[dict[str, int]]] = []
        measured_records: list[dict[str, int]] = []
        try:
            transport.connect()
            for index in range(pool_size):
                _execute_timed(transport, _SETUP_TOKEN_BASE + index)
            worker_ids = _prestart_executor(executor, cohort_size)
            measured_started_ns = 0
            total_cohorts = warmup_cohorts + measured_cohorts
            for cohort_index in range(total_cohorts):
                if cohort_index == warmup_cohorts:
                    server.reset_measurement()
                    measured_started_ns = time.perf_counter_ns()
                if cohort_index < warmup_cohorts:
                    token_start = _WARMUP_TOKEN_BASE + cohort_index * cohort_size
                else:
                    token_start = (cohort_index - warmup_cohorts) * cohort_size
                records = _run_cohort_wave(
                    executor,
                    transport,
                    range(token_start, token_start + cohort_size),
                    active_futures,
                )
                server.wait_for_idle()
                boundary = _broker_boundary(transport, pool_size)
                if boundary is not None:
                    boundary_supported = True
                    boundary_checks += 1
                    if not boundary["clean"]:
                        raise AssertionError(f"cohort boundary retained Broker state: {boundary!r}")
                if cohort_index >= warmup_cohorts:
                    measured_records.extend(records)
                    call_latencies_ns.extend(record["call_latency_ns"] for record in records)
                    cohort_latencies_ns.extend(record["cohort_latency_ns"] for record in records)
                    wave_makespans_ns.append(max(record["cohort_latency_ns"] for record in records))
            wall_elapsed_ns = time.perf_counter_ns() - measured_started_ns
        except BaseException as exc:
            cleanup_errors = _close_after_failure(server, transport, executor, active_futures)
            if cleanup_errors:
                raise RuntimeError(f"{type(exc).__name__}: {exc}; {'; '.join(cleanup_errors)}") from exc
            raise
        executor.shutdown(wait=True)
        transport.close()

        requests = cohort_size * measured_cohorts
        elapsed_ns = sum(wave_makespans_ns)
        selected_latencies = (
            call_latencies_ns if latency_metric == "call_latency_ns" else cohort_latencies_ns
        )
        return {
            "mode": "fixed_cohort",
            "pool_size": pool_size,
            "cohort_size": cohort_size,
            "measured_cohorts": measured_cohorts,
            "warmup_cohorts": warmup_cohorts,
            "requests": requests,
            "successes": len(selected_latencies),
            "errors": [],
            "worker_threads": len(worker_ids),
            "server_max_active": server.max_active,
            "server_requests": server.requests,
            **_completion_summary(measured_records, server),
            "boundary_checks_supported": boundary_supported,
            "boundary_checks": boundary_checks,
            "boundary_checks_all_clean": True,
            "latency_metric": latency_metric,
            "latency_ns": selected_latencies,
            "call_latency_ns": call_latencies_ns,
            "cohort_latency_ns": cohort_latencies_ns,
            "wave_makespan_ns": wave_makespans_ns,
            "wall_elapsed_ns": wall_elapsed_ns,
            **_summary_fields(selected_latencies, elapsed_ns, requests),
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


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def run_fifo_trial(
    *,
    label: str,
    campaign_id: str,
    trial_index: int,
    role: str,
    declaration_sha256: str,
) -> dict[str, Any]:
    if role not in {"baseline", "current"}:
        raise ValueError(f"unsupported trial role: {role}")
    config = fifo_v2_config()
    delay = config["delay_ns"] / 1_000_000_000
    started_at = _utc_now()
    cases = {
        "sequential": run_case(
            1,
            1,
            10_000,
            delay,
            warmup_requests=1_000,
            include_latencies=True,
        ),
        "saturated": run_case(
            4,
            100,
            100_000,
            delay,
            warmup_requests=1_000,
            include_latencies=True,
        ),
        "no_backlog": run_fixed_cohort_case(
            4,
            4,
            2_500,
            delay,
            warmup_cohorts=100,
            latency_metric="call_latency_ns",
        ),
        "contended_wave": run_fixed_cohort_case(
            4,
            100,
            50,
            delay,
            warmup_cohorts=10,
            latency_metric="cohort_latency_ns",
        ),
    }
    source = Path(__file__).read_bytes()
    return {
        "schema": 4,
        "kind": "actor-performance-trial",
        "protocol": FIFO_PROTOCOL,
        "label": label,
        "campaign_id": campaign_id,
        "trial_id": f"{campaign_id}/{trial_index:03d}",
        "trial_index": trial_index,
        "attempt": 1,
        "role": role,
        "declaration_sha256": declaration_sha256,
        "workload_sha256": hashlib.sha256(source).hexdigest(),
        "source_root": str(SOURCE_ROOT),
        "system": platform.system(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "started_at_utc": started_at,
        "ended_at_utc": _utc_now(),
        "config": config,
        "config_sha256": _canonical_sha256(config),
        **_git_identity(),
        "cases": cases,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--delay-ms", type=float, default=1.0)
    parser.add_argument("--acceptance", action="store_true")
    parser.add_argument("--fifo-trial", action="store_true")
    parser.add_argument("--campaign-id")
    parser.add_argument("--trial-index", type=int)
    parser.add_argument("--role", choices=("baseline", "current"))
    parser.add_argument("--declaration-sha256")
    parser.add_argument("--sequential-requests", type=int, default=10_000)
    parser.add_argument("--concurrent-requests", type=int, default=100_000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.fifo_trial:
        required = {
            "campaign-id": args.campaign_id,
            "trial-index": args.trial_index,
            "role": args.role,
            "declaration-sha256": args.declaration_sha256,
            "output": args.output,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            parser.error(f"--fifo-trial requires: {', '.join(missing)}")
        result = run_fifo_trial(
            label=args.label,
            campaign_id=args.campaign_id,
            trial_index=args.trial_index,
            role=args.role,
            declaration_sha256=args.declaration_sha256,
        )
        encoded = json.dumps(result, ensure_ascii=True, separators=(",", ":")) + "\n"
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
        print(encoded, end="")
        return

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
