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
import struct
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from eltdx.exceptions import PushOverflowError, TransportError  # noqa: E402
from eltdx.models import FileContentChunk  # noqa: E402
from eltdx.protocol.constants import (  # noqa: E402
    TYPE_FILE_CONTENT,
    TYPE_HANDSHAKE,
    TYPE_HEARTBEAT,
    TYPE_REFRESH_STREAM,
    TYPE_SECURITY_COUNT,
)
from eltdx.transport import PooledSocketTransport, SocketTransport  # noqa: E402


_STRESS_VALUE = struct.Struct("<IHIQ")
_STRESS_PATH = "actor-stress.bin"


class StressLedger:
    """Wire-side provenance shared by both real loopback servers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._attempt_sequence = 0
        self._attempts: dict[int, list[tuple[int, int, int]]] = {}
        self._active_business = 0
        self._max_active_business = 0

    def enter_business(self) -> None:
        with self._lock:
            self._active_business += 1
            self._max_active_business = max(self._max_active_business, self._active_business)

    def leave_business(self) -> None:
        with self._lock:
            self._active_business -= 1

    def record_attempt(self, token: int, server_id: int, connection_id: int) -> int:
        with self._lock:
            self._attempt_sequence += 1
            sequence = self._attempt_sequence
            self._attempts.setdefault(token, []).append((sequence, server_id, connection_id))
            return sequence

    def expected_identity(self, token: int) -> tuple[int, int] | None:
        with self._lock:
            attempts = self._attempts.get(token)
            if not attempts:
                return None
            _, server_id, connection_id = attempts[-1]
            return server_id, connection_id

    def expected_provenance(self, token: int) -> tuple[int, int, int] | None:
        with self._lock:
            attempts = self._attempts.get(token)
            if not attempts:
                return None
            sequence, server_id, connection_id = attempts[-1]
            return server_id, connection_id, sequence

    def attempt_count(self, token: int) -> int:
        with self._lock:
            return len(self._attempts.get(token, ()))

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            retried = [items for items in self._attempts.values() if len(items) > 1]
            cross_endpoint = sum(len({item[1] for item in items}) > 1 for items in retried)
            return {
                "attempts": self._attempt_sequence,
                "logical_requests": len(self._attempts),
                "retried_requests": len(retried),
                "cross_endpoint_retried_requests": cross_endpoint,
                "same_endpoint_retried_requests": len(retried) - cross_endpoint,
                "max_business_active": self._max_active_business,
            }


class StressServer:
    def __init__(
        self,
        *,
        server_id: int = 0,
        ledger: StressLedger | None = None,
        push_every: int = 0,
        poison_every: int = 0,
        close_every: int = 0,
        keep_open_token: int | None = None,
        fail_before_response_every: int = 0,
        response_delay: float = 0.0,
    ) -> None:
        self.server_id = server_id
        self.ledger = ledger
        self.push_every = push_every
        self.poison_every = poison_every
        self.close_every = close_every
        self.keep_open_token = keep_open_token
        self.fail_before_response_every = fail_before_response_every
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
        self.heartbeat_responses = 0
        self.heartbeat_during_business = 0
        self.heartbeat_by_connection: dict[int, int] = {}
        self.heartbeat_responses_by_connection: dict[int, int] = {}
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

    def wait_for_heartbeat(self, count: int, timeout: float = 2.0) -> bool:
        with self._condition:
            return self._condition.wait_for(lambda: self.heartbeat_requests >= count, timeout=timeout)

    def wait_for_heartbeat_connections(self, count: int, timeout: float = 2.0) -> bool:
        with self._condition:
            return self._condition.wait_for(lambda: len(self.heartbeat_by_connection) >= count, timeout=timeout)

    def wait_for_heartbeat_response_connections(self, count: int, timeout: float = 2.0) -> bool:
        with self._condition:
            return self._condition.wait_for(
                lambda: len(self.heartbeat_responses_by_connection) >= count,
                timeout=timeout,
            )

    def heartbeat_response_snapshot(self) -> dict[int, int]:
        with self._condition:
            return dict(self.heartbeat_responses_by_connection)

    def wait_for_heartbeat_response_round(self, baseline: dict[int, int], timeout: float = 2.0) -> bool:
        with self._condition:
            return self._condition.wait_for(
                lambda: baseline and all(
                    self.heartbeat_responses_by_connection.get(connection_id, 0) > count
                    for connection_id, count in baseline.items()
                ),
                timeout=timeout,
            )

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
            worker = threading.Thread(
                target=self._serve,
                args=(conn, index),
                name=f"eltdx-stress-{self.server_id}-conn-{index}",
                daemon=True,
            )
            self._workers.append(worker)
            worker.start()

    def _serve(self, conn: socket.socket, connection_id: int) -> None:
        try:
            with conn:
                while not self._stop.is_set():
                    msg_id, msg_type, request = _read_request(conn)
                    token: int | None = None
                    if msg_type == TYPE_HANDSHAKE:
                        conn.sendall(_response(msg_id, msg_type, _handshake_payload()))
                        continue
                    if msg_type == TYPE_HEARTBEAT:
                        with self._condition:
                            self.heartbeat_requests += 1
                            self.heartbeat_by_connection[connection_id] = (
                                self.heartbeat_by_connection.get(connection_id, 0) + 1
                            )
                            if self.active_business:
                                self.heartbeat_during_business += 1
                            self._condition.notify_all()
                        if self.response_delay:
                            time.sleep(self.response_delay)
                        conn.sendall(_response(msg_id, msg_type, bytes.fromhex("0000000000008f173501")))
                        with self._condition:
                            self.heartbeat_responses += 1
                            self.heartbeat_responses_by_connection[connection_id] = (
                                self.heartbeat_responses_by_connection.get(connection_id, 0) + 1
                            )
                            self._condition.notify_all()
                        continue
                    if msg_type not in (TYPE_FILE_CONTENT, TYPE_SECURITY_COUNT):
                        raise RuntimeError(f"unexpected stress command: {msg_type:#x}")
                    with self._condition:
                        self.business_requests += 1
                        sequence = self.business_requests
                        self.active_business += 1
                        self.max_business_active = max(self.max_business_active, self.active_business)
                        self._condition.notify_all()
                    if self.ledger is not None:
                        self.ledger.enter_business()
                    try:
                        if self.response_delay:
                            time.sleep(self.response_delay)
                        if msg_type == TYPE_FILE_CONTENT:
                            if len(request) < 8:
                                raise RuntimeError("truncated stress file request")
                            token = int.from_bytes(request[:4], "little")
                            requested_size = int.from_bytes(request[4:8], "little")
                            if requested_size < _STRESS_VALUE.size:
                                raise RuntimeError("stress file request is too small")
                            attempt_sequence = (
                                self.ledger.record_attempt(token, self.server_id, connection_id)
                                if self.ledger is not None
                                else sequence
                            )
                            content = _STRESS_VALUE.pack(token, self.server_id, connection_id, attempt_sequence)
                            payload = len(content).to_bytes(4, "little") + content
                            if self.fail_before_response_every and sequence % self.fail_before_response_every == 0:
                                wire = _response(msg_id, msg_type, payload)
                                conn.sendall(wire[: max(1, len(wire) // 2)])
                                return
                        else:
                            payload = (23285).to_bytes(2, "little")
                        if self.push_every and sequence % self.push_every == 0:
                            conn.sendall(_response(0xF0000000 | (sequence & 0x0FFFFFFF), TYPE_REFRESH_STREAM, b"\x93\x93"))
                            with self._condition:
                                self.push_frames += 1
                        wire = _response(msg_id, msg_type, payload)
                        if msg_type == TYPE_FILE_CONTENT and self.poison_every and sequence % self.poison_every == 0:
                            wire += _response((msg_id + 1) & 0xFFFFFFFF, msg_type, payload)
                            with self._condition:
                                self.push_frames += 1
                        conn.sendall(wire)
                    finally:
                        with self._condition:
                            self.active_business -= 1
                        if self.ledger is not None:
                            self.ledger.leave_business()
                    if (
                        self.close_every
                        and sequence % self.close_every == 0
                        and token != self.keep_open_token
                    ):
                        return
        except (EOFError, OSError, TimeoutError):
            return
        except BaseException as exc:
            self.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            with self._condition:
                self._connections.discard(conn)


def _execute_unique(transport: Any, token: int) -> dict[str, int]:
    chunk = transport.execute(
        TYPE_FILE_CONTENT,
        {"path": _STRESS_PATH, "offset": token, "size": _STRESS_VALUE.size},
    )
    if not isinstance(chunk, FileContentChunk) or len(chunk.content) != _STRESS_VALUE.size:
        raise AssertionError("stress response is not an exact FileContentChunk")
    echoed_token, server_id, connection_id, attempt_sequence = _STRESS_VALUE.unpack(chunk.content)
    return {
        "requested_token": token,
        "snapshot_token": chunk.offset,
        "echoed_token": echoed_token,
        "server_id": server_id,
        "connection_id": connection_id,
        "attempt_sequence": attempt_sequence,
    }


def _unique_completion_summary(values: list[dict[str, int]], ledger: StressLedger) -> dict[str, int]:
    echoed = [item["echoed_token"] for item in values]
    response_identities = {
        (item["echoed_token"], item["server_id"], item["connection_id"], item["attempt_sequence"])
        for item in values
    }
    expected_tokens = {item["requested_token"] for item in values}
    returned_tokens = set(echoed)
    cross_request = sum(
        item["echoed_token"] != item["requested_token"] or item["snapshot_token"] != item["requested_token"]
        for item in values
    )
    cross_generation = 0
    for item in values:
        expected = ledger.expected_provenance(item["requested_token"])
        if expected != (item["server_id"], item["connection_id"], item["attempt_sequence"]):
            cross_generation += 1
    return {
        "unique_responses": len(response_identities),
        "duplicate_responses": len(values) - len(response_identities),
        "missing_responses": len(expected_tokens - returned_tokens),
        "unexpected_responses": len(returned_tokens - expected_tokens),
        "cross_request_completions": cross_request,
        "cross_generation_completions": cross_generation,
    }


def _capture_runtime_resources(runtime: Any) -> tuple[Any, Any, Any, Any, Any]:
    with runtime.control_lock:
        generation = runtime.generation
        return (
            runtime.selector,
            runtime.wake_reader,
            runtime.wake_writer,
            generation,
            generation.sock if generation is not None else None,
        )


def _socket_closed(sock: Any) -> bool:
    return sock is not None and sock.fileno() == -1


def _selector_closed(selector: Any) -> bool:
    if selector is None:
        return True
    try:
        return selector.get_map() is None
    except (OSError, RuntimeError, ValueError):
        return True


def _runtime_cleanup_snapshot(runtime: Any, owned: tuple[Any, Any, Any, Any, Any]) -> dict[str, Any]:
    selector, wake_reader, wake_writer, generation, tcp_socket = owned
    with runtime.control_lock:
        thread = runtime.actor_thread
        snapshot = {
            "state": runtime.state.name,
            "actor_alive": thread is not None and thread.is_alive(),
            "generation_present": runtime.generation is not None,
            "selector_present": runtime.selector is not None,
            "wake_reader_present": runtime.wake_reader is not None,
            "wake_writer_present": runtime.wake_writer is not None,
            "pending_ticket_present": runtime.pending_task is not None,
            "active_ticket_present": runtime.active_task is not None,
            "cancel_count": len(runtime.cancel_requests),
        }
    snapshot.update(
        saved_selector_present=selector is not None,
        saved_wake_reader_present=wake_reader is not None,
        saved_wake_writer_present=wake_writer is not None,
        saved_generation_present=generation is not None,
        saved_tcp_present=tcp_socket is not None,
        saved_selector_closed=_selector_closed(selector),
        saved_wake_reader_closed=_socket_closed(wake_reader),
        saved_wake_writer_closed=_socket_closed(wake_writer),
        saved_tcp_closed=_socket_closed(tcp_socket),
    )
    snapshot["all_owned_resources_closed"] = (
        not snapshot["actor_alive"]
        and not snapshot["generation_present"]
        and not snapshot["selector_present"]
        and not snapshot["wake_reader_present"]
        and not snapshot["wake_writer_present"]
        and not snapshot["pending_ticket_present"]
        and not snapshot["active_ticket_present"]
        and snapshot["cancel_count"] == 0
        and snapshot["saved_selector_present"]
        and snapshot["saved_wake_reader_present"]
        and snapshot["saved_wake_writer_present"]
        and snapshot["saved_generation_present"]
        and snapshot["saved_tcp_present"]
        and snapshot["saved_selector_closed"]
        and snapshot["saved_wake_reader_closed"]
        and snapshot["saved_wake_writer_closed"]
        and snapshot["saved_tcp_closed"]
    )
    return snapshot


def run_generation_stress(count: int) -> dict[str, Any]:
    before_threads = _actor_threads()
    ledger = StressLedger()
    with (
        StressServer(
            server_id=1,
            ledger=ledger,
            close_every=1,
            keep_open_token=count - 1,
            poison_every=31,
        ) as first,
        StressServer(
            server_id=2,
            ledger=ledger,
            close_every=1,
            keep_open_token=count - 1,
            poison_every=29,
        ) as second,
    ):
        transport = SocketTransport(hosts=[first.host, second.host], timeout=5, heartbeat_interval=None)
        runtime_ref = None
        thread_ref = None
        closed = False
        try:
            started = time.perf_counter()
            values = []
            for token in range(count):
                values.append(_execute_unique(transport, token))
                runtime = transport._runtime
                if runtime is None or runtime.actor_thread is None:
                    raise AssertionError("Actor runtime disappeared")
                if runtime_ref is None:
                    runtime_ref = runtime
                    thread_ref = runtime.actor_thread
                elif runtime is not runtime_ref or runtime.actor_thread is not thread_ref:
                    raise AssertionError("Actor object identity changed across generations")
            elapsed = time.perf_counter() - started
            transport.connect()
            diagnostics = transport.diagnostics
            generation_counter = diagnostics.actor.tcp_generation if diagnostics.actor is not None else 0
            stale_events = diagnostics.actor.stale_event_count if diagnostics.actor is not None else -1
            if runtime_ref is None or thread_ref is None:
                raise AssertionError("generation stress did not create an Actor")
            owned = _capture_runtime_resources(runtime_ref)
            transport.close()
            closed = True
            cleanup = _runtime_cleanup_snapshot(runtime_ref, owned)
            result = {
                "requests": count,
                "seconds": round(elapsed, 6),
                "throughput_rps": round(count / elapsed, 3),
                "runtime_identity": id(runtime_ref),
                "actor_object_identity": id(thread_ref),
                "actor_thread_ident": thread_ref.ident,
                "generation_counter": generation_counter,
                "server_accepts": [first.accept_count, second.accept_count],
                "server_requests": [first.business_requests, second.business_requests],
                "servers_used": sum(item > 0 for item in (first.business_requests, second.business_requests)),
                "stale_events": stale_events,
                "ledger": ledger.snapshot(),
                "cleanup": cleanup,
                **_unique_completion_summary(values, ledger),
            }
        finally:
            if not closed:
                transport.close()
    del first, second
    result.update(
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
    ledger = StressLedger()
    first_fail_every = max(2, close_every // 2) if close_every else 0
    with (
        StressServer(
            server_id=1,
            ledger=ledger,
            push_every=push_every,
            poison_every=push_every,
            close_every=close_every,
            fail_before_response_every=first_fail_every,
            response_delay=response_delay,
        ) as first,
        StressServer(
            server_id=2,
            ledger=ledger,
            push_every=push_every,
            poison_every=push_every + 2 if push_every else 0,
            close_every=close_every,
            response_delay=response_delay,
        ) as second,
    ):
        pool = PooledSocketTransport(
            hosts=[first.host, second.host],
            timeout=10,
            pool_size=pool_size,
            heartbeat_interval=None,
            max_pending_requests=max(concurrency * 2, 256),
            push_queue_size=1024,
            push_queue_bytes=8 * 1024 * 1024,
        )

        def execute(token: int) -> dict[str, int]:
            return _execute_unique(pool, token)

        closed = False
        try:
            started = time.perf_counter()
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                values = list(executor.map(execute, range(requests)))
            elapsed = time.perf_counter() - started
            del executor
            gap_reported = False
            try:
                pool.drain_pushes()
            except PushOverflowError:
                gap_reported = True
                pool.drain_pushes()
            pool.connect()
            diagnostics = pool.diagnostics
            actor_generations = [actor.tcp_generation for actor in diagnostics.actors]
            stale_events = sum(actor.stale_event_count for actor in diagnostics.actors)
            broker = pool._broker
            push_buffer = pool._push_buffer
            runtimes = [transport._runtime for transport in pool._transports]
            if broker is None or push_buffer is None or any(runtime is None for runtime in runtimes):
                raise AssertionError("mixed stress pool ownership disappeared")
            owned = [_capture_runtime_resources(runtime) for runtime in runtimes]
            pool.close()
            closed = True
            broker_after = broker.snapshot()
            push_after = push_buffer.snapshot()
            cleanup = [
                _runtime_cleanup_snapshot(runtime, resources)
                for runtime, resources in zip(runtimes, owned)
            ]
            result = {
                "requests": requests,
                "pool_size": pool_size,
                "concurrency": concurrency,
                "seconds": round(elapsed, 6),
                "throughput_rps": round(requests / elapsed, 3),
                "server_requests": [first.business_requests, second.business_requests],
                "server_accepts": [first.accept_count, second.accept_count],
                "servers_used": sum(item > 0 for item in (first.business_requests, second.business_requests)),
                "server_max_active": ledger.snapshot()["max_business_active"],
                "push_frames_sent": first.push_frames + second.push_frames,
                "push_dropped": diagnostics.push_dropped,
                "push_gap_reported": gap_reported,
                "actor_generations": actor_generations,
                "stale_events": stale_events,
                "ledger": ledger.snapshot(),
                "broker_after_close": {
                    "idle_slots": broker_after.idle_slots,
                    "waiters": broker_after.waiter_count,
                    "pin_waiters": broker_after.pin_waiter_count,
                    "leases": broker_after.active_leases,
                    "closed": broker_after.closed,
                },
                "push_after_close": {
                    "frames": push_after.frame_count,
                    "bytes": push_after.byte_count,
                    "configured_max_frames": push_buffer.max_frames,
                    "configured_max_bytes": push_buffer.max_bytes,
                    "max_frames_observed": push_after.max_frames_observed,
                    "max_bytes_observed": push_after.max_bytes_observed,
                    "dropped_total": push_after.dropped_total,
                    "gap_pending": push_after.gap_pending,
                    "closed": push_after.closed,
                },
                "cleanup": cleanup,
                **_unique_completion_summary(values, ledger),
            }
        finally:
            if not closed:
                pool.close()
    del first, second
    result["actor_threads_after"] = len(_actor_threads())
    return result


def run_warmed_resource_stress(
    *,
    warmup_rounds: int = 3,
    measured_rounds: int = 6,
    generations_per_round: int = 20,
) -> dict[str, Any]:
    if warmup_rounds < 1 or measured_rounds < 3 or generations_per_round < 1:
        raise ValueError("resource stress requires warmup >= 1, measured >= 3, and generations >= 1")
    rounds = []
    for index in range(warmup_rounds + measured_rounds):
        result = run_generation_stress(generations_per_round)
        rounds.append(
            {
                "round": index,
                "phase": "warmup" if index < warmup_rounds else "measured",
                "resources": _resource_after_gc(),
                "actor_threads": result["actor_threads_after"],
                "cross_request_completions": result["cross_request_completions"],
                "cross_generation_completions": result["cross_generation_completions"],
                "owned_resources_closed": result["cleanup"]["all_owned_resources_closed"],
            }
        )
    measured = rounds[warmup_rounds:]
    counts = [item["resources"] for item in measured]
    supported = all(value is not None for value in counts)
    plateau = supported and len(set(counts)) == 1
    monotonic_growth = supported and all(
        later >= earlier for earlier, later in zip(counts, counts[1:])
    ) and counts[-1] > counts[0]
    return {
        "warmup_rounds": warmup_rounds,
        "measured_rounds": measured_rounds,
        "generations_per_round": generations_per_round,
        "samples": rounds,
        "measured_resources": counts,
        "resource_counter_supported": supported,
        "exact_plateau": plateau,
        "monotonic_growth": monotonic_growth,
        "all_actor_threads_closed": all(item["actor_threads"] == 0 for item in measured),
        "all_owned_resources_closed": all(item["owned_resources_closed"] for item in measured),
        "cross_request_completions": sum(item["cross_request_completions"] for item in measured),
        "cross_generation_completions": sum(item["cross_generation_completions"] for item in measured),
    }


def run_close_samples(samples: int, *, pool_size: int = 4, loaded: bool = False) -> dict[str, Any]:
    latencies: list[float] = []
    sample_evidence: list[dict[str, Any]] = []
    completed_results = 0
    expected_errors: dict[str, int] = {}
    with StressServer(response_delay=0.05 if loaded else 0.0) as server:
        handled = 0
        for _ in range(samples):
            pool = PooledSocketTransport(hosts=[server.host], timeout=2, pool_size=pool_size, heartbeat_interval=None)
            pool.connect()
            runtimes = [transport._runtime for transport in pool._transports]
            if any(runtime is None for runtime in runtimes):
                raise AssertionError("close sample pool lost an Actor runtime")
            owned = [_capture_runtime_resources(runtime) for runtime in runtimes]
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
            cleanup = [
                _runtime_cleanup_snapshot(runtime, resources)
                for runtime, resources in zip(runtimes, owned)
            ]
            if not all(item["all_owned_resources_closed"] for item in cleanup):
                raise AssertionError("close sample retained Actor-owned resources")
            settle_started = time.perf_counter()
            if executor is not None:
                for future in futures:
                    try:
                        future.result(timeout=2)
                    except FutureTimeoutError as exc:
                        raise AssertionError("caller future did not settle after loaded close") from exc
                    except TransportError as exc:
                        name = type(exc).__name__
                        expected_errors[name] = expected_errors.get(name, 0) + 1
                    else:
                        completed_results += 1
                executor.shutdown()
            settle_ms = (time.perf_counter() - settle_started) * 1000
            sample_evidence.append(
                {
                    "futures": len(futures),
                    "futures_terminal_within_timeout": all(future.done() for future in futures),
                    "caller_settle_ms": round(settle_ms, 4),
                    "cleanup": cleanup,
                }
            )
    ordered = sorted(latencies)
    return {
        "samples": samples,
        "loaded": loaded,
        "p50_ms": round(statistics.median(ordered), 4),
        "p95_ms": round(_percentile(ordered, 0.95), 4),
        "p99_ms": round(_percentile(ordered, 0.99), 4),
        "max_ms": round(max(ordered), 4),
        "completed_results": completed_results,
        "expected_request_errors": expected_errors,
        "all_futures_terminal_within_timeout": all(
            item["futures_terminal_within_timeout"] for item in sample_evidence
        ),
        "max_caller_settle_ms": round(max(item["caller_settle_ms"] for item in sample_evidence), 4),
        "all_tickets_terminal_at_close": all(
            not cleanup["pending_ticket_present"] and not cleanup["active_ticket_present"]
            for item in sample_evidence
            for cleanup in item["cleanup"]
        ),
        "all_owned_resources_closed": all(
            cleanup["all_owned_resources_closed"]
            for item in sample_evidence
            for cleanup in item["cleanup"]
        ),
        "sample_evidence": sample_evidence,
    }


def run_heartbeat_impact(requests: int, *, blocks: int = 4) -> dict[str, Any]:
    if requests < 100 or blocks < 3:
        raise ValueError("heartbeat impact requires at least 100 requests and three balanced blocks")

    pool_size = 4
    concurrency = pool_size + 1
    heartbeat_interval = 0.02

    def set_interval(pool: PooledSocketTransport, interval: float | None) -> None:
        now = time.monotonic()
        for transport in pool._transports:
            runtime = transport._runtime
            if runtime is None:
                raise AssertionError("heartbeat pool lost an Actor runtime")
            runtime.heartbeat_interval = interval
            generation = runtime.generation
            if generation is not None:
                generation.last_activity_at = now

    next_token = 0
    all_values: list[dict[str, int]] = []

    def run_workers(executor: ThreadPoolExecutor, pool: PooledSocketTransport, total: int) -> float:
        nonlocal next_token
        tokens = list(range(next_token, next_token + total))
        next_token += total
        quotient, remainder = divmod(total, concurrency)
        token_groups = []
        offset = 0
        for index in range(concurrency):
            count = quotient + int(index < remainder)
            token_groups.append(tokens[offset : offset + count])
            offset += count
        barrier = threading.Barrier(concurrency + 1)

        def worker(worker_tokens: list[int]) -> list[dict[str, int]]:
            barrier.wait()
            return [_execute_unique(pool, token) for token in worker_tokens]

        futures = [executor.submit(worker, worker_tokens) for worker_tokens in token_groups]
        started = time.perf_counter()
        barrier.wait()
        values = [value for future in futures for value in future.result()]
        elapsed = time.perf_counter() - started
        if len(values) != total:
            raise AssertionError("heartbeat workload response mismatch")
        all_values.extend(values)
        return elapsed

    samples: dict[str, list[dict[str, Any]]] = {"without_heartbeat": [], "with_heartbeat": []}
    raw_elapsed: dict[str, list[float]] = {"without_heartbeat": [], "with_heartbeat": []}
    block_ratios = []
    ledger = StressLedger()
    with StressServer(server_id=1, ledger=ledger, response_delay=0.005) as server:
        pool = PooledSocketTransport(
            hosts=[server.host],
            timeout=10,
            pool_size=pool_size,
            heartbeat_interval=heartbeat_interval,
            max_pending_requests=256,
        )
        idle_heartbeat_before = server.heartbeat_responses
        pool.connect()
        if not server.wait_for_heartbeat_response_connections(pool_size, timeout=2.0):
            pool.close()
            raise AssertionError("automatic heartbeat did not reach every idle Actor connection")
        idle_probe_heartbeats = server.heartbeat_responses - idle_heartbeat_before
        idle_probe_connections = len(server.heartbeat_responses_by_connection)
        warmup_count = min(100, requests)
        try:
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                paced_heartbeat_before = server.heartbeat_responses
                paced_business_requests = 0
                for _ in range(8):
                    heartbeat_baseline = server.heartbeat_response_snapshot()
                    if not server.wait_for_heartbeat_response_round(heartbeat_baseline, timeout=2.0):
                        raise AssertionError("paced automatic heartbeat round did not complete")
                    run_workers(executor, pool, pool_size)
                    paced_business_requests += pool_size
                paced_heartbeat_requests = server.heartbeat_responses - paced_heartbeat_before
                set_interval(pool, None)
                threading.Event().wait(heartbeat_interval * 1.5)
                base_order = (
                    None,
                    heartbeat_interval,
                    heartbeat_interval,
                    None,
                    heartbeat_interval,
                    None,
                    None,
                    heartbeat_interval,
                )
                for block in range(blocks):
                    order = base_order if block % 2 == 0 else tuple(reversed(base_order))
                    block_elapsed = {"without_heartbeat": [], "with_heartbeat": []}
                    for phase, interval in enumerate(order):
                        set_interval(pool, None)
                        threading.Event().wait(heartbeat_interval * 1.5)
                        try:
                            set_interval(pool, interval)
                            run_workers(executor, pool, warmup_count)
                            set_interval(pool, None)
                            threading.Event().wait(heartbeat_interval * 1.5)
                            gc.collect()
                            gc_enabled = gc.isenabled()
                            gc.disable()
                            try:
                                set_interval(pool, interval)
                                heartbeat_before = server.heartbeat_requests
                                elapsed = run_workers(executor, pool, requests)
                            finally:
                                set_interval(pool, None)
                                if gc_enabled:
                                    gc.enable()
                            key = "without_heartbeat" if interval is None else "with_heartbeat"
                            sample = {
                                "block": block,
                                "phase": phase,
                                "seconds": round(elapsed, 6),
                                "throughput_rps": round(requests / elapsed, 3),
                                "heartbeat_requests": server.heartbeat_requests - heartbeat_before,
                            }
                            samples[key].append(sample)
                            raw_elapsed[key].append(elapsed)
                            block_elapsed[key].append(elapsed)
                        finally:
                            set_interval(pool, None)
                    block_ratios.append(
                        round(
                            sum(block_elapsed["without_heartbeat"]) / sum(block_elapsed["with_heartbeat"]),
                            6,
                        )
                    )
        finally:
            set_interval(pool, None)
            pool.close()

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
    completion_summary = _unique_completion_summary(all_values, ledger)
    return {
        "requests_per_phase": requests,
        "blocks": blocks,
        "phases": blocks * 8,
        "pool_size": pool_size,
        "concurrency": concurrency,
        "response_delay_ms": 5.0,
        "heartbeat_interval_ms": heartbeat_interval * 1000,
        "idle_probe_heartbeats": idle_probe_heartbeats,
        "idle_probe_connections": idle_probe_connections,
        "paced_heartbeat_requests": paced_heartbeat_requests,
        "paced_business_requests": paced_business_requests,
        "heartbeat_during_business": server.heartbeat_during_business,
        "business_requests": len(all_values),
        "without_heartbeat": baseline,
        "with_heartbeat": active,
        "block_throughput_ratios": block_ratios,
        "median_block_throughput_ratio": round(statistics.median(block_ratios), 6),
        "throughput_estimator": "aggregate_elapsed_ratio",
        "throughput_ratio": round(
            sum(raw_elapsed["without_heartbeat"]) / sum(raw_elapsed["with_heartbeat"]),
            6,
        ),
        **completion_summary,
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


def _git_identity() -> dict[str, Any]:
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        )
    except (OSError, subprocess.CalledProcessError):
        return {"implementation_sha": None, "worktree_dirty": None}
    return {"implementation_sha": sha, "worktree_dirty": dirty}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--generations", type=int, default=0)
    parser.add_argument("--requests", type=int, default=0)
    parser.add_argument("--pool-size", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--close-samples", type=int, default=0)
    parser.add_argument("--heartbeat-requests", type=int, default=0)
    parser.add_argument("--idle-seconds", type=float, default=0.0)
    parser.add_argument("--resource-rounds", type=int, default=0)
    parser.add_argument("--resource-warmup", type=int, default=3)
    parser.add_argument("--resource-generations", type=int, default=20)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    source_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    result: dict[str, Any] = {
        "schema": 2,
        "workload_sha256": source_hash,
        "platform": platform.platform(),
        "python": platform.python_version(),
        **_git_identity(),
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
    if args.resource_rounds:
        result["warmed_resources"] = run_warmed_resource_stress(
            warmup_rounds=args.resource_warmup,
            measured_rounds=args.resource_rounds,
            generations_per_round=args.resource_generations,
        )
    encoded = json.dumps(result, ensure_ascii=True, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")


if __name__ == "__main__":
    main()
