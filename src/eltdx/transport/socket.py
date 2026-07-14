"""Synchronous Actor-backed 7709 socket transport facade."""

from __future__ import annotations

import threading
import time
from collections.abc import Sequence
from typing import Any

from eltdx.exceptions import ConnectionClosedError, ResponseTimeoutError
from eltdx.hosts import DEFAULT_HOSTS, resolve_hosts, unique_hosts
from eltdx.protocol.commands import COMMANDS, parse_command_response
from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_HEARTBEAT

from .actor import (
    ActorRuntime,
    FrameEnvelope,
    RuntimeState,
    cancel_ticket,
    close_actor,
    start_actor,
    submit_connect,
    submit_request,
    wait_ticket,
)
from .push import PushBuffer, PushFrame

DEFAULT_HEARTBEAT_INTERVAL = 30.0
DEFAULT_PUSH_QUEUE_SIZE = 1024
DEFAULT_PUSH_QUEUE_BYTES = 8 * 1024 * 1024


class SocketTransport:
    """Synchronous facade for one single-threaded non-blocking Actor."""

    def __init__(
        self,
        hosts: Sequence[str] | None = None,
        *,
        timeout: float = 8.0,
        heartbeat_interval: float | None = DEFAULT_HEARTBEAT_INTERVAL,
        push_queue_size: int = DEFAULT_PUSH_QUEUE_SIZE,
        push_queue_bytes: int = DEFAULT_PUSH_QUEUE_BYTES,
    ) -> None:
        self._hosts = unique_hosts(list(hosts or DEFAULT_HOSTS))
        if not self._hosts:
            raise ValueError("at least one host is required")
        if timeout <= 0:
            raise ValueError("timeout must be > 0")
        self._timeout = float(timeout)
        self._heartbeat_interval = heartbeat_interval
        self._push_queue_size = int(push_queue_size)
        self._push_queue_bytes = int(push_queue_bytes)
        if self._push_queue_size <= 0:
            raise ValueError("push_queue_size must be > 0")
        if self._push_queue_bytes <= 0:
            raise ValueError("push_queue_bytes must be > 0")

        self._lifecycle = threading.Condition()
        self._request_lock = threading.Lock()
        self._runtime: ActorRuntime | None = None
        self._push_buffer: PushBuffer | None = None
        self._epoch = 0
        self._starting = False
        self._closing = False
        self._last_handshake: Any = None
        self._last_heartbeat: Any = None

    @property
    def connected_host(self) -> str | None:
        with self._lifecycle:
            runtime = self._runtime
        return runtime.connected_host if runtime is not None else None

    @property
    def last_handshake(self) -> Any:
        with self._lifecycle:
            runtime = self._runtime
            cached = self._last_handshake
        return runtime.last_handshake if runtime is not None and runtime.last_handshake is not None else cached

    @property
    def last_heartbeat(self) -> Any:
        with self._lifecycle:
            runtime = self._runtime
            cached = self._last_heartbeat
        return runtime.last_heartbeat if runtime is not None and runtime.last_heartbeat is not None else cached

    @property
    def pending_push_count(self) -> int:
        with self._lifecycle:
            push_buffer = self._push_buffer
        return push_buffer.pending_count if push_buffer is not None else 0

    def connect(self) -> None:
        runtime = self._ensure_runtime()
        deadline = time.monotonic() + self._timeout
        if not self._acquire_request_lock(deadline):
            raise ResponseTimeoutError("7709 response timed out during queue")
        try:
            self._require_current_runtime(runtime)
            wait_ticket(submit_connect(runtime, deadline))
        finally:
            self._request_lock.release()

    def close(self) -> None:
        with self._lifecycle:
            while self._closing:
                self._lifecycle.wait()
            self._closing = True
            self._epoch += 1
            while self._starting:
                self._lifecycle.wait()
            runtime = self._runtime
            push_buffer = self._push_buffer
        if push_buffer is not None:
            push_buffer.close()
        try:
            if runtime is not None:
                close_actor(runtime)
        except BaseException:
            with self._lifecycle:
                self._closing = False
                self._lifecycle.notify_all()
            raise
        with self._lifecycle:
            if self._runtime is runtime:
                if runtime is not None:
                    if runtime.last_handshake is not None:
                        self._last_handshake = runtime.last_handshake
                    if runtime.last_heartbeat is not None:
                        self._last_heartbeat = runtime.last_heartbeat
                self._runtime = None
                self._push_buffer = None
            self._closing = False
            self._lifecycle.notify_all()

    def execute(self, command: int, payload: dict[str, Any] | None = None) -> Any:
        request_payload = dict(payload or {})
        runtime = self._ensure_runtime()
        deadline = time.monotonic() + self._timeout
        if not self._acquire_request_lock(deadline):
            raise ResponseTimeoutError("7709 response timed out during queue")
        ticket = None
        try:
            self._require_current_runtime(runtime)
            ticket = submit_request(
                runtime,
                lease_id=0,
                command=command,
                payload=request_payload,
                deadline=deadline,
                retry_safe=_retry_safe(command),
            )
            envelope = wait_ticket(ticket)
        except BaseException:
            if ticket is not None and not ticket.completed.is_set():
                cancel_ticket(runtime, ticket)
                ticket.completed.wait(max(0.0, deadline - time.monotonic()) + 0.05)
            raise
        finally:
            self._request_lock.release()

        if not isinstance(envelope, FrameEnvelope):
            raise ConnectionClosedError("7709 Actor returned an invalid response envelope")
        result = parse_command_response(envelope.command, envelope.response, envelope.request_payload_snapshot)
        if command == TYPE_HANDSHAKE:
            self._last_handshake = result
        elif command == TYPE_HEARTBEAT:
            self._last_heartbeat = result
        return result

    def request(self, command: str) -> str:
        if command == "ping":
            return "pong"
        raise ValueError(f"unsupported command: {command}")

    def poll_push(self, timeout: float | None = 0.0, *, parse: bool = False) -> Any:
        with self._lifecycle:
            push_buffer = self._push_buffer
        if push_buffer is None:
            return None
        frame = push_buffer.poll(timeout)
        if frame is None or not parse:
            return frame.response if frame is not None else None
        return _parse_push(frame)

    def drain_pushes(self, *, parse: bool = False) -> list[Any]:
        with self._lifecycle:
            push_buffer = self._push_buffer
        if push_buffer is None:
            return []
        frames = push_buffer.drain()
        if parse:
            return [_parse_push(frame) for frame in frames]
        return [frame.response for frame in frames]

    def _ensure_runtime(self) -> ActorRuntime:
        while True:
            with self._lifecycle:
                runtime = self._runtime
                if runtime is not None:
                    if runtime.state is RuntimeState.RUNNING:
                        return runtime
                    raise ConnectionClosedError(f"7709 Actor is not usable: {runtime.state.name}")
                if self._closing:
                    raise ConnectionClosedError("7709 transport is closing")
                if self._starting:
                    self._lifecycle.wait()
                    continue
                self._starting = True
                observed_epoch = self._epoch
                candidate_epoch = observed_epoch + 1
                break

        candidate: ActorRuntime | None = None
        try:
            endpoints = resolve_hosts(self._hosts)
            push_buffer = PushBuffer(
                candidate_epoch,
                max_frames=self._push_queue_size,
                max_bytes=self._push_queue_bytes,
            )
            candidate = start_actor(
                candidate_epoch,
                endpoints,
                push_buffer=push_buffer,
                heartbeat_interval=self._heartbeat_interval,
                request_timeout=self._timeout,
            )
        except BaseException:
            with self._lifecycle:
                self._starting = False
                self._lifecycle.notify_all()
            raise

        publish = False
        with self._lifecycle:
            if self._epoch == observed_epoch and not self._closing and self._runtime is None:
                self._epoch = candidate_epoch
                self._runtime = candidate
                self._push_buffer = push_buffer
                publish = True
            self._starting = False
            self._lifecycle.notify_all()
        if not publish:
            close_actor(candidate)
            raise ConnectionClosedError("7709 transport changed while resolving endpoints")
        return candidate

    def _require_current_runtime(self, runtime: ActorRuntime) -> None:
        with self._lifecycle:
            if self._runtime is not runtime or self._closing:
                raise ConnectionClosedError("7709 transport runtime changed")

    def _acquire_request_lock(self, deadline: float) -> bool:
        remaining = max(0.0, deadline - time.monotonic())
        return self._request_lock.acquire(timeout=remaining)


def _retry_safe(command: int) -> bool:
    for spec in COMMANDS.values():
        if spec.code == command:
            return spec.retry_safe
    return False


def _parse_push(frame: PushFrame) -> Any:
    return parse_command_response(frame.response.msg_type, frame.response, {})
