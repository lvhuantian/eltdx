"""Synchronous Actor-backed 7709 socket transport facade."""

from __future__ import annotations

import ipaddress
import threading
import time
import weakref
from collections.abc import Sequence
from dataclasses import dataclass, field as dataclass_field
from typing import Any

from eltdx.exceptions import ConnectionClosedError, ResponseTimeoutError, TransportCloseTimeoutError
from eltdx.hosts import DEFAULT_HOSTS, ResolvedEndpoint, resolve_hosts, unique_hosts
from eltdx.protocol.commands import COMMANDS, parse_command_response
from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_HEARTBEAT

from .actor import (
    ActorRuntime,
    ActorSnapshot,
    ActorStartupError,
    ConnectTicket,
    FrameEnvelope,
    IdentityGate,
    RequestTicket,
    RuntimeState,
    _acquire_control_lock,
    abandon_actor,
    actor_snapshot,
    cancel_ticket,
    close_actor,
    request_actor_stop,
    start_actor,
    submit_connect,
    submit_request,
    wait_ticket,
)
from .push import PushBuffer, PushFrame

DEFAULT_HEARTBEAT_INTERVAL = 30.0
DEFAULT_PUSH_QUEUE_SIZE = 1024
DEFAULT_PUSH_QUEUE_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class TransportDiagnostics:
    epoch: int
    actor: ActorSnapshot | None
    push_frames: int
    push_bytes: int
    push_dropped: int
    push_max_frames: int
    push_max_bytes: int


class _TerminalCompletion:
    def __init__(
        self,
        callback: Any = None,
        request_gate: _RequestGate | None = None,
        request_token: object | None = None,
    ) -> None:
        self._callback = callback
        self._request_gate = request_gate
        self._request_token = request_token
        self._guard = threading.Lock()
        self._done = False

    def __call__(self, ticket: object | None) -> None:
        with self._guard:
            if self._done:
                return
            self._done = True
        try:
            if self._callback is not None:
                self._callback(ticket)
        finally:
            if self._request_gate is not None and self._request_token is not None:
                self._request_gate.release_token(self._request_token)


class _RequestGate(IdentityGate):
    """One cross-thread request owner, released only by exact token identity."""

    def release(self) -> None:
        token = getattr(self._compat, "token", None)
        if token is None or not self.release_token(token):
            raise RuntimeError("cannot release an unowned request gate")
        self._compat.token = None


class _SubmissionOwnership:
    __slots__ = ("ticket",)

    def __init__(self) -> None:
        self.ticket: ConnectTicket | RequestTicket | None = None

    def claim(self, ticket: ConnectTicket | RequestTicket) -> None:
        self.ticket = ticket


class _RequestLockOwnership:
    __slots__ = ("acquired", "token")

    def __init__(self) -> None:
        self.acquired = False
        self.token = object()


@dataclass(slots=True)
class _CloseAttempt:
    generation: int
    completed: threading.Event = dataclass_field(default_factory=threading.Event)
    error: BaseException | None = None


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
        _shared_push_buffer: PushBuffer | None = None,
        _runtime_epoch: int | None = None,
        _resolved_endpoints: tuple[ResolvedEndpoint, ...] | None = None,
        _actor_fatal_callback: Any = None,
        _runtime_started_callback: Any = None,
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
        self._request_lock = _RequestGate()
        self._submission_gate = _RequestGate()
        self._runtime: ActorRuntime | None = None
        self._candidate: ActorRuntime | None = None
        self._unpublished_candidate: ActorRuntime | None = None
        self._candidate_admitted = False
        self._candidate_registration: Any = None
        self._push_buffer: PushBuffer | None = None
        self._epoch = 0
        self._close_generation = 0
        self._starting = False
        self._startup_aborted = threading.Event()
        self._closing = False
        self._close_attempt: _CloseAttempt | None = None
        self._close_failed = False
        self._last_handshake: Any = None
        self._last_heartbeat: Any = None
        self._shared_push_buffer = _shared_push_buffer
        self._fixed_runtime_epoch = _runtime_epoch
        self._resolved_endpoints = _resolved_endpoints
        self._owns_push_buffer = _shared_push_buffer is None
        self._actor_fatal_callback = _actor_fatal_callback
        self._runtime_started_callback = _runtime_started_callback
        self._heartbeat_allowed: Any = None
        self._successor_grace = 0.0
        self._terminal_yield = False
        self._pool_runtime_retired = False
        self._resolver_claim: tuple[int, int] | None = None
        self._finalizer: weakref.finalize | None = None

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

    @property
    def diagnostics(self) -> TransportDiagnostics:
        with self._lifecycle:
            runtime = self._runtime
            push_buffer = self._push_buffer
            epoch = self._epoch
        push = push_buffer.snapshot() if push_buffer is not None else None
        return TransportDiagnostics(
            epoch=epoch,
            actor=actor_snapshot(runtime) if runtime is not None else None,
            push_frames=push.frame_count if push is not None else 0,
            push_bytes=push.byte_count if push is not None else 0,
            push_dropped=push.dropped_total if push is not None else 0,
            push_max_frames=push.max_frames_observed if push is not None else 0,
            push_max_bytes=push.max_bytes_observed if push is not None else 0,
        )

    def connect(self) -> None:
        if _requires_dns(self._hosts) and self._resolved_endpoints is None:
            _, close_generation, _ = self._preflight_endpoints()
            deadline = time.monotonic() + self._timeout
        else:
            deadline = time.monotonic() + self._timeout
            _, close_generation, deadline = self._preflight_endpoints(deadline)
        runtime = self._ensure_runtime(deadline, expected_close_generation=close_generation)
        self._connect_with_deadline(
            deadline=deadline,
            completion=None,
            runtime=runtime,
            lock_slot=True,
        )

    def _connect_with_deadline(
        self,
        *,
        deadline: float,
        completion: Any,
        runtime: ActorRuntime | None = None,
        lock_slot: bool = True,
        lease_id: int = 0,
        expected_runtime_epoch: int | None = None,
        submission_check: Any = None,
    ) -> None:
        if runtime is None:
            try:
                runtime = self._ensure_runtime(deadline, expected_runtime_epoch=expected_runtime_epoch)
            except BaseException:
                if completion is not None:
                    completion(None)
                raise
        lock_ownership = _RequestLockOwnership()
        terminal: Any = None
        terminal_owned_by_ticket = False
        ownership = _SubmissionOwnership()
        ticket: ConnectTicket | None = None
        submission_token = object()
        try:
            if lock_slot:
                if not self._acquire_request_lock(deadline, ownership=lock_ownership):
                    if completion is not None:
                        completion(None)
                    raise ResponseTimeoutError("7709 response timed out during queue")
                terminal = _TerminalCompletion(completion, self._request_lock, lock_ownership.token)
            else:
                terminal = completion
            try:
                acquired = _acquire_gate_token(self._submission_gate, submission_token, deadline)
            except BaseException:
                _release_gate_token(self._submission_gate, submission_token)
                raise
            if not acquired:
                raise ResponseTimeoutError("7709 response timed out during connect submission")
            try:
                if submission_check is not None:
                    submission_check()
                self._require_current_runtime(
                    runtime,
                    expected_runtime_epoch=expected_runtime_epoch,
                    deadline=deadline,
                )
                ticket = submit_connect(
                    runtime,
                    deadline,
                    lease_id=lease_id,
                    completion=terminal,
                    submission_claim=ownership.claim,
                )
                terminal_owned_by_ticket = True
            finally:
                _release_gate_token(self._submission_gate, submission_token)
            wait_ticket(ticket)
            if expected_runtime_epoch is not None and not self._pool_runtime_is_active(
                expected_runtime_epoch,
                deadline=deadline,
            ):
                raise ConnectionClosedError("7709 pool closed during connect")
        except BaseException:
            owned_ticket = ticket or ownership.ticket
            terminal_owned_by_ticket = owned_ticket is not None
            cancel_accepted = False
            cancel_failed = False
            if owned_ticket is not None and not owned_ticket.completed.is_set():
                try:
                    cancel_accepted = cancel_ticket(runtime, owned_ticket, deadline=deadline)
                except BaseException:
                    cancel_failed = True
            if (
                owned_ticket is not None
                and not cancel_accepted
                and not cancel_failed
                and not owned_ticket.completed.is_set()
            ):
                terminal_owned_by_ticket = False
            if not terminal_owned_by_ticket and terminal is not None:
                terminal(None)
            elif not terminal_owned_by_ticket and lock_slot:
                self._request_lock.release_token(lock_ownership.token)
            raise

    def close(self) -> None:
        self._close_with_timeout(1.0)

    def _close_with_timeout(self, timeout: float) -> None:
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            if not self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic())):
                self._close_failed = True
                raise TransportCloseTimeoutError("7709 transport lifecycle blocked close before deadline")
            wait_attempt: _CloseAttempt | None = None
            try:
                current_attempt = self._close_attempt
                if current_attempt is not None and current_attempt.completed.is_set():
                    if self._close_attempt is current_attempt:
                        self._close_attempt = None
                        self._closing = False
                        self._lifecycle.notify_all()
                    current_attempt = None
                if self._closing:
                    if current_attempt is None:
                        self._close_failed = True
                        raise TransportCloseTimeoutError("7709 transport close owner state is inconsistent")
                    wait_attempt = current_attempt
                remaining = deadline - time.monotonic()
            finally:
                self._lifecycle.release()
            if wait_attempt is not None:
                if remaining <= 0 or not wait_attempt.completed.wait(timeout=remaining):
                    self._close_failed = True
                    raise TransportCloseTimeoutError("7709 transport close is already in progress")
                continue
            submission_token = object()
            try:
                submission_acquired = remaining > 0 and _acquire_gate_token(
                    self._submission_gate,
                    submission_token,
                    deadline,
                )
            except BaseException:
                _release_gate_token(self._submission_gate, submission_token)
                raise
            if not submission_acquired:
                self._close_failed = True
                runtime = None
                candidate = None
                acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
                if acquired:
                    try:
                        self._close_generation += 1
                        self._epoch += 1
                        runtime = self._runtime
                        candidate = self._candidate or self._unpublished_candidate
                        self._lifecycle.notify_all()
                    finally:
                        self._lifecycle.release()
                items = _unique_runtimes(runtime, candidate)
                stop_error = _request_actor_stop_all(items, deadline=deadline)
                try:
                    _mark_failed_closing(items, deadline=deadline)
                except BaseException as exc:
                    if stop_error is None:
                        stop_error = exc
                error = TransportCloseTimeoutError("7709 transport submission did not finish before close deadline")
                if stop_error is not None:
                    error.__cause__ = stop_error
                raise error
            retry = False
            owner_claimed = False
            attempt: _CloseAttempt | None = None
            try:
                try:
                    acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
                    if not acquired:
                        self._close_failed = True
                        raise TransportCloseTimeoutError(
                            "7709 transport lifecycle blocked close owner claim before deadline"
                        )
                    try:
                        current_attempt = self._close_attempt
                        if current_attempt is not None and current_attempt.completed.is_set():
                            if self._close_attempt is current_attempt:
                                self._close_attempt = None
                                self._closing = False
                            current_attempt = None
                        if self._closing:
                            retry = True
                        else:
                            attempt = _CloseAttempt(self._close_generation + 1)
                            self._close_attempt = attempt
                            self._closing = True
                            owner_claimed = True
                            self._close_generation += 1
                            self._epoch += 1
                            runtime = self._runtime
                            candidate = self._candidate or self._unpublished_candidate
                            self._lifecycle.notify_all()
                    finally:
                        self._lifecycle.release()
                finally:
                    _release_gate_token(self._submission_gate, submission_token)
            except BaseException:
                if owner_claimed and attempt is not None:
                    self._abort_close_owner(deadline, attempt)
                raise
            if not retry:
                break
        assert attempt is not None
        try:
            self._finish_close_owner(deadline, runtime, candidate)
        except BaseException as exc:
            self._abort_close_owner(deadline, attempt, error=exc)
            raise
        self._complete_close_owner(deadline, attempt)

    def _abort_close_owner(
        self,
        deadline: float,
        attempt: _CloseAttempt,
        *,
        error: BaseException | None = None,
    ) -> None:
        self._close_failed = True
        attempt.error = error
        failed_items: tuple[ActorRuntime, ...] = ()
        acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if acquired:
            try:
                failed_items = _unique_runtimes(
                    self._runtime,
                    self._candidate,
                    self._unpublished_candidate,
                )
            finally:
                self._lifecycle.release()
        _request_actor_stop_all(failed_items, deadline=deadline)
        try:
            _mark_failed_closing(failed_items, deadline=deadline)
        except BaseException:
            pass
        acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if acquired:
            try:
                if self._close_attempt is attempt:
                    self._close_attempt = None
                    self._closing = False
                try:
                    self._lifecycle.notify_all()
                except BaseException:
                    pass
            finally:
                self._lifecycle.release()
        attempt.completed.set()

    def _complete_close_owner(self, deadline: float, attempt: _CloseAttempt) -> None:
        acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            self._close_failed = True
            error = TransportCloseTimeoutError("7709 transport lifecycle blocked close completion before deadline")
            attempt.error = error
            attempt.completed.set()
            raise error
        try:
            if self._close_attempt is not attempt:
                self._close_failed = True
                raise RuntimeError("7709 transport close owner identity changed")
            self._close_attempt = None
            self._closing = False
            self._lifecycle.notify_all()
        except BaseException as exc:
            self._close_failed = True
            attempt.error = exc
            raise
        finally:
            self._lifecycle.release()
            attempt.completed.set()

    def _finish_close_owner(
        self,
        deadline: float,
        initial_runtime: ActorRuntime | None,
        initial_candidate: ActorRuntime | None,
    ) -> None:
        initial_items = _unique_runtimes(initial_runtime, initial_candidate)
        close_errors: list[BaseException] = []
        stop_error = _request_actor_stop_all(initial_items, deadline=deadline)
        if stop_error is not None:
            close_errors.append(stop_error)
        startup_timeout_items: tuple[ActorRuntime, ...] | None = None
        acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 transport lifecycle blocked close before deadline")
        try:
            if self._startup_aborted.is_set():
                self._starting = False
                self._lifecycle.notify_all()
            while self._starting:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._lifecycle.wait(timeout=remaining):
                    startup_timeout_items = _unique_runtimes(
                        self._runtime,
                        self._candidate,
                        self._unpublished_candidate,
                    )
                    break
            if startup_timeout_items is not None:
                runtime = None
                candidate = None
                push_buffer = None
            else:
                runtime = self._runtime
                candidate = self._candidate or self._unpublished_candidate
                push_buffer = self._push_buffer
        finally:
            self._lifecycle.release()
        if startup_timeout_items is not None:
            _mark_failed_closing(startup_timeout_items, deadline=deadline)
            error = TransportCloseTimeoutError("7709 Actor startup did not finish before close deadline")
            if close_errors:
                error.__cause__ = close_errors[0]
            raise error
        items = _unique_runtimes(runtime, candidate)
        newly_owned_items = tuple(
            item for item in items if all(item is not initial for initial in initial_items)
        )
        stop_error = _request_actor_stop_all(newly_owned_items, deadline=deadline)
        if stop_error is not None:
            close_errors.append(stop_error)
        owned_push_buffers = _unique_owned_push_buffers(
            push_buffer if self._owns_push_buffer else None,
            items,
        )
        push_close_errors: list[tuple[PushBuffer, BaseException]] = []
        for owned_push_buffer in owned_push_buffers:
            push_closed = False
            try:
                owned_push_buffer.close_before_deadline(deadline)
            except BaseException as exc:
                push_close_errors.append((owned_push_buffer, exc))
            try:
                push_closed = owned_push_buffer.snapshot_before_deadline(deadline).closed
            except BaseException as exc:
                push_close_errors.append((owned_push_buffer, exc))
            else:
                if not push_closed and not any(item is owned_push_buffer for item, _ in push_close_errors):
                    push_close_errors.append(
                        (owned_push_buffer, RuntimeError("7709 owned push buffer did not close"))
                    )
            if push_closed:
                for item in items:
                    try:
                        _clear_resolved_push_cleanup(item, owned_push_buffer, deadline=deadline)
                    except BaseException as exc:
                        close_errors.append(exc)
        for item in items:
            try:
                close_actor(item, timeout=max(0.0, deadline - time.monotonic()))
            except BaseException as exc:
                close_errors.append(exc)
        unresolved_push_errors: list[BaseException] = []
        for owned_push_buffer in owned_push_buffers:
            try:
                push_closed = owned_push_buffer.snapshot_before_deadline(deadline).closed
            except BaseException as exc:
                unresolved_push_errors.append(exc)
                continue
            if push_closed:
                for item in items:
                    try:
                        _clear_resolved_push_cleanup(item, owned_push_buffer, deadline=deadline)
                    except BaseException as exc:
                        close_errors.append(exc)
            else:
                unresolved_push_errors.append(
                    next(
                        (error for buffer, error in push_close_errors if buffer is owned_push_buffer),
                        RuntimeError("7709 owned push buffer did not close"),
                    )
                )
        if unresolved_push_errors:
            error = TransportCloseTimeoutError("7709 Actor resource cleanup failed")
            error.__cause__ = unresolved_push_errors[0]
            close_errors.append(error)
            _mark_failed_closing(items, deadline=deadline)
        if close_errors:
            raise close_errors[0]
        acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 transport lifecycle blocked close publication before deadline")
        try:
            close_failed = self._close_failed
        finally:
            self._lifecycle.release()
        if close_failed:
            _mark_failed_closed(_unique_runtimes(runtime, candidate), deadline=deadline)
        acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 transport lifecycle blocked close publication before deadline")
        try:
            failed_runtime: ActorRuntime | None = None
            if self._runtime is runtime:
                if runtime is not None:
                    if runtime.last_handshake is not None:
                        self._last_handshake = runtime.last_handshake
                    if runtime.last_heartbeat is not None:
                        self._last_heartbeat = runtime.last_heartbeat
                failed_closed = runtime is not None and (
                    self._close_failed
                    or runtime.state in (
                        RuntimeState.FAILED,
                        RuntimeState.FAILED_CLOSING,
                        RuntimeState.FAILED_CLOSED,
                    )
                )
                if not failed_closed:
                    self._runtime = None
                    self._push_buffer = None
                else:
                    failed_runtime = runtime
            if self._candidate is candidate or self._unpublished_candidate is candidate:
                candidate_failed = candidate is not None and (
                    self._close_failed
                    or candidate.state in (
                        RuntimeState.FAILED,
                        RuntimeState.FAILED_CLOSING,
                        RuntimeState.FAILED_CLOSED,
                    )
                )
                if candidate_failed:
                    failed_runtime = candidate
                    if self._runtime is None:
                        self._runtime = candidate
                self._candidate = None
                if self._unpublished_candidate is candidate:
                    self._unpublished_candidate = None
                self._candidate_admitted = False
                self._candidate_registration = None
            if failed_runtime is None and self._runtime is None:
                self._push_buffer = None
            if self._runtime is None and self._candidate is None and self._unpublished_candidate is None:
                if self._finalizer is not None:
                    self._finalizer.detach()
                    self._finalizer = None
        finally:
            self._lifecycle.release()

    def execute(self, command: int, payload: dict[str, Any] | None = None) -> Any:
        request_payload = dict(payload or {})
        if _requires_dns(self._hosts) and self._resolved_endpoints is None:
            _, close_generation, _ = self._preflight_endpoints()
            deadline = time.monotonic() + self._timeout
        else:
            deadline = time.monotonic() + self._timeout
            _, close_generation, deadline = self._preflight_endpoints(deadline)
        runtime = self._ensure_runtime(deadline, expected_close_generation=close_generation)
        return self._execute_with_lease(
            command,
            request_payload,
            lease_id=0,
            deadline=deadline,
            completion=None,
            runtime=runtime,
        )

    def _execute_with_lease(
        self,
        command: int,
        payload: dict[str, Any] | None,
        *,
        lease_id: int,
        deadline: float,
        completion: Any,
        runtime: ActorRuntime | None = None,
        lock_slot: bool = True,
        expected_runtime_epoch: int | None = None,
        submission_check: Any = None,
    ) -> Any:
        try:
            request_payload = dict(payload or {})
        except BaseException:
            if completion is not None:
                completion(None)
            raise
        if runtime is None:
            try:
                runtime = self._ensure_runtime(deadline, expected_runtime_epoch=expected_runtime_epoch)
            except BaseException:
                if completion is not None:
                    completion(None)
                raise
        lock_ownership = _RequestLockOwnership()
        terminal: Any = None
        terminal_owned_by_ticket = False
        ownership = _SubmissionOwnership()
        ticket: RequestTicket | None = None
        submission_token = object()
        try:
            if lock_slot:
                if not self._acquire_request_lock(deadline, ownership=lock_ownership):
                    if completion is not None:
                        completion(None)
                    raise ResponseTimeoutError("7709 response timed out during queue")
                terminal = _TerminalCompletion(completion, self._request_lock, lock_ownership.token)
            else:
                terminal = completion
            try:
                acquired = _acquire_gate_token(self._submission_gate, submission_token, deadline)
            except BaseException:
                _release_gate_token(self._submission_gate, submission_token)
                raise
            if not acquired:
                raise ResponseTimeoutError("7709 response timed out during request submission")
            try:
                if submission_check is not None:
                    submission_check()
                self._require_current_runtime(
                    runtime,
                    expected_runtime_epoch=expected_runtime_epoch,
                    deadline=deadline,
                )
                ticket = submit_request(
                    runtime,
                    lease_id=lease_id,
                    command=command,
                    payload=request_payload,
                    deadline=deadline,
                    retry_safe=_retry_safe(command),
                    completion=terminal,
                    submission_claim=ownership.claim,
                )
                terminal_owned_by_ticket = True
            finally:
                _release_gate_token(self._submission_gate, submission_token)
            envelope = wait_ticket(ticket)
        except BaseException:
            owned_ticket = ticket or ownership.ticket
            terminal_owned_by_ticket = owned_ticket is not None
            cancel_accepted = False
            cancel_failed = False
            if owned_ticket is not None and not owned_ticket.completed.is_set():
                try:
                    cancel_accepted = cancel_ticket(runtime, owned_ticket, deadline=deadline)
                except BaseException:
                    cancel_failed = True
            if (
                owned_ticket is not None
                and not cancel_accepted
                and not cancel_failed
                and not owned_ticket.completed.is_set()
            ):
                terminal_owned_by_ticket = False
            if not terminal_owned_by_ticket and terminal is not None:
                terminal(None)
            raise
        finally:
            if lock_slot and not terminal_owned_by_ticket:
                if terminal is not None:
                    terminal(None)
                else:
                    self._request_lock.release_token(lock_ownership.token)

        if not isinstance(envelope, FrameEnvelope):
            raise ConnectionClosedError("7709 Actor returned an invalid response envelope")
        if expected_runtime_epoch is not None and not self._pool_runtime_is_active(
            expected_runtime_epoch,
            deadline=deadline,
        ):
            raise ConnectionClosedError("7709 pool closed before response delivery")
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

    def _preflight_endpoints(
        self,
        deadline: float | None = None,
    ) -> tuple[tuple[ResolvedEndpoint, ...], int, float | None]:
        if deadline is None:
            self._lifecycle.acquire()
            acquired = True
        else:
            acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise ResponseTimeoutError("7709 response timed out during endpoint preflight")
        try:
            invocation_close_generation = self._close_generation
            invocation_epoch = self._epoch
        finally:
            self._lifecycle.release()
        claim = (invocation_close_generation, invocation_epoch)
        while True:
            if deadline is None:
                self._lifecycle.acquire()
                acquired = True
            else:
                acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
            if not acquired:
                raise ResponseTimeoutError("7709 response timed out during endpoint preflight")
            try:
                if self._close_generation != invocation_close_generation:
                    raise ConnectionClosedError("7709 transport closed during endpoint resolution")
                if self._epoch != invocation_epoch:
                    raise ConnectionClosedError("7709 transport changed during endpoint resolution")
                if self._close_failed:
                    raise ConnectionClosedError("7709 Actor is not usable: FAILED_CLOSED")
                if self._pool_runtime_retired or self._closing:
                    raise ConnectionClosedError("7709 transport is not available for endpoint resolution")
                if self._resolved_endpoints is not None:
                    return self._resolved_endpoints, invocation_close_generation, deadline
                if self._resolver_claim == claim:
                    remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
                    if remaining == 0 or not self._lifecycle.wait(timeout=remaining):
                        raise ResponseTimeoutError("7709 response timed out during endpoint resolution")
                    continue
                self._resolver_claim = claim
                break
            finally:
                self._lifecycle.release()
        dns_preflight = _requires_dns(self._hosts)
        try:
            endpoints = resolve_hosts(self._hosts)
        except BaseException as exc:
            cleanup_deadline = time.monotonic() + self._timeout if dns_preflight else deadline
            if cleanup_deadline is None:
                self._lifecycle.acquire()
                acquired = True
            else:
                acquired = self._lifecycle.acquire(timeout=max(0.0, cleanup_deadline - time.monotonic()))
            if acquired:
                try:
                    if self._resolver_claim == claim:
                        self._resolver_claim = None
                    self._lifecycle.notify_all()
                finally:
                    self._lifecycle.release()
            if isinstance(exc, OSError):
                raise ConnectionClosedError("7709 unable to resolve any configured host") from exc
            raise
        if dns_preflight and deadline is not None:
            deadline = time.monotonic() + self._timeout
        if deadline is None:
            self._lifecycle.acquire()
            acquired = True
        else:
            acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise ResponseTimeoutError("7709 response timed out publishing resolved endpoints")
        try:
            valid = (
                self._resolver_claim == claim
                and self._close_generation == invocation_close_generation
                and self._epoch == invocation_epoch
                and not self._close_failed
                and not self._pool_runtime_retired
                and not self._closing
            )
            if valid and self._resolved_endpoints is None:
                self._resolved_endpoints = endpoints
            resolved = self._resolved_endpoints
            if self._resolver_claim == claim:
                self._resolver_claim = None
            self._lifecycle.notify_all()
        finally:
            self._lifecycle.release()
        if not valid or resolved is None:
            raise ConnectionClosedError("7709 transport changed during endpoint resolution")
        return resolved, invocation_close_generation, deadline

    def _ensure_runtime(
        self,
        deadline: float | None = None,
        *,
        expected_runtime_epoch: int | None = None,
        expected_close_generation: int | None = None,
    ) -> ActorRuntime:
        endpoints, preflight_close_generation, deadline = self._preflight_endpoints(deadline)
        if expected_close_generation is None:
            expected_close_generation = preflight_close_generation
        elif expected_close_generation != preflight_close_generation:
            raise ConnectionClosedError("7709 transport closed after endpoint resolution")
        self._acquire_lifecycle_for_request(deadline, "Actor startup")
        try:
            invocation_epoch = self._epoch
            invocation_close_generation = expected_close_generation
            if self._close_generation != invocation_close_generation:
                raise ConnectionClosedError("7709 transport closed after endpoint resolution")
        finally:
            self._lifecycle.release()
        while True:
            if not self._pool_runtime_is_active(expected_runtime_epoch, deadline=deadline):
                raise ConnectionClosedError("7709 pool runtime epoch is no longer active")
            self._acquire_lifecycle_for_request(deadline, "Actor startup")
            try:
                if self._startup_aborted.is_set():
                    self._starting = False
                    self._lifecycle.notify_all()
                if self._close_generation != invocation_close_generation:
                    raise ConnectionClosedError("7709 transport closed during Actor startup")
                if expected_runtime_epoch is not None and self._fixed_runtime_epoch != expected_runtime_epoch:
                    raise ConnectionClosedError("7709 pool runtime epoch changed")
                if self._close_failed:
                    raise ConnectionClosedError("7709 Actor is not usable: FAILED_CLOSED")
                if self._pool_runtime_retired:
                    raise ConnectionClosedError("7709 pool runtime epoch is retired")
                runtime = self._runtime
                if runtime is not None:
                    if runtime.state is RuntimeState.RUNNING and (
                        expected_runtime_epoch is None or runtime.runtime_epoch == expected_runtime_epoch
                    ):
                        return runtime
                    raise ConnectionClosedError(f"7709 Actor is not usable: {runtime.state.name}")
                if self._closing:
                    raise ConnectionClosedError("7709 transport is closing")
                if self._epoch != invocation_epoch:
                    raise ConnectionClosedError("7709 transport changed during Actor startup")
                if self._starting:
                    remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
                    if remaining == 0 or not self._lifecycle.wait(timeout=remaining):
                        raise ResponseTimeoutError("7709 response timed out during Actor startup")
                    continue
                if self._candidate is not None or self._unpublished_candidate is not None:
                    raise ConnectionClosedError("7709 transport has an Actor candidate awaiting cleanup")
                self._startup_aborted.clear()
                self._starting = True
                observed_epoch = self._epoch
                candidate_epoch = self._fixed_runtime_epoch or (observed_epoch + 1)
                registration = self._runtime_started_callback
                break
            finally:
                self._lifecycle.release()

        candidate: ActorRuntime | None = None
        try:
            self._acquire_lifecycle_for_request(deadline, "Actor startup")
            try:
                if self._epoch != observed_epoch or self._closing or self._close_failed:
                    raise ConnectionClosedError("7709 transport changed while resolving endpoints")
            finally:
                self._lifecycle.release()
            if deadline is not None and time.monotonic() >= deadline:
                raise ResponseTimeoutError("7709 response timed out during Actor startup")
            if not self._pool_runtime_is_active(expected_runtime_epoch, deadline=deadline):
                raise ConnectionClosedError("7709 pool runtime epoch is no longer active")
            push_buffer = self._shared_push_buffer or PushBuffer(
                candidate_epoch, max_frames=self._push_queue_size, max_bytes=self._push_queue_bytes
            )
            candidate = start_actor(
                candidate_epoch,
                endpoints,
                push_buffer=push_buffer,
                heartbeat_interval=self._heartbeat_interval,
                heartbeat_allowed=self._heartbeat_allowed,
                request_timeout=self._timeout,
                owns_push_buffer=self._owns_push_buffer,
                fatal_callback=self._actor_fatal_callback,
                successor_grace=self._successor_grace,
                terminal_yield=self._terminal_yield,
                candidate_callback=lambda runtime: self._own_candidate(runtime, registration, deadline=deadline),
                startup_timeout=1.0 if deadline is None else max(0.0, deadline - time.monotonic()),
            )
            pool_active = self._pool_runtime_is_active(expected_runtime_epoch, deadline=deadline)
            self._acquire_lifecycle_for_request(deadline, "Actor startup publication")
            try:
                publish = (
                    self._candidate is candidate
                    and self._candidate_admitted
                    and self._candidate_registration is registration
                    and pool_active
                    and not self._pool_runtime_retired
                    and self._epoch == observed_epoch
                    and not self._closing
                    and not self._close_failed
                    and self._runtime is None
                    and candidate.state is RuntimeState.RUNNING
                    and not candidate.stop_requested
                )
                if publish:
                    self._epoch = candidate_epoch
                    self._runtime = candidate
                    self._candidate = None
                    self._candidate_admitted = False
                    self._candidate_registration = None
                    self._push_buffer = push_buffer
                closing = self._closing
                self._starting = False
                self._lifecycle.notify_all()
            finally:
                self._lifecycle.release()
            if not publish:
                request_actor_stop(candidate, deadline=deadline)
                if closing:
                    raise ConnectionClosedError("7709 transport closed during Actor startup")
                remaining = 1.0 if deadline is None else max(0.0, deadline - time.monotonic())
                close_actor(candidate, timeout=remaining)
                self._discard_candidate(candidate, deadline=deadline)
                raise ConnectionClosedError("7709 transport changed while resolving endpoints")
            return candidate
        except ActorStartupError as exc:
            candidate = exc.runtime
            self._abort_runtime_start(
                candidate,
                registration,
                deadline=deadline,
                observed_epoch=observed_epoch,
                observed_close_generation=invocation_close_generation,
            )
            raise
        except BaseException:
            self._abort_runtime_start(
                candidate,
                registration,
                deadline=deadline,
                observed_epoch=observed_epoch,
                observed_close_generation=invocation_close_generation,
            )
            raise

    def _own_candidate(
        self,
        runtime: ActorRuntime,
        registration: Any,
        *,
        deadline: float | None = None,
    ) -> None:
        unpublished = self._unpublished_candidate
        if unpublished is not None and unpublished is not runtime:
            raise RuntimeError("7709 transport already owns another unpublished Actor candidate")
        self._unpublished_candidate = runtime
        self._acquire_lifecycle_for_request(deadline, "Actor candidate ownership")
        try:
            if self._candidate is not None and self._candidate is not runtime:
                raise RuntimeError("7709 transport already owns another Actor candidate")
            self._candidate = runtime
            if self._unpublished_candidate is runtime:
                self._unpublished_candidate = None
            self._candidate_admitted = False
            self._candidate_registration = registration
            if self._finalizer is None:
                self._finalizer = weakref.finalize(self, abandon_actor, runtime)
            self._lifecycle.notify_all()
        finally:
            self._lifecycle.release()

        accepted = registration is None
        try:
            if registration is not None:
                register = getattr(registration, "register", None)
                result = registration(runtime) if register is None else register(runtime, deadline=deadline)
                accepted = result is not False
        except BaseException:
            request_actor_stop(runtime, deadline=deadline)
            raise

        self._acquire_lifecycle_for_request(deadline, "Actor candidate admission")
        try:
            current = self._candidate is runtime and self._candidate_registration is registration
            pool_identity_valid = self._fixed_runtime_epoch is None or registration is self._runtime_started_callback
            accepted = (
                accepted
                and current
                and pool_identity_valid
                and not self._pool_runtime_retired
                and not self._closing
                and not self._close_failed
            )
            if current:
                self._candidate_admitted = accepted
            self._lifecycle.notify_all()
        finally:
            self._lifecycle.release()
        if not accepted:
            request_actor_stop(runtime, deadline=deadline)

    def _abort_runtime_start(
        self,
        candidate: ActorRuntime | None,
        registration: Any,
        *,
        deadline: float | None,
        observed_epoch: int,
        observed_close_generation: int,
    ) -> None:
        if candidate is not None:
            try:
                request_actor_stop(candidate, deadline=deadline)
            except BaseException:
                pass
        try:
            self._acquire_lifecycle_for_request(deadline, "Actor startup cleanup")
        except ResponseTimeoutError:
            self._close_failed = True
            self._startup_aborted.set()
            return
        try:
            owns_unpublished = self._unpublished_candidate is candidate
            same_invocation = (
                self._epoch == observed_epoch
                and self._close_generation == observed_close_generation
            )
            if (
                candidate is not None
                and self._candidate is None
                and self._runtime is None
                and (owns_unpublished or same_invocation)
            ):
                self._candidate = candidate
                if self._unpublished_candidate is candidate:
                    self._unpublished_candidate = None
                self._candidate_admitted = False
                self._candidate_registration = registration
                if self._finalizer is None:
                    self._finalizer = weakref.finalize(self, abandon_actor, candidate)
            self._starting = False
            self._startup_aborted.clear()
            self._lifecycle.notify_all()
        finally:
            self._lifecycle.release()

    def _discard_candidate(self, candidate: ActorRuntime, *, deadline: float | None = None) -> None:
        self._acquire_lifecycle_for_request(deadline, "Actor candidate cleanup")
        try:
            if self._candidate is not candidate and self._unpublished_candidate is not candidate:
                return
            failed = self._close_failed or candidate.state in (
                RuntimeState.FAILED,
                RuntimeState.FAILED_CLOSING,
                RuntimeState.FAILED_CLOSED,
            )
            if failed and self._runtime is None:
                if candidate.state is RuntimeState.STOPPED:
                    candidate.state = RuntimeState.FAILED_CLOSED
                self._runtime = candidate
            self._candidate = None
            if self._unpublished_candidate is candidate:
                self._unpublished_candidate = None
            self._candidate_admitted = False
            self._candidate_registration = None
            if not failed and self._runtime is None and self._finalizer is not None:
                self._finalizer.detach()
                self._finalizer = None
            self._lifecycle.notify_all()
        finally:
            self._lifecycle.release()

    def _pool_runtime_is_active(
        self,
        expected_runtime_epoch: int | None = None,
        *,
        deadline: float | None = None,
    ) -> bool:
        if deadline is None:
            self._lifecycle.acquire()
            acquired = True
        else:
            acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise ResponseTimeoutError("7709 response timed out checking pool runtime")
        try:
            fixed_epoch = self._fixed_runtime_epoch
            retired = self._pool_runtime_retired
            registration = self._runtime_started_callback
        finally:
            self._lifecycle.release()
        return _pool_registration_is_active(
            fixed_epoch=fixed_epoch,
            retired=retired,
            registration=registration,
            expected_runtime_epoch=expected_runtime_epoch,
            deadline=deadline,
        )

    def _configure_pool_runtime(
        self,
        *,
        push_buffer: PushBuffer,
        runtime_epoch: int,
        endpoints: tuple[ResolvedEndpoint, ...],
        actor_fatal_callback: Any = None,
        runtime_started_callback: Any = None,
        heartbeat_allowed: Any = None,
        successor_grace: float = 0.0,
        terminal_yield: bool = False,
        deadline: float | None = None,
    ) -> None:
        self._acquire_lifecycle_for_request(deadline, "pool slot configuration")
        try:
            if (
                self._runtime is not None
                or self._candidate is not None
                or self._unpublished_candidate is not None
                or self._starting
                or self._close_failed
            ):
                raise RuntimeError("cannot reconfigure a running socket transport")
            self._shared_push_buffer = push_buffer
            self._fixed_runtime_epoch = runtime_epoch
            self._resolved_endpoints = endpoints
            self._owns_push_buffer = False
            self._push_buffer = push_buffer
            self._actor_fatal_callback = actor_fatal_callback
            self._runtime_started_callback = runtime_started_callback
            self._heartbeat_allowed = heartbeat_allowed
            self._successor_grace = max(0.0, successor_grace)
            self._terminal_yield = terminal_yield
            self._pool_runtime_retired = False
        finally:
            self._lifecycle.release()

    def _retire_pool_runtime(self, registration: Any, *, deadline: float | None = None) -> bool:
        submission_token = object()
        try:
            acquired = _acquire_gate_token(self._submission_gate, submission_token, deadline)
        except BaseException:
            _release_gate_token(self._submission_gate, submission_token)
            raise
        if not acquired:
            raise TransportCloseTimeoutError("7709 pool slot submission did not finish before close deadline")
        try:
            if deadline is None:
                self._lifecycle.acquire()
                lifecycle_acquired = True
            else:
                lifecycle_acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
            if not lifecycle_acquired:
                raise TransportCloseTimeoutError("7709 pool slot lifecycle blocked retirement before deadline")
            try:
                if registration is not self._runtime_started_callback:
                    return False
                self._pool_runtime_retired = True
                self._lifecycle.notify_all()
                return True
            finally:
                self._lifecycle.release()
        finally:
            _release_gate_token(self._submission_gate, submission_token)

    def _clear_pool_runtime(
        self,
        *,
        registration: Any,
        runtime_epoch: int,
        push_buffer: PushBuffer,
        deadline: float | None = None,
    ) -> bool:
        submission_token = object()
        try:
            acquired = _acquire_gate_token(self._submission_gate, submission_token, deadline)
        except BaseException:
            _release_gate_token(self._submission_gate, submission_token)
            raise
        if not acquired:
            raise TransportCloseTimeoutError("7709 pool slot submission blocked cleanup before deadline")
        try:
            if deadline is None:
                self._lifecycle.acquire()
                lifecycle_acquired = True
            else:
                lifecycle_acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
            if not lifecycle_acquired:
                raise TransportCloseTimeoutError("7709 pool slot lifecycle blocked cleanup before deadline")
            try:
                unconfigured = (
                    self._runtime_started_callback is None
                    and self._shared_push_buffer is None
                    and self._fixed_runtime_epoch is None
                    and self._runtime is None
                    and self._candidate is None
                    and self._unpublished_candidate is None
                )
                if registration is not self._runtime_started_callback and unconfigured:
                    return True
                if (
                    registration is not self._runtime_started_callback
                    or runtime_epoch != self._fixed_runtime_epoch
                    or push_buffer is not self._shared_push_buffer
                    or self._runtime is not None
                    or self._candidate is not None
                    or self._unpublished_candidate is not None
                    or self._starting
                    or self._closing
                ):
                    return False
                self._shared_push_buffer = None
                self._fixed_runtime_epoch = None
                self._resolved_endpoints = None
                self._owns_push_buffer = True
                self._actor_fatal_callback = None
                self._runtime_started_callback = None
                self._heartbeat_allowed = None
                self._successor_grace = 0.0
                self._terminal_yield = False
                self._pool_runtime_retired = False
                self._push_buffer = None
                return True
            finally:
                self._lifecycle.release()
        finally:
            _release_gate_token(self._submission_gate, submission_token)

    def _request_stop(self, *, deadline: float | None = None) -> None:
        if deadline is None:
            self._lifecycle.acquire()
            acquired = True
        else:
            acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 transport lifecycle blocked stop before deadline")
        try:
            runtime = self._runtime
            candidate = self._candidate or self._unpublished_candidate
        finally:
            self._lifecycle.release()
        errors: list[BaseException] = []
        for item in (runtime, candidate):
            if item is not None:
                try:
                    request_actor_stop(item, deadline=deadline)
                except BaseException as exc:
                    errors.append(exc)
        if errors:
            raise errors[0]

    def _cancel_lease(self, lease_id: int, *, deadline: float | None = None) -> None:
        if deadline is None:
            self._lifecycle.acquire()
            acquired = True
        else:
            acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise TransportCloseTimeoutError("7709 transport lifecycle blocked cancel before deadline")
        try:
            runtime = self._runtime
        finally:
            self._lifecycle.release()
        if runtime is None:
            return
        control_token = _acquire_control_lock(runtime, deadline, "cancel lease")
        try:
            active = runtime.active_task
            pending = runtime.pending_task
            if isinstance(active, (ConnectTicket, RequestTicket)) and active.lease_id == lease_id:
                target = active
            elif isinstance(pending, (ConnectTicket, RequestTicket)) and pending.lease_id == lease_id:
                target = pending
            else:
                target = None
        finally:
            runtime.control_lock.release_token(control_token)
        if target is not None:
            cancel_ticket(runtime, target, deadline=deadline)

    def _require_current_runtime(
        self,
        runtime: ActorRuntime,
        *,
        expected_runtime_epoch: int | None = None,
        deadline: float | None = None,
    ) -> None:
        if deadline is None:
            self._lifecycle.acquire()
            acquired = True
        else:
            acquired = self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic()))
        if not acquired:
            raise ResponseTimeoutError("7709 response timed out validating transport runtime")
        try:
            invalid = (
                self._runtime is not runtime
                or self._closing
                or self._close_failed
                or self._pool_runtime_retired
                or (expected_runtime_epoch is not None and runtime.runtime_epoch != expected_runtime_epoch)
            )
            fixed_epoch = self._fixed_runtime_epoch
            retired = self._pool_runtime_retired
            registration = self._runtime_started_callback
        finally:
            self._lifecycle.release()
        if invalid or not _pool_registration_is_active(
            fixed_epoch=fixed_epoch,
            retired=retired,
            registration=registration,
            expected_runtime_epoch=expected_runtime_epoch,
            deadline=deadline,
        ):
            raise ConnectionClosedError("7709 transport runtime changed")

    def _acquire_request_lock(
        self,
        deadline: float,
        *,
        ownership: _RequestLockOwnership | None = None,
    ) -> bool:
        if ownership is None:
            ownership = _RequestLockOwnership()
        acquired = self._request_lock.acquire_token(ownership.token, deadline)
        if acquired and ownership is not None:
            ownership.acquired = True
        return acquired

    def _acquire_lifecycle_for_request(self, deadline: float | None, stage: str) -> None:
        if deadline is None:
            self._lifecycle.acquire()
            return
        if self._lifecycle.acquire(timeout=max(0.0, deadline - time.monotonic())):
            return
        raise ResponseTimeoutError(f"7709 response timed out during {stage}")


def _unique_runtimes(*items: ActorRuntime | None) -> tuple[ActorRuntime, ...]:
    unique: list[ActorRuntime] = []
    for item in items:
        if item is not None and all(existing is not item for existing in unique):
            unique.append(item)
    return tuple(unique)


def _pool_registration_is_active(
    *,
    fixed_epoch: int | None,
    retired: bool,
    registration: Any,
    expected_runtime_epoch: int | None,
    deadline: float | None,
) -> bool:
    if expected_runtime_epoch is not None and fixed_epoch != expected_runtime_epoch:
        return False
    if fixed_epoch is None:
        return True
    if retired or registration is None:
        return False
    is_active = getattr(registration, "is_active", None)
    return True if is_active is None else bool(is_active(deadline=deadline))


def _acquire_gate_token(gate: Any, token: object, deadline: float | None) -> bool:
    acquire_token = getattr(gate, "acquire_token", None)
    if acquire_token is not None:
        return bool(acquire_token(token, deadline))
    if deadline is None:
        return bool(gate.acquire())
    return bool(gate.acquire(timeout=max(0.0, deadline - time.monotonic())))


def _release_gate_token(gate: Any, token: object) -> bool:
    release_token = getattr(gate, "release_token", None)
    if release_token is not None:
        return bool(release_token(token))
    try:
        gate.release()
    except RuntimeError:
        return False
    return True


def _request_actor_stop_all(
    items: tuple[ActorRuntime, ...],
    *,
    deadline: float | None = None,
) -> BaseException | None:
    first_error: BaseException | None = None
    for item in items:
        try:
            request_actor_stop(item, deadline=deadline)
        except BaseException as exc:
            if first_error is None:
                first_error = exc
    return first_error


def _unique_owned_push_buffers(
    facade_push_buffer: PushBuffer | None,
    runtimes: tuple[ActorRuntime, ...],
) -> tuple[PushBuffer, ...]:
    unique: list[PushBuffer] = []
    candidates = [facade_push_buffer]
    candidates.extend(
        runtime.push_buffer if runtime.owns_push_buffer else None
        for runtime in runtimes
    )
    for candidate in candidates:
        if candidate is not None and all(existing is not candidate for existing in unique):
            unique.append(candidate)
    return tuple(unique)


def _clear_resolved_push_cleanup(
    runtime: ActorRuntime,
    push_buffer: PushBuffer,
    *,
    deadline: float | None = None,
) -> None:
    control_token = _acquire_control_lock(runtime, deadline, "push cleanup")
    try:
        if runtime.push_buffer is not push_buffer or runtime.push_cleanup_error is None:
            return
        thread = runtime.actor_thread
        cleanup_complete = (
            runtime.stopped.is_set()
            and (thread is None or not thread.is_alive())
            and runtime.generation is None
            and runtime.selector is None
            and runtime.wake_reader is None
            and runtime.wake_writer is None
            and runtime.pending_task is None
            and runtime.active_task is None
            and not runtime.cancel_requests
        )
        if not cleanup_complete:
            return
        if runtime.cleanup_error is runtime.push_cleanup_error:
            runtime.cleanup_error = runtime.deferred_cleanup_error
        runtime.push_cleanup_error = None
    finally:
        runtime.control_lock.release_token(control_token)


def _mark_failed_closing(
    items: tuple[ActorRuntime, ...],
    *,
    deadline: float | None = None,
) -> None:
    first_error: BaseException | None = None
    for runtime in items:
        try:
            control_token = _acquire_control_lock(runtime, deadline, "failed-close publication")
            try:
                if runtime.state is not RuntimeState.FAILED_CLOSED:
                    runtime.state = RuntimeState.FAILED_CLOSING
            finally:
                runtime.control_lock.release_token(control_token)
        except BaseException as exc:
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error


def _mark_failed_closed(
    items: tuple[ActorRuntime, ...],
    *,
    deadline: float | None = None,
) -> None:
    first_error: BaseException | None = None
    for runtime in items:
        try:
            control_token = _acquire_control_lock(runtime, deadline, "failed-close publication")
        except TransportCloseTimeoutError:
            if first_error is None:
                first_error = TransportCloseTimeoutError(
                    "7709 Actor control lock blocked failed-close publication"
                )
            continue
        try:
            thread = runtime.actor_thread
            if thread is None or not thread.is_alive():
                runtime.state = RuntimeState.FAILED_CLOSED
        finally:
            runtime.control_lock.release_token(control_token)
    if first_error is not None:
        raise first_error


def _retry_safe(command: int) -> bool:
    for spec in COMMANDS.values():
        if spec.code == command:
            return spec.retry_safe
    return False


def _requires_dns(hosts: Sequence[str]) -> bool:
    for host in hosts:
        address = host.rsplit(":", 1)[0].removeprefix("[").removesuffix("]")
        try:
            ipaddress.ip_address(address)
        except ValueError:
            return True
    return False


def _parse_push(frame: PushFrame) -> Any:
    return parse_command_response(frame.response.msg_type, frame.response, {})
