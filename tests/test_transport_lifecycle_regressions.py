from __future__ import annotations

import gc
import selectors
import socket
import threading
import time
import weakref

import pytest

from actor_support import Scripted7709Server, handshake_payload, read_request, response_bytes
from eltdx.exceptions import ConnectionClosedError, ResponseTimeoutError, TransportCloseTimeoutError
from eltdx.protocol.constants import TYPE_SECURITY_COUNT
from eltdx.transport import actor as actor_module
from eltdx.transport import pool as pool_module
from eltdx.transport import socket as socket_module
from eltdx.transport.actor import ActorRuntime, ActorStartupError, RuntimeState
from eltdx.transport.pool import (
    LeaseBroker,
    PoolRuntimeGuard,
    PoolState,
    PooledSocketTransport,
    RuntimeRegistration,
)
from eltdx.transport.push import PushBuffer
from eltdx.transport.socket import SocketTransport


class FatalSelector:
    def __init__(self, trigger: threading.Event) -> None:
        self._trigger = trigger

    def register(self, *_args, **_kwargs) -> None:
        return None

    def select(self, _timeout=None):
        self._trigger.wait()
        raise RuntimeError("fatal selector injection")

    def unregister(self, *_args, **_kwargs) -> None:
        return None

    def close(self) -> None:
        return None


@pytest.mark.parametrize("blocked_owner", ("guard", "broker", "push", "sibling"))
def test_failed_actor_exits_without_waiting_for_pool_owned_locks(blocked_owner: str) -> None:
    broker = LeaseBroker(1, pool_size=2, max_pending_requests=2)
    push = PushBuffer(1)
    guard = PoolRuntimeGuard()
    sibling = ActorRuntime(1, ())
    held = threading.Event()
    release = threading.Event()
    trigger = threading.Event()
    retire = threading.Event()
    guard.configure(broker, push, retire_event=retire)
    assert guard.add_runtime(sibling, pool_epoch=1, broker=broker)
    registration = RuntimeRegistration(weakref.ref(guard), 1, weakref.ref(broker), retire)
    handle = pool_module.ActorFatalHandle(weakref.ref(guard), 1, weakref.ref(broker), retire)
    runtime = actor_module.start_actor(
        1,
        (),
        selector_factory=lambda: FatalSelector(trigger),
        fatal_callback=handle,
        candidate_callback=registration,
    )

    def hold_lock() -> None:
        if blocked_owner == "guard":
            manager = guard._lock
        elif blocked_owner == "broker":
            manager = broker._condition
        elif blocked_owner == "push":
            manager = push._condition
        else:
            manager = sibling.control_lock
        with manager:
            held.set()
            release.wait()

    holder = threading.Thread(target=hold_lock)
    holder.start()
    assert held.wait(timeout=2)
    try:
        trigger.set()
        assert runtime.stopped.wait(timeout=0.2)
        assert runtime.actor_thread is not None and not runtime.actor_thread.is_alive()
        assert retire.is_set()
    finally:
        release.set()
        trigger.set()
        holder.join(timeout=2)
        runtime.stopped.wait(timeout=2)


def test_runtime_registered_after_pool_fatal_is_stopped_immediately(monkeypatch) -> None:
    stopped = []
    monkeypatch.setattr(pool_module, "request_actor_stop", stopped.append)
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    push = PushBuffer(1)
    guard = PoolRuntimeGuard()
    guard.configure(broker, push)
    failed = ActorRuntime(1, ())
    late = ActorRuntime(1, ())

    guard.fail(failed, RuntimeError("fatal"), pool_epoch=1, broker=broker)
    guard.add_runtime(late, pool_epoch=1, broker=broker)

    assert failed in stopped
    assert late in stopped


def test_stale_epoch_runtime_and_fatal_cannot_poison_new_pool(monkeypatch) -> None:
    stopped = []
    monkeypatch.setattr(pool_module, "request_actor_stop", stopped.append)
    guard = PoolRuntimeGuard()
    old_broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    old_push = PushBuffer(1)
    guard.configure(old_broker, old_push)
    new_broker = LeaseBroker(2, pool_size=1, max_pending_requests=1)
    new_push = PushBuffer(2)
    guard.configure(new_broker, new_push)
    stale = ActorRuntime(1, ())

    guard.add_runtime(stale, pool_epoch=1, broker=old_broker)
    guard.fail(stale, RuntimeError("stale fatal"), pool_epoch=1, broker=old_broker)

    assert stale in stopped
    assert guard.failure() is None
    assert not new_broker.snapshot().closed
    assert not new_push.snapshot().closed


def test_close_cannot_return_while_unpublished_candidate_is_alive(monkeypatch) -> None:
    candidate_started = threading.Event()
    allow_start_return = threading.Event()
    cleanup_entered = threading.Event()
    allow_cleanup = threading.Event()
    close_done = threading.Event()
    candidate = ActorRuntime(1, ())
    results: list[object] = []

    def fake_start_actor(*args, **kwargs):
        kwargs["candidate_callback"](candidate)
        candidate_started.set()
        assert allow_start_return.wait(timeout=2)
        return candidate

    def fake_close_actor(observed, timeout=1.0) -> None:
        assert observed is candidate
        cleanup_entered.set()
        assert allow_cleanup.wait(timeout=2)

    monkeypatch.setattr(socket_module, "start_actor", fake_start_actor)
    monkeypatch.setattr(socket_module, "close_actor", fake_close_actor)
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)

    def start() -> None:
        try:
            results.append(transport._ensure_runtime())
        except BaseException as exc:
            results.append(exc)

    def close() -> None:
        try:
            transport.close()
        except BaseException as exc:
            results.append(exc)
        finally:
            close_done.set()

    starter = threading.Thread(target=start)
    closer = threading.Thread(target=close)
    starter.start()
    assert candidate_started.wait(timeout=2)
    closer.start()
    with transport._lifecycle:
        assert transport._lifecycle.wait_for(lambda: transport._closing, timeout=2)
    allow_start_return.set()
    assert cleanup_entered.wait(timeout=2)
    returned_before_cleanup = close_done.wait(timeout=0.05)
    allow_cleanup.set()
    starter.join(timeout=2)
    closer.join(timeout=2)

    assert not starter.is_alive() and not closer.is_alive()
    assert not returned_before_cleanup
    assert any(isinstance(item, ConnectionClosedError) for item in results)


def test_concurrent_pool_close_cannot_overwrite_failed_closing(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    pool._ensure_started()
    first_entered = threading.Event()
    allow_first_failure = threading.Event()
    results: list[object] = []
    close_calls = 0
    close_lock = threading.Lock()

    def fake_close(timeout: float) -> None:
        nonlocal close_calls
        with close_lock:
            close_calls += 1
            call = close_calls
        if call == 1:
            first_entered.set()
            assert allow_first_failure.wait(timeout=2)
            raise TransportCloseTimeoutError("first close timed out")

    monkeypatch.setattr(pool._transports[0], "_close_with_timeout", fake_close)

    def run_close() -> None:
        try:
            pool.close()
        except BaseException as exc:
            results.append(exc)
        else:
            results.append("closed")

    first = threading.Thread(target=run_close, name="first-pool-close")
    second = threading.Thread(target=run_close, name="second-pool-close")
    first.start()
    assert first_entered.wait(timeout=2)
    second.start()
    allow_first_failure.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive() and not second.is_alive()
    assert close_calls == 1
    assert pool._state is PoolState.FAILED_CLOSING
    assert len(results) == 2
    assert all(isinstance(item, TransportCloseTimeoutError) for item in results)
    assert {str(item) for item in results} == {"first close timed out"}

    pool.close()

    assert close_calls == 2
    assert pool._state is PoolState.FAILED_CLOSED


def test_pool_connect_failure_stops_siblings_before_waiting_for_completion(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=2, timeout=1, heartbeat_interval=None)
    slow_started = threading.Event()
    failure_raised = threading.Event()
    stop_seen = threading.Event()
    release_slow = threading.Event()
    result: list[object] = []

    def slow_connect() -> None:
        slow_started.set()
        assert release_slow.wait(timeout=2)

    def failed_connect() -> None:
        assert slow_started.wait(timeout=2)
        failure_raised.set()
        raise ConnectionClosedError("slot failed")

    monkeypatch.setattr(pool._transports[0], "_connect_with_deadline", lambda **kwargs: slow_connect())
    monkeypatch.setattr(pool._transports[1], "_connect_with_deadline", lambda **kwargs: failed_connect())
    for transport in pool._transports:
        monkeypatch.setattr(
            transport,
            "_request_stop",
            lambda **_kwargs: (stop_seen.set(), release_slow.set()),
        )
        monkeypatch.setattr(transport, "_close_with_timeout", lambda timeout: None)

    def connect() -> None:
        try:
            pool.connect()
        except BaseException as exc:
            result.append(exc)

    thread = threading.Thread(target=connect)
    thread.start()
    assert failure_raised.wait(timeout=2)
    stopped_before_fallback = stop_seen.wait(timeout=0.05)
    release_slow.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert stopped_before_fallback
    assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)


def test_pool_close_submission_gate_wait_obeys_one_second_deadline() -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    pool._ensure_started()
    gate = pool._transports[0]._submission_gate
    assert gate.acquire(timeout=1)
    done = threading.Event()
    results: list[object] = []

    def close() -> None:
        try:
            results.append(pool.close())
        except BaseException as exc:
            results.append(exc)
        finally:
            done.set()

    thread = threading.Thread(target=close)
    started = time.monotonic()
    thread.start()
    try:
        assert done.wait(timeout=1.25)
        assert time.monotonic() - started < 1.25
        assert len(results) == 1 and isinstance(results[0], TransportCloseTimeoutError)
        assert pool._state is PoolState.FAILED_CLOSING
    finally:
        gate.release()
        done.wait(timeout=2)
        thread.join(timeout=2)
        try:
            pool.close()
        except TransportCloseTimeoutError:
            pass

    assert not thread.is_alive()
    assert pool._state is PoolState.FAILED_CLOSED


def test_standalone_close_control_lock_wait_obeys_one_second_deadline() -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    runtime = transport._ensure_runtime(time.monotonic() + 1)
    done = threading.Event()
    results: list[object] = []
    runtime.control_lock.acquire()

    def close() -> None:
        try:
            results.append(transport.close())
        except BaseException as exc:
            results.append(exc)
        finally:
            done.set()

    thread = threading.Thread(target=close)
    started = time.monotonic()
    thread.start()
    try:
        assert done.wait(timeout=1.25)
        assert time.monotonic() - started < 1.25
        assert len(results) == 1 and isinstance(results[0], TransportCloseTimeoutError)
        assert transport._close_failed
        assert transport._runtime is runtime
    finally:
        runtime.control_lock.release()
        thread.join(timeout=2)

    transport.close()
    assert runtime.actor_thread is not None and not runtime.actor_thread.is_alive()
    assert runtime.state is RuntimeState.FAILED_CLOSED


def test_connect_rollback_stops_slots_before_waiting_for_submission_gate(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=2, timeout=1, heartbeat_interval=None)
    pool._ensure_started()
    held_gate = pool._transports[0]._submission_gate
    assert held_gate.acquire(timeout=1)
    slow_started = threading.Event()
    failure_raised = threading.Event()
    stop_seen = threading.Event()
    release_slow = threading.Event()
    results: list[object] = []

    def slow_connect(**_kwargs) -> None:
        slow_started.set()
        assert release_slow.wait(timeout=2)

    def failed_connect(**_kwargs) -> None:
        assert slow_started.wait(timeout=2)
        failure_raised.set()
        raise ConnectionClosedError("slot failed")

    monkeypatch.setattr(pool._transports[0], "_connect_with_deadline", slow_connect)
    monkeypatch.setattr(pool._transports[1], "_connect_with_deadline", failed_connect)
    for transport in pool._transports:
        monkeypatch.setattr(transport, "_request_stop", lambda **_kwargs: (stop_seen.set(), release_slow.set()))
        monkeypatch.setattr(transport, "_close_with_timeout", lambda _timeout: None)

    def connect() -> None:
        try:
            pool.connect()
        except BaseException as exc:
            results.append(exc)

    thread = threading.Thread(target=connect)
    thread.start()
    try:
        assert failure_raised.wait(timeout=2)
        assert stop_seen.wait(timeout=0.2)
    finally:
        held_gate.release()
        release_slow.set()
        thread.join(timeout=2)
        if pool._state is not PoolState.STOPPED:
            try:
                pool.close()
            except TransportCloseTimeoutError:
                pass

    assert not thread.is_alive()
    assert len(results) == 1 and isinstance(results[0], ConnectionClosedError)


def test_actor_startup_timeout_remains_owned_until_failed_closed(monkeypatch) -> None:
    selector_entered = threading.Event()
    release_selector = threading.Event()
    real_start_actor = actor_module.start_actor

    def blocking_selector_factory():
        selector_entered.set()
        assert release_selector.wait(timeout=2)
        return selectors.DefaultSelector()

    def start_with_blocked_selector(*args, **kwargs):
        kwargs["selector_factory"] = blocking_selector_factory
        return real_start_actor(*args, **kwargs)

    monkeypatch.setattr(socket_module, "start_actor", start_with_blocked_selector)
    transport = SocketTransport(["127.0.0.1:9"], timeout=0.05, heartbeat_interval=None)
    candidate: ActorRuntime | None = None
    try:
        with pytest.raises(ActorStartupError):
            transport._ensure_runtime(time.monotonic() + 0.05)
        assert selector_entered.is_set()
        candidate = transport._candidate
        assert candidate is not None
        assert candidate.actor_thread is not None and candidate.actor_thread.is_alive()

        with pytest.raises(TransportCloseTimeoutError):
            transport._close_with_timeout(0.02)
        assert transport._candidate is candidate
        assert candidate.state is RuntimeState.FAILED_CLOSING

        release_selector.set()
        assert candidate.stopped.wait(timeout=2)
        transport._close_with_timeout(1)

        assert transport._candidate is None
        assert transport._runtime is candidate
        assert candidate.state is RuntimeState.FAILED_CLOSED
        assert candidate.actor_thread is not None and not candidate.actor_thread.is_alive()
        assert candidate.selector is None
        assert candidate.wake_reader is None and candidate.wake_writer is None
        with pytest.raises(ConnectionClosedError):
            transport._ensure_runtime(time.monotonic() + 0.1)
    finally:
        release_selector.set()
        if candidate is not None and candidate.actor_thread is not None:
            candidate.actor_thread.join(timeout=2)


def test_pool_connect_stops_real_candidate_blocked_during_startup(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=2, timeout=0.25, heartbeat_interval=None)
    slow = pool._transports[0]
    failed = pool._transports[1]
    selector_entered = threading.Event()
    release_selector = threading.Event()
    candidate_stopped = threading.Event()
    result: list[BaseException] = []
    local = threading.local()
    real_start_actor = actor_module.start_actor
    real_request_stop = socket_module.request_actor_stop
    real_slow_connect = slow._connect_with_deadline

    def blocking_selector_factory():
        selector_entered.set()
        assert release_selector.wait(timeout=2)
        return selectors.DefaultSelector()

    def dispatch_start(*args, **kwargs):
        if getattr(local, "slow_slot", False):
            kwargs["selector_factory"] = blocking_selector_factory
        return real_start_actor(*args, **kwargs)

    def slow_connect(**kwargs) -> None:
        local.slow_slot = True
        try:
            real_slow_connect(**kwargs)
        finally:
            local.slow_slot = False

    def failed_connect(**kwargs) -> None:
        assert selector_entered.wait(timeout=2)
        raise ConnectionClosedError("slot failed")

    def observing_stop(runtime: ActorRuntime, **kwargs) -> None:
        assert runtime is slow._candidate
        real_request_stop(runtime, **kwargs)
        assert runtime.stop_requested
        candidate_stopped.set()
        release_selector.set()

    monkeypatch.setattr(socket_module, "start_actor", dispatch_start)
    monkeypatch.setattr(socket_module, "request_actor_stop", observing_stop)
    monkeypatch.setattr(slow, "_connect_with_deadline", slow_connect)
    monkeypatch.setattr(failed, "_connect_with_deadline", failed_connect)
    try:
        with pytest.raises(ConnectionClosedError, match="slot failed"):
            pool.connect()
        assert candidate_stopped.is_set()
        assert pool._state is PoolState.STOPPED
        assert slow._candidate is None and slow._runtime is None
        assert not any(
            thread.name.startswith("eltdx-7709-actor-") and thread.is_alive()
            for thread in threading.enumerate()
        )
    except BaseException as exc:
        result.append(exc)
        raise
    finally:
        release_selector.set()
        try:
            pool.close()
        except BaseException:
            if not result:
                raise


def test_admitted_execute_cannot_start_actor_after_pool_shutdown(monkeypatch) -> None:
    release_server = threading.Event()

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, (321).to_bytes(2, "little")))
        assert release_server.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        pool = PooledSocketTransport([server.host], pool_size=1, timeout=0.5, heartbeat_interval=None)
        pool._ensure_started()
        slot = pool._transports[0]
        entered_slot = threading.Event()
        release_slot = threading.Event()
        actor_started = threading.Event()
        original_execute = slot._execute_with_lease
        real_start_actor = socket_module.start_actor
        result: list[object] = []

        def paused_execute(*args, **kwargs):
            entered_slot.set()
            assert release_slot.wait(timeout=2)
            return original_execute(*args, **kwargs)

        def observed_start(*args, **kwargs):
            actor_started.set()
            return real_start_actor(*args, **kwargs)

        monkeypatch.setattr(slot, "_execute_with_lease", paused_execute)
        monkeypatch.setattr(socket_module, "start_actor", observed_start)

        def execute() -> None:
            try:
                result.append(pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"}))
            except BaseException as exc:
                result.append(exc)

        thread = threading.Thread(target=execute)
        thread.start()
        assert entered_slot.wait(timeout=2)
        pool.close()
        release_slot.set()
        thread.join(timeout=2)
        release_server.set()
        try:
            slot.close()
        except BaseException:
            pass

        assert not thread.is_alive()
        assert not actor_started.is_set()
        assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)
        assert slot._runtime is None and slot._candidate is None


def test_fatal_during_pool_join_cannot_be_published_as_stopped(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    pool._ensure_started()
    broker = pool._broker
    assert broker is not None
    fatal = RuntimeError("fatal during join")
    late = ActorRuntime(broker.pool_epoch, ())

    def close_with_fatal(timeout: float) -> None:
        pool._runtime_guard.fail(late, fatal, pool_epoch=broker.pool_epoch, broker=broker)

    monkeypatch.setattr(pool._transports[0], "_close_with_timeout", close_with_fatal)
    pool.close()

    assert pool._state is PoolState.FAILED_CLOSED
    assert pool._runtime_guard.failure() is fatal
    assert pool._broker is broker
    with pytest.raises(ConnectionClosedError, match="FAILED_CLOSED"):
        pool._ensure_started()


def test_dead_old_broker_identity_cannot_register_into_same_epoch(monkeypatch) -> None:
    stopped: list[ActorRuntime] = []
    monkeypatch.setattr(pool_module, "request_actor_stop", stopped.append)
    guard = PoolRuntimeGuard()
    old_broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    guard.configure(old_broker, PushBuffer(1))
    registration = RuntimeRegistration(weakref.ref(guard), 1, weakref.ref(old_broker))
    guard.seal(pool_epoch=1, broker=old_broker)
    guard.finish_epoch(pool_epoch=1, broker=old_broker)
    old_ref = weakref.ref(old_broker)
    del old_broker
    gc.collect()
    assert old_ref() is None

    guard.configure(LeaseBroker(1, pool_size=1, max_pending_requests=1), PushBuffer(1))
    stale = ActorRuntime(1, ())
    registration(stale)

    assert stopped == [stale]


def test_guard_abandon_rejects_registration_after_snapshot(monkeypatch) -> None:
    stopped: list[ActorRuntime] = []
    monkeypatch.setattr(pool_module, "request_actor_stop", stopped.append)
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    guard = PoolRuntimeGuard()
    guard.configure(broker, PushBuffer(1))
    guard.abandon()
    late = ActorRuntime(1, ())

    guard.add_runtime(late, pool_epoch=1, broker=broker)

    assert stopped == [late]


def test_guard_abandon_cannot_miss_runtime_appended_after_snapshot(monkeypatch) -> None:
    append_entered = threading.Event()
    allow_append = threading.Event()
    stopped: list[ActorRuntime] = []

    class BlockingRuntimeList(list[ActorRuntime]):
        def append(self, runtime: ActorRuntime) -> None:
            append_entered.set()
            assert allow_append.wait(timeout=2)
            super().append(runtime)

    def stop(runtime: ActorRuntime) -> None:
        stopped.append(runtime)
        runtime.stop_requested = True

    monkeypatch.setattr(pool_module, "request_actor_stop", stop)
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    guard = PoolRuntimeGuard()
    guard.configure(broker, PushBuffer(1))
    guard._runtimes = BlockingRuntimeList()
    late = ActorRuntime(1, ())
    results: list[bool] = []
    registration = threading.Thread(
        target=lambda: results.append(guard.add_runtime(late, pool_epoch=1, broker=broker))
    )
    registration.start()
    assert append_entered.wait(timeout=2)

    guard.abandon()
    allow_append.set()
    registration.join(timeout=2)

    assert not registration.is_alive()
    assert results == [False]
    assert stopped == [late]
    assert late.stop_requested


def test_normal_pool_close_clears_epoch_configuration_and_reopens() -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=2, timeout=1, heartbeat_interval=None)
    old_broker, old_push = pool._ensure_started()
    expected_grace, expected_yield = pool_module._actor_cooperation(pool.pool_size)
    for slot in pool._transports:
        assert (slot._successor_grace, slot._terminal_yield) == (expected_grace, expected_yield)
    broker_ref = weakref.ref(old_broker)
    push_ref = weakref.ref(old_push)
    pool.close()

    for slot in pool._transports:
        assert slot._shared_push_buffer is None
        assert slot._fixed_runtime_epoch is None
        assert slot._resolved_endpoints is None
        assert slot._actor_fatal_callback is None
        assert slot._runtime_started_callback is None
        assert slot._heartbeat_allowed is None
        assert (slot._successor_grace, slot._terminal_yield) == (0.0, False)

    del old_broker, old_push
    gc.collect()
    assert broker_ref() is None
    assert push_ref() is None

    new_broker, new_push = pool._ensure_started()
    try:
        assert new_broker.pool_epoch > 1
        assert new_push.owner_epoch == new_broker.pool_epoch
        for slot in pool._transports:
            assert (slot._successor_grace, slot._terminal_yield) == (expected_grace, expected_yield)
    finally:
        pool.close()


def test_socket_passes_actor_cooperation_configuration_to_runtime(monkeypatch) -> None:
    captured: list[tuple[float, bool]] = []

    def fake_start_actor(runtime_epoch, endpoints, **kwargs):
        captured.append((kwargs["successor_grace"], kwargs["terminal_yield"]))
        runtime = ActorRuntime(
            runtime_epoch,
            tuple(endpoints),
            successor_grace=kwargs["successor_grace"],
            terminal_yield=kwargs["terminal_yield"],
        )
        kwargs["candidate_callback"](runtime)
        runtime.state = RuntimeState.RUNNING
        return runtime

    def fake_close_actor(runtime, timeout=1.0) -> None:
        del timeout
        runtime.stop_requested = True
        runtime.state = RuntimeState.STOPPED
        runtime.stopped.set()

    monkeypatch.setattr(socket_module, "start_actor", fake_start_actor)
    monkeypatch.setattr(socket_module, "close_actor", fake_close_actor)
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    transport._successor_grace = 0.0005
    transport._terminal_yield = False
    try:
        runtime = transport._ensure_runtime(time.monotonic() + 1)
        assert captured == [(0.0005, False)]
        assert (runtime.successor_grace, runtime.terminal_yield) == (0.0005, False)
    finally:
        transport.close()


def test_shutdown_owner_exception_publishes_failure_and_allows_retry(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    pool._ensure_started()
    slot = pool._transports[0]
    real_request_stop = slot._request_stop
    stop_calls = 0

    def fail_first_stop(**kwargs) -> None:
        nonlocal stop_calls
        stop_calls += 1
        if stop_calls == 1:
            raise RuntimeError("stop injection")
        real_request_stop(**kwargs)

    monkeypatch.setattr(slot, "_request_stop", fail_first_stop)
    with pytest.raises(RuntimeError, match="stop injection"):
        pool.close()

    assert pool._state is PoolState.FAILED_CLOSING
    assert pool._shutdown_active is False

    pool.close()

    assert stop_calls == 2
    assert pool._state is PoolState.FAILED_CLOSED


def test_close_timeout_during_startup_is_permanently_failed_closed(monkeypatch) -> None:
    selector_entered = threading.Event()
    release_selector = threading.Event()
    real_start_actor = actor_module.start_actor
    starts = 0

    def blocking_selector_factory():
        selector_entered.set()
        assert release_selector.wait(timeout=2)
        return selectors.DefaultSelector()

    def blocked_start(*args, **kwargs):
        nonlocal starts
        starts += 1
        kwargs["selector_factory"] = blocking_selector_factory
        return real_start_actor(*args, **kwargs)

    monkeypatch.setattr(socket_module, "start_actor", blocked_start)
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    result: list[object] = []

    def start() -> None:
        try:
            result.append(transport._ensure_runtime(time.monotonic() + 1))
        except BaseException as exc:
            result.append(exc)

    starter = threading.Thread(target=start)
    starter.start()
    assert selector_entered.wait(timeout=2)
    candidate = transport._candidate
    assert candidate is not None
    with pytest.raises(TransportCloseTimeoutError, match="startup did not finish"):
        transport._close_with_timeout(0.02)
    assert transport._close_failed
    assert candidate.state is RuntimeState.FAILED_CLOSING

    release_selector.set()
    starter.join(timeout=2)
    assert not starter.is_alive()
    assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)
    assert transport._runtime is candidate
    assert transport._candidate is None
    assert candidate.state is RuntimeState.FAILED_CLOSED
    with pytest.raises(ConnectionClosedError, match="FAILED_CLOSED|failed closed"):
        transport._ensure_runtime(time.monotonic() + 0.1)
    assert starts == 1
    transport.close()


def test_startup_waiter_cannot_reopen_after_successful_close(monkeypatch) -> None:
    selector_entered = threading.Event()
    release_selector = threading.Event()
    waiter_waiting = threading.Event()
    real_start_actor = actor_module.start_actor
    starts = 0

    def blocking_selector_factory():
        selector_entered.set()
        assert release_selector.wait(timeout=2)
        return selectors.DefaultSelector()

    def blocked_start(*args, **kwargs):
        nonlocal starts
        starts += 1
        kwargs["selector_factory"] = blocking_selector_factory
        return real_start_actor(*args, **kwargs)

    monkeypatch.setattr(socket_module, "start_actor", blocked_start)
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    original_wait = transport._lifecycle.wait

    def observed_wait(timeout=None):
        if threading.current_thread().name == "startup-waiter":
            waiter_waiting.set()
        return original_wait(timeout)

    monkeypatch.setattr(transport._lifecycle, "wait", observed_wait)
    results: list[object] = []

    def ensure() -> None:
        try:
            results.append(transport._ensure_runtime(time.monotonic() + 1))
        except BaseException as exc:
            results.append(exc)

    starter = threading.Thread(target=ensure, name="startup-owner")
    waiter = threading.Thread(target=ensure, name="startup-waiter")
    closer = threading.Thread(target=transport.close, name="startup-closer")
    starter.start()
    assert selector_entered.wait(timeout=2)
    waiter.start()
    assert waiter_waiting.wait(timeout=2)
    closer.start()
    with transport._lifecycle:
        assert transport._lifecycle.wait_for(lambda: transport._closing, timeout=2)
    release_selector.set()
    starter.join(timeout=2)
    waiter.join(timeout=2)
    closer.join(timeout=2)

    assert not starter.is_alive() and not waiter.is_alive() and not closer.is_alive()
    assert len(results) == 2
    assert all(isinstance(item, ConnectionClosedError) for item in results)
    assert starts == 1
    assert transport._runtime is None and transport._candidate is None


def test_pool_retire_is_atomic_with_request_submission(monkeypatch) -> None:
    submit_entered = threading.Event()
    allow_submit = threading.Event()
    release_server = threading.Event()
    original_submit = socket_module.submit_request

    def gated_submit(*args, **kwargs):
        submit_entered.set()
        assert allow_submit.wait(timeout=2)
        return original_submit(*args, **kwargs)

    monkeypatch.setattr(socket_module, "submit_request", gated_submit)

    def handler(conn: socket.socket) -> None:
        try:
            msg_id, msg_type, _ = read_request(conn)
            conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
            msg_id, msg_type, _ = read_request(conn)
            conn.sendall(response_bytes(msg_id, msg_type, (321).to_bytes(2, "little")))
            release_server.wait(timeout=2)
        except (EOFError, OSError):
            return

    with Scripted7709Server([handler]) as server:
        pool = PooledSocketTransport([server.host], pool_size=1, timeout=1, heartbeat_interval=None)
        result: list[object] = []
        close_result: list[BaseException] = []

        def execute() -> None:
            try:
                result.append(pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"}))
            except BaseException as exc:
                result.append(exc)

        def close() -> None:
            try:
                pool.close()
            except BaseException as exc:
                close_result.append(exc)

        worker = threading.Thread(target=execute)
        closer = threading.Thread(target=close)
        worker.start()
        assert submit_entered.wait(timeout=2)
        closer.start()
        with pool._condition:
            assert pool._condition.wait_for(lambda: pool._state is PoolState.CLOSING, timeout=2)
        allow_submit.set()
        worker.join(timeout=2)
        closer.join(timeout=2)
        release_server.set()

        assert not worker.is_alive() and not closer.is_alive()
        assert close_result == []
        assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)
        assert pool._state is PoolState.STOPPED
        assert pool._transports[0]._runtime is None


def test_pooled_connect_future_cannot_resume_as_standalone_after_close(monkeypatch) -> None:
    future_entered = threading.Event()
    allow_future = threading.Event()
    close_done = threading.Event()
    close_result: list[object] = []

    def handler(conn: socket.socket) -> None:
        raise AssertionError("retired pool connect reached the server")

    with Scripted7709Server([handler]) as server:
        pool = PooledSocketTransport([server.host], pool_size=1, timeout=1, heartbeat_interval=None)
        slot = pool._transports[0]
        original_connect = slot._connect_with_deadline
        result: list[object] = []

        def paused_connect(**kwargs) -> None:
            future_entered.set()
            assert allow_future.wait(timeout=2)
            original_connect(**kwargs)

        monkeypatch.setattr(slot, "_connect_with_deadline", paused_connect)

        def connect() -> None:
            try:
                pool.connect()
            except BaseException as exc:
                result.append(exc)

        thread = threading.Thread(target=connect)
        def close() -> None:
            try:
                close_result.append(pool.close())
            except BaseException as exc:
                close_result.append(exc)
            finally:
                close_done.set()

        closer = threading.Thread(target=close)
        thread.start()
        assert future_entered.wait(timeout=2)
        closer.start()
        with pool._condition:
            assert pool._condition.wait_for(lambda: pool._state is PoolState.CLOSING, timeout=2)
        assert not close_done.wait(timeout=0.05)
        with pytest.raises(ConnectionClosedError, match="CLOSING"):
            pool._ensure_started()
        allow_future.set()
        thread.join(timeout=2)
        closer.join(timeout=2)

        assert not thread.is_alive() and not closer.is_alive()
        assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)
        assert close_result == [None]
        assert server.accepted_count == 0
        assert slot._runtime is None and slot._candidate is None
        assert pool._state is PoolState.STOPPED


def test_connect_rollback_attempt_blocks_close_until_old_futures_join(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=2, timeout=1, heartbeat_interval=None)
    slow_entered = threading.Event()
    release_slow = threading.Event()
    rollback_entered = threading.Event()
    allow_rollback = threading.Event()
    close_waiting = threading.Event()
    close_done = threading.Event()
    connect_result: list[BaseException] = []
    close_result: list[BaseException] = []
    original_begin = pool._begin_connect_rollback
    original_wait_attempt = pool._wait_shutdown_attempt

    def slow_connect(**kwargs) -> None:
        slow_entered.set()
        assert release_slow.wait(timeout=2)

    def failed_connect(**kwargs) -> None:
        assert slow_entered.wait(timeout=2)
        raise ConnectionClosedError("old epoch failed")

    def gated_begin(broker, push_buffer, **kwargs) -> None:
        original_begin(broker, push_buffer, **kwargs)
        rollback_entered.set()
        assert allow_rollback.wait(timeout=2)

    def observed_attempt_wait(attempt, **kwargs) -> None:
        close_waiting.set()
        original_wait_attempt(attempt, **kwargs)

    monkeypatch.setattr(pool._transports[0], "_connect_with_deadline", slow_connect)
    monkeypatch.setattr(pool._transports[1], "_connect_with_deadline", failed_connect)
    for slot in pool._transports:
        monkeypatch.setattr(slot, "_request_stop", lambda **_kwargs: release_slow.set())
    monkeypatch.setattr(pool, "_begin_connect_rollback", gated_begin)
    monkeypatch.setattr(pool, "_wait_shutdown_attempt", observed_attempt_wait)

    def connect() -> None:
        try:
            pool.connect()
        except BaseException as exc:
            connect_result.append(exc)

    def close() -> None:
        try:
            pool.close()
        except BaseException as exc:
            close_result.append(exc)
        finally:
            close_done.set()

    connect_thread = threading.Thread(target=connect)
    close_thread = threading.Thread(target=close)
    connect_thread.start()
    assert rollback_entered.wait(timeout=2)
    close_thread.start()
    assert close_waiting.wait(timeout=2)
    assert not close_done.is_set()
    with pytest.raises(ConnectionClosedError, match="CLOSING"):
        pool._ensure_started()
    allow_rollback.set()
    connect_thread.join(timeout=2)
    close_thread.join(timeout=2)

    assert not connect_thread.is_alive() and not close_thread.is_alive()
    assert len(connect_result) == 1 and isinstance(connect_result[0], ConnectionClosedError)
    assert close_result == []
    assert pool._state is PoolState.STOPPED
    new_broker, _ = pool._ensure_started()
    assert new_broker.pool_epoch > 1
    pool.close()


def test_connect_failure_with_stuck_future_fails_closed_at_request_deadline(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=2, timeout=0.1, heartbeat_interval=None)
    slow_entered = threading.Event()
    release_slow = threading.Event()
    slow_done = threading.Event()
    connect_done = threading.Event()
    results: list[BaseException] = []

    def slow_connect(**_kwargs) -> None:
        slow_entered.set()
        try:
            assert release_slow.wait(timeout=2)
        finally:
            slow_done.set()

    def failed_connect(**_kwargs) -> None:
        assert slow_entered.wait(timeout=2)
        raise ConnectionClosedError("slot failed")

    monkeypatch.setattr(pool._transports[0], "_connect_with_deadline", slow_connect)
    monkeypatch.setattr(pool._transports[1], "_connect_with_deadline", failed_connect)

    def connect() -> None:
        try:
            pool.connect()
        except BaseException as exc:
            results.append(exc)
        finally:
            connect_done.set()

    thread = threading.Thread(target=connect)
    started = time.monotonic()
    thread.start()
    try:
        assert slow_entered.wait(timeout=2)
        assert connect_done.wait(timeout=0.4)
        assert time.monotonic() - started < 0.4
        assert len(results) == 1 and isinstance(results[0], TransportCloseTimeoutError)
        assert pool._state is PoolState.FAILED_CLOSING
        attempt = pool._shutdown_attempt
        assert attempt is not None and attempt.completed.is_set()
        assert isinstance(attempt.error, TransportCloseTimeoutError)
        with pytest.raises(ConnectionClosedError, match="FAILED_CLOSING"):
            pool._ensure_started()
    finally:
        release_slow.set()
        slow_done.wait(timeout=2)
        thread.join(timeout=2)

    pool.close()
    assert pool._state is PoolState.FAILED_CLOSED


def test_partial_connect_worker_submission_is_stopped_and_joined(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=2, timeout=1, heartbeat_interval=None)
    release_slow = threading.Event()
    slow_done = threading.Event()
    real_executor = pool_module.ThreadPoolExecutor

    class FailingSecondSubmit:
        def __init__(self, *args, **kwargs) -> None:
            self.inner = real_executor(*args, **kwargs)
            self.calls = 0

        def submit(self, function, *args, **kwargs):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("connect submit failed")
            return self.inner.submit(function, *args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback) -> None:
            self.shutdown(wait=True, cancel_futures=True)

        def shutdown(self, *args, **kwargs) -> None:
            self.inner.shutdown(*args, **kwargs)

    def slow_connect(**_kwargs) -> None:
        try:
            assert release_slow.wait(timeout=2)
        finally:
            slow_done.set()

    monkeypatch.setattr(pool_module, "ThreadPoolExecutor", FailingSecondSubmit)
    monkeypatch.setattr(pool._transports[0], "_connect_with_deadline", slow_connect)
    for slot in pool._transports:
        monkeypatch.setattr(slot, "_request_stop", lambda **_kwargs: release_slow.set())

    with pytest.raises(RuntimeError, match="connect submit failed"):
        pool.connect()

    assert slow_done.wait(timeout=2)
    assert pool._state is PoolState.STOPPED
    assert pool._connect_executor is None
    assert pool._connect_futures == ()
    assert not any(
        thread.name.startswith("eltdx-pool-connect") and thread.is_alive()
        for thread in threading.enumerate()
    )


def test_shutdown_claim_prevents_connect_worker_after_future_snapshot(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=2, timeout=1, heartbeat_interval=None)
    pool._ensure_started()
    original_transports = list(pool._transports)
    between_submissions = threading.Event()
    allow_iteration = threading.Event()
    release_first = threading.Event()
    second_entered = threading.Event()
    connect_results: list[object] = []
    close_results: list[object] = []

    class PausedTransports:
        def __init__(self, items) -> None:
            self.items = items
            self.paused = False

        def __len__(self) -> int:
            return len(self.items)

        def __getitem__(self, index):
            return self.items[index]

        def __iter__(self):
            yield self.items[0]
            if not self.paused:
                self.paused = True
                between_submissions.set()
                assert allow_iteration.wait(timeout=2)
            yield self.items[1]

    pool._transports = PausedTransports(original_transports)  # type: ignore[assignment]

    def first_connect(**_kwargs) -> None:
        assert release_first.wait(timeout=2)

    def second_connect(**_kwargs) -> None:
        second_entered.set()

    monkeypatch.setattr(original_transports[0], "_connect_with_deadline", first_connect)
    monkeypatch.setattr(original_transports[1], "_connect_with_deadline", second_connect)
    for slot in original_transports:
        monkeypatch.setattr(slot, "_request_stop", lambda **_kwargs: release_first.set())

    connector = threading.Thread(target=lambda: _capture_result(pool.connect, connect_results))
    closer = threading.Thread(target=lambda: _capture_result(pool.close, close_results))
    connector.start()
    assert between_submissions.wait(timeout=2)
    closer.start()
    with pool._condition:
        assert pool._condition.wait_for(lambda: pool._state is PoolState.CLOSING, timeout=2)
        attempt = pool._shutdown_attempt
        assert attempt is not None and len(attempt.connect_futures) == 1
    allow_iteration.set()
    connector.join(timeout=2)
    closer.join(timeout=2)

    assert not connector.is_alive() and not closer.is_alive()
    assert not second_entered.is_set()
    assert len(connect_results) == 1 and isinstance(connect_results[0], ConnectionClosedError)
    assert close_results == [None]
    assert pool._state is PoolState.STOPPED


def _capture_result(function, results: list[object]) -> None:
    try:
        results.append(function())
    except BaseException as exc:
        results.append(exc)


def test_close_waits_for_unpublished_pool_startup_cleanup(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    slot = pool._transports[0]
    configured = threading.Event()
    allow_configure = threading.Event()
    clear_entered = threading.Event()
    allow_clear = threading.Event()
    close_done = threading.Event()
    starter_result: list[BaseException] = []
    close_result: list[BaseException] = []
    original_configure = slot._configure_pool_runtime
    original_clear = slot._clear_pool_runtime

    def gated_configure(**kwargs) -> None:
        original_configure(**kwargs)
        configured.set()
        assert allow_configure.wait(timeout=2)

    def gated_clear(**kwargs) -> bool:
        clear_entered.set()
        assert allow_clear.wait(timeout=2)
        return original_clear(**kwargs)

    monkeypatch.setattr(slot, "_configure_pool_runtime", gated_configure)
    monkeypatch.setattr(slot, "_clear_pool_runtime", gated_clear)

    def start() -> None:
        try:
            pool._ensure_started()
        except BaseException as exc:
            starter_result.append(exc)

    def close() -> None:
        try:
            pool.close()
        except BaseException as exc:
            close_result.append(exc)
        finally:
            close_done.set()

    starter = threading.Thread(target=start)
    closer = threading.Thread(target=close)
    starter.start()
    assert configured.wait(timeout=2)
    closer.start()
    with pool._condition:
        assert pool._condition.wait_for(lambda: pool._state is PoolState.CLOSING, timeout=2)
    allow_configure.set()
    assert clear_entered.wait(timeout=2)
    assert not close_done.is_set()
    allow_clear.set()
    starter.join(timeout=2)
    closer.join(timeout=2)

    assert not starter.is_alive() and not closer.is_alive()
    assert len(starter_result) == 1 and isinstance(starter_result[0], ConnectionClosedError)
    assert close_result == []
    assert pool._state is PoolState.STOPPED
    assert slot._shared_push_buffer is None


def test_old_connect_claim_is_noop_after_close_and_reopen(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    slot = pool._transports[0]
    claim_entered = threading.Event()
    allow_claim = threading.Event()
    result: list[BaseException] = []
    original_claim = pool._claim_shutdown_attempt

    def failed_connect(**kwargs) -> None:
        raise ConnectionClosedError("old connect failed")

    def gated_claim(**kwargs):
        if kwargs.get("expected_broker") is not None:
            claim_entered.set()
            assert allow_claim.wait(timeout=2)
        return original_claim(**kwargs)

    monkeypatch.setattr(slot, "_connect_with_deadline", failed_connect)
    monkeypatch.setattr(pool, "_claim_shutdown_attempt", gated_claim)

    def connect() -> None:
        try:
            pool.connect()
        except BaseException as exc:
            result.append(exc)

    thread = threading.Thread(target=connect)
    thread.start()
    assert claim_entered.wait(timeout=2)
    old_broker = pool._broker
    assert old_broker is not None
    pool.close()
    new_broker, _ = pool._ensure_started()
    assert new_broker is not old_broker
    assert not new_broker.snapshot().closed

    allow_claim.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)
    assert str(result[0]) == "old connect failed"
    assert pool._broker is new_broker
    assert pool._state is PoolState.RUNNING
    assert not new_broker.snapshot().closed
    assert slot._runtime is None and slot._candidate is None
    pool.close()


def test_stale_connect_condition_timeout_cannot_fail_reopened_epoch(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=0.25, heartbeat_interval=None)
    slot = pool._transports[0]
    claim_entered = threading.Event()
    allow_claim = threading.Event()
    holder_ready = threading.Event()
    release_holder = threading.Event()
    result: list[object] = []
    original_claim = pool._claim_shutdown_attempt

    def failed_connect(**_kwargs) -> None:
        raise ConnectionClosedError("old connect failed")

    def gated_claim(**kwargs):
        if kwargs.get("expected_broker") is not None:
            claim_entered.set()
            assert allow_claim.wait(timeout=2)
        return original_claim(**kwargs)

    monkeypatch.setattr(slot, "_connect_with_deadline", failed_connect)
    monkeypatch.setattr(pool, "_claim_shutdown_attempt", gated_claim)
    connector = threading.Thread(target=lambda: _capture_result(pool.connect, result))
    connector.start()
    assert claim_entered.wait(timeout=2)
    old_broker = pool._broker
    old_retire = pool._epoch_retire_event
    old_failure = pool._shutdown_failed
    assert old_broker is not None and old_retire is not None

    pool.close()
    new_broker, _ = pool._ensure_started()
    new_retire = pool._epoch_retire_event
    new_failure = pool._shutdown_failed
    assert new_broker is not old_broker and new_retire is not None

    def hold_new_condition() -> None:
        with pool._condition:
            holder_ready.set()
            assert release_holder.wait(timeout=2)

    holder = threading.Thread(target=hold_new_condition)
    holder.start()
    assert holder_ready.wait(timeout=2)
    allow_claim.set()
    try:
        connector.join(timeout=0.6)
        assert not connector.is_alive()
        assert len(result) == 1 and isinstance(result[0], TransportCloseTimeoutError)
        assert old_retire.is_set() and old_failure.is_set()
        assert not new_retire.is_set() and not new_failure.is_set()
    finally:
        release_holder.set()
        allow_claim.set()
        connector.join(timeout=2)
        holder.join(timeout=2)

    assert pool._broker is new_broker
    assert pool._state is PoolState.RUNNING
    assert pool._registrations[0].is_active()
    assert not new_broker.snapshot().closed
    pool.close()


def test_connect_rollback_publishes_close_timeout_before_slow_startup_deadline(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=2, timeout=3, heartbeat_interval=None)
    slow = pool._transports[0]
    failed = pool._transports[1]
    selector_entered = threading.Event()
    release_selector = threading.Event()
    close_done = threading.Event()
    connect_result: list[BaseException] = []
    close_result: list[BaseException] = []
    local = threading.local()
    real_start_actor = actor_module.start_actor
    real_slow_connect = slow._connect_with_deadline

    def blocking_selector_factory():
        selector_entered.set()
        assert release_selector.wait(timeout=4)
        return selectors.DefaultSelector()

    def dispatch_start(*args, **kwargs):
        if getattr(local, "slow_slot", False):
            kwargs["selector_factory"] = blocking_selector_factory
        return real_start_actor(*args, **kwargs)

    def slow_connect(**kwargs) -> None:
        local.slow_slot = True
        try:
            real_slow_connect(**kwargs)
        finally:
            local.slow_slot = False

    def failed_connect(**kwargs) -> None:
        assert selector_entered.wait(timeout=2)
        raise ConnectionClosedError("fast slot failed")

    monkeypatch.setattr(socket_module, "start_actor", dispatch_start)
    monkeypatch.setattr(slow, "_connect_with_deadline", slow_connect)
    monkeypatch.setattr(failed, "_connect_with_deadline", failed_connect)

    def connect() -> None:
        try:
            pool.connect()
        except BaseException as exc:
            connect_result.append(exc)

    def close() -> None:
        try:
            pool.close()
        except BaseException as exc:
            close_result.append(exc)
        finally:
            close_done.set()

    connect_thread = threading.Thread(target=connect)
    close_thread = threading.Thread(target=close)
    connect_thread.start()
    assert selector_entered.wait(timeout=2)
    with pool._condition:
        assert pool._condition.wait_for(lambda: pool._shutdown_active, timeout=2)
    close_thread.start()
    assert close_done.wait(timeout=1.5)
    assert len(close_result) == 1 and isinstance(close_result[0], TransportCloseTimeoutError)
    assert pool._state is PoolState.FAILED_CLOSING

    release_selector.set()
    connect_thread.join(timeout=2)
    close_thread.join(timeout=2)
    assert not connect_thread.is_alive() and not close_thread.is_alive()
    assert len(connect_result) == 1 and isinstance(connect_result[0], TransportCloseTimeoutError)

    pool.close()
    assert pool._state is PoolState.FAILED_CLOSED
    assert not any(
        thread.name.startswith("eltdx-7709-actor-") and thread.is_alive()
        for thread in threading.enumerate()
    )


def test_thread_start_failure_closes_owned_push_buffer(monkeypatch) -> None:
    def fail_start(thread) -> None:
        raise RuntimeError("thread start injection")

    monkeypatch.setattr(actor_module.threading.Thread, "start", fail_start)
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)

    with pytest.raises(ActorStartupError, match="before thread startup"):
        transport._ensure_runtime(time.monotonic() + 1)

    candidate = transport._candidate
    assert candidate is not None
    assert candidate.push_buffer is not None
    assert candidate.push_buffer.snapshot().closed
    assert candidate.stopped.is_set()
    assert candidate.actor_thread is not None and candidate.actor_thread.ident is None
    transport.close()
    assert transport._runtime is candidate
    assert candidate.state is RuntimeState.FAILED_CLOSED


def test_public_close_retries_unpublished_candidate_push_cleanup(monkeypatch) -> None:
    buffers: list[RetryPushBuffer] = []

    class RetryPushBuffer(PushBuffer):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.allow_close = False
            self.close_calls = 0
            buffers.append(self)

        def close(self, error=None) -> None:
            self.close_calls += 1
            if not self.allow_close:
                raise RuntimeError("candidate push cleanup injection")
            super().close(error)

    def fail_start(thread) -> None:
        raise RuntimeError("thread start injection")

    monkeypatch.setattr(socket_module, "PushBuffer", RetryPushBuffer)
    monkeypatch.setattr(actor_module.threading.Thread, "start", fail_start)
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)

    with pytest.raises(ActorStartupError, match="before thread startup"):
        transport._ensure_runtime(time.monotonic() + 1)

    candidate = transport._candidate
    assert candidate is not None and candidate.push_buffer is buffers[0]
    push = buffers[0]
    assert push.close_calls == 1 and not push.snapshot().closed
    with pytest.raises(TransportCloseTimeoutError, match="resource cleanup failed"):
        transport.close()
    assert push.close_calls == 2
    with transport._lifecycle:
        assert not transport._closing
        assert transport._close_failed

    push.allow_close = True
    transport.close()

    assert push.close_calls == 3 and push.snapshot().closed
    assert candidate.cleanup_error is None
    assert candidate.push_cleanup_error is None
    assert candidate.state is RuntimeState.FAILED_CLOSED
    assert transport._candidate is None and transport._runtime is candidate


def test_push_retry_preserves_startup_callback_cleanup_failure(monkeypatch) -> None:
    buffers: list[RetryPushBuffer] = []

    class RetryPushBuffer(PushBuffer):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.allow_close = False
            buffers.append(self)

        def close(self, error=None) -> None:
            if not self.allow_close:
                raise RuntimeError("candidate push cleanup injection")
            super().close(error)

    def reject_registration(_runtime: ActorRuntime) -> None:
        raise ValueError("candidate registration injection")

    def fail_fatal_callback(_runtime: ActorRuntime, _error: BaseException) -> None:
        raise LookupError("fatal callback cleanup injection")

    monkeypatch.setattr(socket_module, "PushBuffer", RetryPushBuffer)
    transport = SocketTransport(
        ["127.0.0.1:9"],
        timeout=1,
        heartbeat_interval=None,
        _runtime_started_callback=reject_registration,
        _actor_fatal_callback=fail_fatal_callback,
    )

    with pytest.raises(ActorStartupError, match="before thread startup"):
        transport._ensure_runtime(time.monotonic() + 1)

    candidate = transport._candidate
    assert candidate is not None and candidate.push_buffer is buffers[0]
    with pytest.raises(TransportCloseTimeoutError, match="resource cleanup failed"):
        transport.close()

    buffers[0].allow_close = True
    with pytest.raises(TransportCloseTimeoutError, match="resource cleanup failed"):
        transport.close()

    assert buffers[0].snapshot().closed
    assert isinstance(candidate.cleanup_error, LookupError)
    assert str(candidate.cleanup_error) == "fatal callback cleanup injection"
    assert candidate.push_cleanup_error is None
    assert candidate.state is RuntimeState.FAILED_CLOSING

    with candidate.control_lock:
        candidate.cleanup_error = None
        candidate.deferred_cleanup_error = None
    transport.close()
    assert candidate.state is RuntimeState.FAILED_CLOSED


def test_push_snapshot_failure_cannot_strand_public_close(monkeypatch) -> None:
    buffers: list[UnreliableSnapshotPushBuffer] = []

    class UnreliableSnapshotPushBuffer(PushBuffer):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.allow_close = False
            self.allow_snapshot = False
            buffers.append(self)

        def close(self, error=None) -> None:
            if not self.allow_close:
                raise RuntimeError("candidate push cleanup injection")
            super().close(error)

        def snapshot(self):
            if not self.allow_snapshot:
                raise LookupError("push snapshot injection")
            return super().snapshot()

    def fail_start(thread) -> None:
        raise RuntimeError("thread start injection")

    monkeypatch.setattr(socket_module, "PushBuffer", UnreliableSnapshotPushBuffer)
    monkeypatch.setattr(actor_module.threading.Thread, "start", fail_start)
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)

    with pytest.raises(ActorStartupError, match="before thread startup"):
        transport._ensure_runtime(time.monotonic() + 1)
    candidate = transport._candidate
    assert candidate is not None and candidate.push_buffer is buffers[0]

    with pytest.raises(TransportCloseTimeoutError, match="resource cleanup failed"):
        transport.close()
    with transport._lifecycle:
        assert not transport._closing
        assert transport._close_failed

    buffers[0].allow_close = True
    buffers[0].allow_snapshot = True
    transport.close()

    assert buffers[0].snapshot().closed
    assert candidate.cleanup_error is None
    assert candidate.state is RuntimeState.FAILED_CLOSED


def test_actor_fatal_callback_failure_survives_push_cleanup_retry(monkeypatch) -> None:
    buffers: list[RetryPushBuffer] = []
    real_start_actor = actor_module.start_actor

    class RetryPushBuffer(PushBuffer):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.allow_close = False
            buffers.append(self)

        def close(self, error=None) -> None:
            if not self.allow_close:
                raise RuntimeError("actor push cleanup injection")
            super().close(error)

    def start_with_fatal_selector(*args, **kwargs):
        kwargs["selector_factory"] = lambda: (_ for _ in ()).throw(ValueError("selector startup injection"))
        return real_start_actor(*args, **kwargs)

    def fail_fatal_callback(_runtime: ActorRuntime, _error: BaseException) -> None:
        raise LookupError("actor fatal callback injection")

    monkeypatch.setattr(socket_module, "PushBuffer", RetryPushBuffer)
    monkeypatch.setattr(socket_module, "start_actor", start_with_fatal_selector)
    transport = SocketTransport(
        ["127.0.0.1:9"],
        timeout=1,
        heartbeat_interval=None,
        _actor_fatal_callback=fail_fatal_callback,
    )

    with pytest.raises(ActorStartupError, match="failed during startup"):
        transport._ensure_runtime(time.monotonic() + 1)
    candidate = transport._candidate
    assert candidate is not None and candidate.push_buffer is buffers[0]

    with pytest.raises(TransportCloseTimeoutError, match="resource cleanup failed"):
        transport.close()
    buffers[0].allow_close = True
    transport.close()

    assert buffers[0].snapshot().closed
    assert candidate.cleanup_error is None
    assert candidate.push_cleanup_error is None
    assert candidate.state is RuntimeState.FAILED_CLOSED


def test_close_stop_failure_still_cleans_every_owned_runtime(monkeypatch) -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    runtime = ActorRuntime(1, ())
    candidate = ActorRuntime(2, ())
    with transport._lifecycle:
        transport._runtime = runtime
        transport._candidate = candidate
    stop_calls: list[ActorRuntime] = []
    close_calls: list[ActorRuntime] = []

    def stop(item: ActorRuntime, **_kwargs) -> None:
        stop_calls.append(item)
        if item is runtime:
            raise RuntimeError("stop request injection")

    monkeypatch.setattr(socket_module, "request_actor_stop", stop)
    monkeypatch.setattr(socket_module, "close_actor", lambda item, timeout: close_calls.append(item))

    with pytest.raises(RuntimeError, match="stop request injection"):
        transport._close_with_timeout(0.1)

    assert stop_calls == [runtime, candidate, runtime, candidate]
    assert close_calls == [runtime, candidate]
    with transport._lifecycle:
        assert not transport._closing
        assert transport._close_failed


def test_close_owner_resets_state_when_startup_wait_raises(monkeypatch) -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    candidate = ActorRuntime(7, ())
    with transport._lifecycle:
        transport._starting = True

    def fail_wait(timeout=None) -> bool:
        transport._candidate = candidate
        raise RuntimeError("lifecycle wait injection")

    monkeypatch.setattr(transport._lifecycle, "wait", fail_wait)

    with pytest.raises(RuntimeError, match="lifecycle wait injection"):
        transport._close_with_timeout(0.1)

    with transport._lifecycle:
        assert not transport._closing
        assert transport._close_failed
        transport._starting = False
    assert candidate.stop_requested
    assert candidate.state is RuntimeState.FAILED_CLOSING


def test_close_owner_resets_state_when_publish_notification_raises(monkeypatch) -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    real_notify_all = transport._lifecycle.notify_all
    notify_calls = 0

    def fail_first_notify() -> None:
        nonlocal notify_calls
        notify_calls += 1
        if notify_calls == 1:
            raise RuntimeError("close owner notification injection")
        real_notify_all()

    monkeypatch.setattr(transport._lifecycle, "notify_all", fail_first_notify)

    with pytest.raises(RuntimeError, match="close owner notification injection"):
        transport._close_with_timeout(0.1)

    with transport._lifecycle:
        assert not transport._closing
        assert transport._close_failed
    transport.close()
    assert notify_calls >= 3


def test_thread_constructor_failure_closes_owned_push_buffer(monkeypatch) -> None:
    def fail_constructor(*args, **kwargs):
        raise RuntimeError("thread constructor injection")

    monkeypatch.setattr(actor_module.threading, "Thread", fail_constructor)
    push = PushBuffer(77)

    with pytest.raises(ActorStartupError, match="before thread startup") as raised:
        actor_module.start_actor(77, (), push_buffer=push)

    runtime = raised.value.runtime
    assert isinstance(raised.value.__cause__, RuntimeError)
    assert str(raised.value.__cause__) == "thread constructor injection"
    assert runtime.actor_thread is None
    assert runtime.stopped.is_set() and runtime.started.is_set()
    assert push.snapshot().closed
    assert runtime.cleanup_error is None
    actor_module.close_actor(runtime)
    assert runtime.state is RuntimeState.FAILED_CLOSED


def test_before_thread_push_cleanup_failure_is_retained_and_surfaced() -> None:
    class FailingPushBuffer(PushBuffer):
        def __init__(self) -> None:
            super().__init__(78)
            self.allow_close = False

        def close(self, error=None) -> None:
            if not self.allow_close:
                raise RuntimeError("push cleanup injection")
            super().close(error)

    push = FailingPushBuffer()

    def reject_candidate(_runtime: ActorRuntime) -> None:
        raise ValueError("candidate callback injection")

    with pytest.raises(ActorStartupError, match="before thread startup") as raised:
        actor_module.start_actor(78, (), push_buffer=push, candidate_callback=reject_candidate)

    runtime = raised.value.runtime
    assert isinstance(raised.value.__cause__, ValueError)
    assert str(raised.value.__cause__) == "candidate callback injection"
    assert isinstance(runtime.cleanup_error, RuntimeError)
    assert str(runtime.cleanup_error) == "push cleanup injection"
    assert not push.snapshot().closed
    with pytest.raises(TransportCloseTimeoutError, match="resource cleanup failed"):
        actor_module.close_actor(runtime)

    push.allow_close = True
    push.close()
    with runtime.control_lock:
        runtime.cleanup_error = None
    actor_module.close_actor(runtime)
    assert runtime.state is RuntimeState.FAILED_CLOSED


def test_public_close_surfaces_and_retains_selector_cleanup_failure(monkeypatch) -> None:
    selectors_created: list[selectors.SelectSelector] = []
    real_start_actor = actor_module.start_actor

    class FailingCloseSelector(selectors.SelectSelector):
        def __init__(self) -> None:
            super().__init__()
            selectors_created.append(self)

        def register(self, *args, **kwargs):
            raise RuntimeError("deterministic selector registration failure")

        def close(self) -> None:
            raise RuntimeError("deterministic selector close failure")

    def start_with_failing_selector(*args, **kwargs):
        kwargs["selector_factory"] = FailingCloseSelector
        return real_start_actor(*args, **kwargs)

    monkeypatch.setattr(socket_module, "start_actor", start_with_failing_selector)
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)

    with pytest.raises(ActorStartupError, match="failed during startup"):
        transport._ensure_runtime(time.monotonic() + 1)

    candidate = transport._candidate
    assert candidate is not None and candidate.actor_thread is not None
    candidate.actor_thread.join(timeout=2)
    assert not candidate.actor_thread.is_alive()
    assert len(selectors_created) == 1 and candidate.selector is selectors_created[0]
    assert candidate.wake_reader is None and candidate.wake_writer is None
    assert isinstance(candidate.cleanup_error, RuntimeError)
    with pytest.raises(TransportCloseTimeoutError, match="resource cleanup failed"):
        transport.close()
    assert transport._candidate is candidate
    assert candidate.selector is selectors_created[0]
    assert candidate.state is RuntimeState.FAILED_CLOSING

    selectors.SelectSelector.close(selectors_created[0])
    with candidate.control_lock:
        candidate.selector = None
        candidate.cleanup_error = None
    transport.close()
    assert transport._candidate is None and transport._runtime is candidate
    assert candidate.state is RuntimeState.FAILED_CLOSED


def test_public_close_can_retry_owned_push_cleanup_failure(monkeypatch) -> None:
    release = threading.Event()

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, (75).to_bytes(2, "little")))
        release.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        transport = SocketTransport([server.host], timeout=1, heartbeat_interval=None)
        assert transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 75
        runtime = transport._runtime
        push = transport._push_buffer
        assert runtime is not None and push is not None
        real_close = push.close
        allow_close = False

        def fail_close(error=None) -> None:
            if not allow_close:
                raise RuntimeError("push cleanup injection")
            real_close(error)

        monkeypatch.setattr(push, "close", fail_close)
        try:
            with pytest.raises(TransportCloseTimeoutError, match="resource cleanup failed"):
                transport.close()

            assert runtime.stopped.wait(timeout=2)
            with transport._lifecycle:
                assert not transport._closing
                assert transport._close_failed
                assert transport._runtime is runtime
            assert isinstance(runtime.cleanup_error, RuntimeError)
            assert str(runtime.cleanup_error) == "push cleanup injection"
            assert not push.snapshot().closed

            allow_close = True
            transport.close()

            assert push.snapshot().closed
            assert runtime.cleanup_error is None
            assert runtime.state is RuntimeState.FAILED_CLOSED
            with transport._lifecycle:
                assert not transport._closing
        finally:
            allow_close = True
            release.set()
            try:
                transport.close()
            except TransportCloseTimeoutError:
                pass


def test_push_retry_cannot_hide_retained_selector_cleanup_failure(monkeypatch) -> None:
    release = threading.Event()
    selectors_created: list[selectors.SelectSelector] = []
    real_start_actor = actor_module.start_actor

    class FailingCloseSelector(selectors.SelectSelector):
        def __init__(self) -> None:
            super().__init__()
            self.allow_close = False
            self.close_calls = 0
            selectors_created.append(self)

        def close(self) -> None:
            self.close_calls += 1
            if not self.allow_close:
                raise RuntimeError("selector cleanup injection")
            super().close()

    def start_with_failing_selector(*args, **kwargs):
        kwargs["selector_factory"] = FailingCloseSelector
        return real_start_actor(*args, **kwargs)

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, (76).to_bytes(2, "little")))
        release.wait(timeout=2)

    monkeypatch.setattr(socket_module, "start_actor", start_with_failing_selector)
    with Scripted7709Server([handler]) as server:
        transport = SocketTransport([server.host], timeout=1, heartbeat_interval=None)
        assert transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 76
        runtime = transport._runtime
        push = transport._push_buffer
        assert runtime is not None and push is not None
        real_push_close = push.close
        allow_push_close = False

        def fail_push_close(error=None) -> None:
            if not allow_push_close:
                raise RuntimeError("push cleanup injection")
            real_push_close(error)

        monkeypatch.setattr(push, "close", fail_push_close)
        selector = selectors_created[0]
        try:
            with pytest.raises(TransportCloseTimeoutError, match="resource cleanup failed"):
                transport.close()
            assert runtime.stopped.wait(timeout=2)
            assert runtime.selector is selector
            assert runtime.cleanup_error is runtime.push_cleanup_error

            allow_push_close = True
            with pytest.raises(TransportCloseTimeoutError, match="resource cleanup failed"):
                transport.close()

            assert push.snapshot().closed
            assert runtime.selector is selector
            assert runtime.cleanup_error is not None
            assert runtime.state is RuntimeState.FAILED_CLOSING

            selector.allow_close = True
            selector.close()
            with runtime.control_lock:
                runtime.selector = None
                runtime.cleanup_error = None
                runtime.deferred_cleanup_error = None
            transport.close()

            assert selector.close_calls == 2
            assert runtime.cleanup_error is None
            assert runtime.push_cleanup_error is None
            assert runtime.state is RuntimeState.FAILED_CLOSED
        finally:
            allow_push_close = True
            selector.allow_close = True
            release.set()
            try:
                transport.close()
            except TransportCloseTimeoutError:
                pass


def test_old_startup_waiter_cannot_inherit_runtime_created_after_close(monkeypatch) -> None:
    selector_entered = threading.Event()
    release_selector = threading.Event()
    waiter_waiting = threading.Event()
    waiter_gated = threading.Event()
    allow_waiter = threading.Event()
    real_start_actor = actor_module.start_actor
    starts = 0

    def blocking_selector_factory():
        selector_entered.set()
        assert release_selector.wait(timeout=2)
        return selectors.DefaultSelector()

    def controlled_start(*args, **kwargs):
        nonlocal starts
        starts += 1
        if starts == 1:
            kwargs["selector_factory"] = blocking_selector_factory
        return real_start_actor(*args, **kwargs)

    monkeypatch.setattr(socket_module, "start_actor", controlled_start)
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    original_wait = transport._lifecycle.wait
    original_active = transport._pool_runtime_is_active
    active_calls: dict[int, int] = {}

    def observed_wait(timeout=None):
        if threading.current_thread().name == "old-startup-waiter":
            waiter_waiting.set()
        return original_wait(timeout)

    def gated_active(expected_runtime_epoch=None, **kwargs):
        ident = threading.get_ident()
        active_calls[ident] = active_calls.get(ident, 0) + 1
        if threading.current_thread().name == "old-startup-waiter" and active_calls[ident] >= 2:
            waiter_gated.set()
            assert allow_waiter.wait(timeout=2)
        return original_active(expected_runtime_epoch, **kwargs)

    monkeypatch.setattr(transport._lifecycle, "wait", observed_wait)
    monkeypatch.setattr(transport, "_pool_runtime_is_active", gated_active)
    results: list[object] = []

    def ensure() -> None:
        try:
            results.append(transport._ensure_runtime(time.monotonic() + 1))
        except BaseException as exc:
            results.append(exc)

    owner = threading.Thread(target=ensure, name="old-startup-owner")
    waiter = threading.Thread(target=ensure, name="old-startup-waiter")
    closer = threading.Thread(target=transport.close)
    owner.start()
    assert selector_entered.wait(timeout=2)
    waiter.start()
    assert waiter_waiting.wait(timeout=2)
    closer.start()
    with transport._lifecycle:
        assert transport._lifecycle.wait_for(lambda: transport._closing, timeout=2)
    release_selector.set()
    assert waiter_gated.wait(timeout=2)
    owner.join(timeout=2)
    closer.join(timeout=2)
    assert not owner.is_alive() and not closer.is_alive()

    new_runtime = transport._ensure_runtime(time.monotonic() + 1)
    assert new_runtime.state is RuntimeState.RUNNING
    assert starts == 2
    allow_waiter.set()
    waiter.join(timeout=2)

    assert not waiter.is_alive()
    assert len(results) == 2
    assert all(isinstance(item, ConnectionClosedError) for item in results)
    assert transport._runtime is new_runtime
    transport.close()


class ObservedSubmissionGate:
    def __init__(self, close_waiting: threading.Event) -> None:
        self._lock = threading.Lock()
        self._close_waiting = close_waiting

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._lock.release()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        if threading.current_thread().name == "standalone-closer":
            self._close_waiting.set()
        return self._lock.acquire(blocking, timeout)

    def release(self) -> None:
        self._lock.release()


def test_standalone_close_linearizes_with_submission_gate(monkeypatch) -> None:
    submit_entered = threading.Event()
    allow_submit = threading.Event()
    allow_response = threading.Event()
    close_waiting = threading.Event()
    original_submit = socket_module.submit_request

    def gated_submit(*args, **kwargs):
        submit_entered.set()
        assert allow_submit.wait(timeout=2)
        return original_submit(*args, **kwargs)

    monkeypatch.setattr(socket_module, "submit_request", gated_submit)

    def handler(conn: socket.socket) -> None:
        try:
            msg_id, msg_type, _ = read_request(conn)
            conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
            msg_id, msg_type, _ = read_request(conn)
            assert allow_response.wait(timeout=2)
            conn.sendall(response_bytes(msg_id, msg_type, (444).to_bytes(2, "little")))
        except (EOFError, OSError):
            return

    with Scripted7709Server([handler]) as server:
        transport = SocketTransport([server.host], timeout=1, heartbeat_interval=None)
        transport._submission_gate = ObservedSubmissionGate(close_waiting)
        result: list[object] = []
        close_result: list[BaseException] = []

        def execute() -> None:
            try:
                result.append(transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}))
            except BaseException as exc:
                result.append(exc)

        def close() -> None:
            try:
                transport.close()
            except BaseException as exc:
                close_result.append(exc)

        worker = threading.Thread(target=execute)
        closer = threading.Thread(target=close, name="standalone-closer")
        worker.start()
        assert submit_entered.wait(timeout=2)
        closer.start()
        assert close_waiting.wait(timeout=2)
        with transport._lifecycle:
            assert not transport._closing
        allow_submit.set()
        worker.join(timeout=2)
        closer.join(timeout=2)
        allow_response.set()

        assert not worker.is_alive() and not closer.is_alive()
        assert close_result == []
        assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)
        assert transport._runtime is None and transport._candidate is None


def test_configure_failure_cleanup_exception_does_not_stick_startup(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=2, timeout=0.2, heartbeat_interval=None)
    first = pool._transports[0]
    second = pool._transports[1]
    original_first_configure = first._configure_pool_runtime
    retire_calls = 0

    def configure_first(**kwargs) -> None:
        original_first_configure(**kwargs)

    def configure_second(**kwargs) -> None:
        raise RuntimeError("configure injection")

    def fail_first_retire(registration, **_kwargs) -> bool:
        nonlocal retire_calls
        retire_calls += 1
        if retire_calls == 1:
            raise RuntimeError("cleanup injection")
        return True

    monkeypatch.setattr(first, "_configure_pool_runtime", configure_first)
    monkeypatch.setattr(second, "_configure_pool_runtime", configure_second)
    monkeypatch.setattr(first, "_retire_pool_runtime", fail_first_retire)

    with pytest.raises(RuntimeError, match="configure injection"):
        pool._ensure_started()

    assert not pool._startup_active
    assert pool._state is PoolState.FAILED_CLOSING
    with pytest.raises(RuntimeError, match="cleanup injection"):
        pool.close()
    assert not pool._shutdown_active
    assert pool._state is PoolState.FAILED_CLOSING
    pool.close()
    assert pool._state is PoolState.FAILED_CLOSED


def test_publish_false_cleanup_exception_fails_waiting_close(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    slot = pool._transports[0]
    configured = threading.Event()
    allow_configure = threading.Event()
    original_configure = slot._configure_pool_runtime
    original_retire = slot._retire_pool_runtime
    retire_calls = 0
    starter_result: list[BaseException] = []
    close_result: list[BaseException] = []

    def gated_configure(**kwargs) -> None:
        original_configure(**kwargs)
        configured.set()
        assert allow_configure.wait(timeout=2)

    def fail_first_retire(registration, **kwargs) -> bool:
        nonlocal retire_calls
        retire_calls += 1
        if retire_calls == 1:
            raise RuntimeError("publish cleanup injection")
        return original_retire(registration, **kwargs)

    monkeypatch.setattr(slot, "_configure_pool_runtime", gated_configure)
    monkeypatch.setattr(slot, "_retire_pool_runtime", fail_first_retire)

    def start() -> None:
        try:
            pool._ensure_started()
        except BaseException as exc:
            starter_result.append(exc)

    def close() -> None:
        try:
            pool.close()
        except BaseException as exc:
            close_result.append(exc)

    starter = threading.Thread(target=start)
    closer = threading.Thread(target=close)
    starter.start()
    assert configured.wait(timeout=2)
    closer.start()
    with pool._condition:
        assert pool._condition.wait_for(lambda: pool._state is PoolState.CLOSING, timeout=2)
    allow_configure.set()
    starter.join(timeout=2)
    closer.join(timeout=2)

    assert not starter.is_alive() and not closer.is_alive()
    assert len(starter_result) == 1 and isinstance(starter_result[0], RuntimeError)
    assert len(close_result) == 1 and isinstance(close_result[0], RuntimeError)
    assert str(close_result[0]) == "publish cleanup injection"
    assert not pool._startup_active and not pool._shutdown_active
    assert pool._state is PoolState.FAILED_CLOSING
    assert pool._broker is not None and pool._broker.snapshot().closed
    assert pool._push_buffer is not None and pool._push_buffer.snapshot().closed
    assert slot._shared_push_buffer is None
    pool.close()
    assert pool._state is PoolState.FAILED_CLOSED


def test_submission_gate_close_timeout_rejects_runtime_before_stop_publication(monkeypatch) -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    runtime = transport._ensure_runtime(time.monotonic() + 1)
    stop_entered = threading.Event()
    allow_stop = threading.Event()
    close_result: list[BaseException] = []
    real_stop = socket_module.request_actor_stop

    def gated_stop(item: ActorRuntime, **kwargs) -> None:
        stop_entered.set()
        assert allow_stop.wait(timeout=2)
        real_stop(item, **kwargs)

    monkeypatch.setattr(socket_module, "request_actor_stop", gated_stop)
    assert transport._submission_gate.acquire(timeout=1)

    def close() -> None:
        try:
            transport._close_with_timeout(0.02)
        except BaseException as exc:
            close_result.append(exc)

    closer = threading.Thread(target=close)
    closer.start()
    assert stop_entered.wait(timeout=2)
    assert transport._close_failed
    transport._submission_gate.release()

    with pytest.raises(ConnectionClosedError, match="not usable"):
        transport._ensure_runtime(time.monotonic() + 0.1)
    with pytest.raises(ConnectionClosedError, match="not usable"):
        transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"})

    allow_stop.set()
    closer.join(timeout=2)
    assert not closer.is_alive()
    assert len(close_result) == 1 and isinstance(close_result[0], TransportCloseTimeoutError)
    assert runtime.stopped.wait(timeout=2)
    transport.close()
    assert runtime.state is RuntimeState.FAILED_CLOSED


def test_socket_close_lifecycle_lock_obeys_single_deadline() -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    lock_held = threading.Event()
    release_lock = threading.Event()

    def hold_lifecycle_lock() -> None:
        with transport._lifecycle:
            lock_held.set()
            release_lock.wait(timeout=0.5)

    holder = threading.Thread(target=hold_lifecycle_lock)
    holder.start()
    assert lock_held.wait(timeout=2)

    started = time.monotonic()
    try:
        with pytest.raises(TransportCloseTimeoutError, match="lifecycle"):
            transport._close_with_timeout(0.05)
    finally:
        elapsed = time.monotonic() - started
        release_lock.set()
        holder.join(timeout=2)

    assert not holder.is_alive()
    assert elapsed < 0.25
    assert transport._close_failed
    transport.close()


def test_close_attempt_event_wakes_waiter_when_abort_cannot_notify(monkeypatch) -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    finish_entered = threading.Event()
    fail_first_finish = threading.Event()
    waiter_waiting = threading.Event()
    holder_locked = threading.Event()
    release_holder = threading.Event()
    owner_result: list[object] = []
    waiter_result: list[object] = []
    finish_calls = 0

    def controlled_finish(*_args, **_kwargs) -> None:
        nonlocal finish_calls
        finish_calls += 1
        if finish_calls == 1:
            finish_entered.set()
            assert fail_first_finish.wait(timeout=2)
            raise RuntimeError("close owner injection")

    monkeypatch.setattr(transport, "_finish_close_owner", controlled_finish)
    owner = threading.Thread(
        target=lambda: _capture_result(lambda: transport._close_with_timeout(0.15), owner_result),
        name="close-owner",
    )
    owner.start()
    assert finish_entered.wait(timeout=2)
    with transport._lifecycle:
        attempt = transport._close_attempt
        assert attempt is not None
        real_wait = attempt.completed.wait

        def observed_wait(timeout=None) -> bool:
            if threading.current_thread().name == "close-waiter":
                waiter_waiting.set()
            return real_wait(timeout)

        monkeypatch.setattr(attempt.completed, "wait", observed_wait)

    waiter = threading.Thread(
        target=lambda: _capture_result(lambda: transport._close_with_timeout(0.6), waiter_result),
        name="close-waiter",
    )
    waiter.start()
    assert waiter_waiting.wait(timeout=2)

    def hold_lifecycle() -> None:
        with transport._lifecycle:
            holder_locked.set()
            assert release_holder.wait(timeout=2)

    holder = threading.Thread(target=hold_lifecycle)
    holder.start()
    assert holder_locked.wait(timeout=2)
    fail_first_finish.set()
    owner.join(timeout=0.4)
    assert not owner.is_alive()

    released_at = time.monotonic()
    release_holder.set()
    waiter.join(timeout=0.3)
    holder.join(timeout=2)

    assert not waiter.is_alive() and not holder.is_alive()
    assert time.monotonic() - released_at < 0.3
    assert len(owner_result) == 1 and isinstance(owner_result[0], RuntimeError)
    assert waiter_result == [None]
    assert finish_calls == 2


def test_late_candidate_after_lifecycle_close_timeout_is_rejected(monkeypatch) -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    start_entered = threading.Event()
    allow_start = threading.Event()
    created: list[ActorRuntime] = []
    result: list[object] = []
    real_start_actor = socket_module.start_actor

    def blocked_start(*args, **kwargs):
        start_entered.set()
        assert allow_start.wait(timeout=2)
        runtime = real_start_actor(*args, **kwargs)
        created.append(runtime)
        return runtime

    monkeypatch.setattr(socket_module, "start_actor", blocked_start)
    starter = threading.Thread(
        target=lambda: _capture_result(lambda: transport._ensure_runtime(time.monotonic() + 1), result)
    )
    starter.start()
    assert start_entered.wait(timeout=2)

    with pytest.raises(TransportCloseTimeoutError, match="startup did not finish"):
        transport._close_with_timeout(0.05)
    assert transport._close_failed and not transport._closing

    allow_start.set()
    starter.join(timeout=2)
    assert not starter.is_alive()
    assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)
    assert len(created) == 1
    runtime = created[0]
    assert runtime.stop_requested and runtime.stopped.is_set()
    assert runtime.actor_thread is not None and not runtime.actor_thread.is_alive()
    assert transport._runtime is runtime
    assert runtime.state is RuntimeState.FAILED_CLOSED
    with pytest.raises(ConnectionClosedError, match="FAILED_CLOSED"):
        transport._ensure_runtime(time.monotonic() + 0.1)
    transport.close()


def test_pool_close_stops_actor_when_shutdown_condition_hits_deadline() -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    broker, _ = pool._ensure_started()
    runtime = pool._transports[0]._ensure_runtime(
        time.monotonic() + 1,
        expected_runtime_epoch=broker.pool_epoch,
    )
    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_condition() -> None:
        with pool._condition:
            condition_held.set()
            release_condition.wait(timeout=2)

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)

    started = time.monotonic()
    try:
        with pytest.raises(TransportCloseTimeoutError, match="shutdown claim"):
            pool.close()
        elapsed = time.monotonic() - started
        assert elapsed < 1.25
        assert runtime.stop_requested
        assert runtime.stopped.wait(timeout=0.25)
        assert pool._shutdown_failed.is_set()
    finally:
        release_condition.set()
        holder.join(timeout=2)

    assert not holder.is_alive()
    assert pool.diagnostics.state is PoolState.FAILED_CLOSING
    pool.close()
    assert pool._state is PoolState.FAILED_CLOSED
    assert runtime.actor_thread is not None and not runtime.actor_thread.is_alive()


def test_pool_connect_condition_cleanup_obeys_request_deadline(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=0.1, heartbeat_interval=None)
    task_entered = threading.Event()
    release_task = threading.Event()
    holder_ready = threading.Event()
    release_holder = threading.Event()
    connect_done = threading.Event()
    results: list[object] = []

    def fake_connect(**_kwargs) -> None:
        task_entered.set()
        assert release_task.wait(timeout=2)

    monkeypatch.setattr(pool._transports[0], "_connect_with_deadline", fake_connect)

    def connect() -> None:
        try:
            _capture_result(pool.connect, results)
        finally:
            connect_done.set()

    connector = threading.Thread(target=connect)
    started = time.monotonic()
    connector.start()
    assert task_entered.wait(timeout=2)

    def hold_condition() -> None:
        with pool._condition:
            holder_ready.set()
            assert release_holder.wait(timeout=2)

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert holder_ready.wait(timeout=2)
    release_task.set()
    try:
        assert connect_done.wait(timeout=0.35)
        assert time.monotonic() - started < 0.35
        assert len(results) == 1 and isinstance(results[0], TransportCloseTimeoutError)
        assert pool._shutdown_failed.is_set()
    finally:
        release_holder.set()
        release_task.set()
        connector.join(timeout=2)
        holder.join(timeout=2)

    assert not connector.is_alive() and not holder.is_alive()
    pool.close()
    assert pool._state is PoolState.FAILED_CLOSED


def test_pool_connect_does_not_wait_unbounded_for_done_callback(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=0.1, heartbeat_interval=None)
    task_entered = threading.Event()
    release_task = threading.Event()
    wait_entered = threading.Event()
    callback_entered = threading.Event()
    callback_done = threading.Event()
    release_callback = threading.Event()
    connect_done = threading.Event()
    results: list[object] = []
    real_wait = pool_module.wait
    real_terminal = pool._connect_future_terminal
    wait_calls = 0

    def fake_connect(**_kwargs) -> None:
        task_entered.set()
        assert release_task.wait(timeout=2)

    def observed_wait(*args, **kwargs):
        nonlocal wait_calls
        wait_calls += 1
        if wait_calls == 1:
            wait_entered.set()
        return real_wait(*args, **kwargs)

    def blocked_callback(executor, **kwargs) -> None:
        worker_callback = threading.current_thread().name.startswith("eltdx-pool-connect")
        try:
            if worker_callback:
                callback_entered.set()
                assert release_callback.wait(timeout=2)
            real_terminal(executor, **kwargs)
        finally:
            if worker_callback:
                callback_done.set()

    monkeypatch.setattr(pool._transports[0], "_connect_with_deadline", fake_connect)
    monkeypatch.setattr(pool_module, "wait", observed_wait)
    monkeypatch.setattr(pool, "_connect_future_terminal", blocked_callback)

    def connect() -> None:
        try:
            _capture_result(pool.connect, results)
        finally:
            connect_done.set()

    connector = threading.Thread(target=connect)
    started = time.monotonic()
    connector.start()
    assert task_entered.wait(timeout=2)
    assert wait_entered.wait(timeout=2)
    release_task.set()
    assert callback_entered.wait(timeout=2)
    try:
        assert connect_done.wait(timeout=0.35)
        assert time.monotonic() - started < 0.35
        assert len(results) == 1 and isinstance(results[0], TransportCloseTimeoutError)
        assert pool._shutdown_failed.is_set()
    finally:
        release_callback.set()
        release_task.set()
        connector.join(timeout=2)

    assert not connector.is_alive()
    assert callback_done.wait(timeout=2)
    pool.close()
    assert pool._state is PoolState.FAILED_CLOSED
    assert pool._connect_executor is None
    assert pool._connect_futures == ()
    assert not any(
        thread.name.startswith("eltdx-pool-connect") and thread.is_alive()
        for thread in threading.enumerate()
    )


def test_socket_cached_endpoint_lifecycle_wait_obeys_request_deadline(monkeypatch) -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=0.05, heartbeat_interval=None)
    transport._resolved_endpoints = socket_module.resolve_hosts(transport._hosts)
    runtime = ActorRuntime(1, transport._resolved_endpoints)
    monkeypatch.setattr(transport, "_ensure_runtime", lambda *_args, **_kwargs: runtime)
    monkeypatch.setattr(transport, "_execute_with_lease", lambda *_args, **_kwargs: 123)
    lifecycle_held = threading.Event()
    release_lifecycle = threading.Event()

    def hold_lifecycle() -> None:
        with transport._lifecycle:
            lifecycle_held.set()
            release_lifecycle.wait(timeout=0.5)

    holder = threading.Thread(target=hold_lifecycle)
    holder.start()
    assert lifecycle_held.wait(timeout=2)
    started = time.monotonic()
    try:
        with pytest.raises(ResponseTimeoutError, match="endpoint preflight"):
            transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"})
        assert time.monotonic() - started < 0.25
    finally:
        release_lifecycle.set()
        holder.join(timeout=2)

    assert not holder.is_alive()
    transport.close()


def test_pool_started_condition_wait_obeys_request_deadline(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=0.05, heartbeat_interval=None)
    pool._ensure_started()
    monkeypatch.setattr(pool._transports[0], "_execute_with_lease", lambda *_args, **_kwargs: 456)
    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_condition() -> None:
        with pool._condition:
            condition_held.set()
            release_condition.wait(timeout=0.5)

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)
    started = time.monotonic()
    try:
        with pytest.raises(ResponseTimeoutError):
            pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"})
        assert time.monotonic() - started < 0.25
    finally:
        release_condition.set()
        holder.join(timeout=2)

    assert not holder.is_alive()
    pool.close()


def test_socket_runtime_lifecycle_stage_obeys_request_deadline(monkeypatch) -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=0.05, heartbeat_interval=None)
    endpoints = socket_module.resolve_hosts(transport._hosts)
    runtime = ActorRuntime(1, endpoints)
    runtime.state = RuntimeState.RUNNING
    runtime.started.set()
    transport._resolved_endpoints = endpoints
    transport._runtime = runtime
    transport._push_buffer = PushBuffer(1)
    transport._epoch = 1
    first_preflight = threading.Event()
    holder_ready = threading.Event()
    release_holder = threading.Event()
    preflight_calls = 0
    real_preflight = transport._preflight_endpoints

    def controlled_preflight(deadline=None):
        nonlocal preflight_calls
        preflight_calls += 1
        if preflight_calls == 1:
            result = real_preflight(deadline)
            first_preflight.set()
            assert holder_ready.wait(timeout=2)
            return result
        return endpoints, transport._close_generation, deadline

    monkeypatch.setattr(transport, "_preflight_endpoints", controlled_preflight)
    monkeypatch.setattr(transport, "_execute_with_lease", lambda *_args, **_kwargs: 999)

    def hold_lifecycle() -> None:
        assert first_preflight.wait(timeout=2)
        with transport._lifecycle:
            holder_ready.set()
            assert release_holder.wait(timeout=0.5)

    holder = threading.Thread(target=hold_lifecycle)
    holder.start()
    started = time.monotonic()
    try:
        with pytest.raises(ResponseTimeoutError, match="Actor startup"):
            transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"})
        assert time.monotonic() - started < 0.25
    finally:
        release_holder.set()
        holder.join(timeout=2)

    assert not holder.is_alive()
    transport.close()


def test_socket_submission_gate_obeys_request_deadline(monkeypatch) -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=0.05, heartbeat_interval=None)
    endpoints = socket_module.resolve_hosts(transport._hosts)
    runtime = ActorRuntime(1, endpoints)
    runtime.state = RuntimeState.RUNNING
    runtime.started.set()
    transport._resolved_endpoints = endpoints
    transport._runtime = runtime
    transport._push_buffer = PushBuffer(1)
    transport._epoch = 1
    assert transport._submission_gate.acquire(timeout=1)
    started = time.monotonic()
    try:
        with pytest.raises(ResponseTimeoutError, match="request submission"):
            transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"})
        assert time.monotonic() - started < 0.25
        assert runtime.pending_task is None and runtime.request_id_counter == 0
        assert transport._request_lock.acquire(timeout=0.1)
        transport._request_lock.release()
    finally:
        transport._submission_gate.release()
    transport.close()


def test_actor_control_lock_submission_obeys_request_deadline() -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=0.05, heartbeat_interval=None)
    endpoints = socket_module.resolve_hosts(transport._hosts)
    runtime = ActorRuntime(1, endpoints)
    runtime.state = RuntimeState.RUNNING
    runtime.started.set()
    transport._resolved_endpoints = endpoints
    transport._runtime = runtime
    transport._push_buffer = PushBuffer(1)
    transport._epoch = 1
    control_held = threading.Event()
    release_control = threading.Event()

    def hold_control() -> None:
        with runtime.control_lock:
            control_held.set()
            release_control.wait(timeout=0.5)

    holder = threading.Thread(target=hold_control)
    holder.start()
    assert control_held.wait(timeout=2)
    started = time.monotonic()
    try:
        with pytest.raises(ResponseTimeoutError, match="Actor request submission"):
            transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"})
        assert time.monotonic() - started < 0.25
        assert runtime.pending_task is None and runtime.request_id_counter == 0
        assert transport._request_lock.acquire(timeout=0.1)
        transport._request_lock.release()
    finally:
        release_control.set()
        holder.join(timeout=2)

    assert not holder.is_alive()
    transport.close()


def test_unpublished_candidate_cleanup_failure_remains_owned_and_retryable(monkeypatch) -> None:
    buffers: list[PushBuffer] = []

    class RetryPushBuffer(PushBuffer):
        def __init__(self, owner_epoch, **kwargs) -> None:
            super().__init__(owner_epoch, **kwargs)
            self.allow_close = False

        def close(self, error=None) -> None:
            if not self.allow_close:
                raise RuntimeError("unpublished push cleanup injection")
            super().close(error)

    real_push = socket_module.PushBuffer

    def make_push(*args, **kwargs):
        push = RetryPushBuffer(*args, **kwargs)
        buffers.append(push)
        return push

    monkeypatch.setattr(socket_module, "PushBuffer", make_push)
    transport = SocketTransport(["127.0.0.1:9"], timeout=0.08, heartbeat_interval=None)
    callback_entered = threading.Event()
    holder_ready = threading.Event()
    release_holder = threading.Event()
    result: list[object] = []
    real_own = transport._own_candidate

    def gated_own(runtime, registration, **kwargs) -> None:
        callback_entered.set()
        assert holder_ready.wait(timeout=2)
        real_own(runtime, registration, **kwargs)

    monkeypatch.setattr(transport, "_own_candidate", gated_own)

    def hold_lifecycle() -> None:
        assert callback_entered.wait(timeout=2)
        with transport._lifecycle:
            holder_ready.set()
            release_holder.wait(timeout=0.5)

    holder = threading.Thread(target=hold_lifecycle)
    holder.start()
    starter = threading.Thread(
        target=lambda: _capture_result(lambda: transport._ensure_runtime(time.monotonic() + 0.08), result)
    )
    starter.start()
    assert callback_entered.wait(timeout=2)
    starter.join(timeout=0.3)
    try:
        assert not starter.is_alive()
        assert len(result) == 1 and isinstance(result[0], ActorStartupError)
        candidate = transport._unpublished_candidate
        assert candidate is not None and candidate is result[0].runtime
        assert isinstance(candidate.cleanup_error, RuntimeError)
        assert buffers and not buffers[0].snapshot().closed
    finally:
        release_holder.set()
        holder.join(timeout=2)

    with pytest.raises(TransportCloseTimeoutError, match="resource cleanup failed"):
        transport.close()
    assert transport._runtime is candidate or transport._unpublished_candidate is candidate

    buffers[0].allow_close = True
    transport.close()
    assert buffers[0].snapshot().closed
    assert transport._unpublished_candidate is None
    assert transport._runtime is candidate
    assert candidate.state is RuntimeState.FAILED_CLOSED
    assert candidate.cleanup_error is None
    monkeypatch.setattr(socket_module, "PushBuffer", real_push)


def test_close_owner_abort_stops_exact_unpublished_candidate(monkeypatch) -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    candidate = ActorRuntime(4, ())
    transport._unpublished_candidate = candidate
    real_notify = transport._lifecycle.notify_all
    calls = 0

    def fail_first_notify() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("close publication injection")
        real_notify()

    monkeypatch.setattr(transport._lifecycle, "notify_all", fail_first_notify)
    with pytest.raises(RuntimeError, match="close publication injection"):
        transport.close()

    assert candidate.stop_requested
    assert candidate.state is RuntimeState.FAILED_CLOSING
    assert transport._unpublished_candidate is candidate
    assert not transport._closing

    transport.close()
    assert candidate.state is RuntimeState.FAILED_CLOSED
    assert transport._runtime is candidate
    assert transport._unpublished_candidate is None


def test_pool_startup_attempt_survives_deadline_cleanup_locks(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=0.08, heartbeat_interval=None)
    slot = pool._transports[0]
    configured = threading.Event()
    allow_configure = threading.Event()
    result: list[object] = []
    original_configure = slot._configure_pool_runtime

    def gated_configure(**kwargs) -> None:
        original_configure(**kwargs)
        configured.set()
        assert allow_configure.wait(timeout=2)

    monkeypatch.setattr(slot, "_configure_pool_runtime", gated_configure)
    worker = threading.Thread(
        target=lambda: _capture_result(
            lambda: pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"}),
            result,
        )
    )
    worker.start()
    assert configured.wait(timeout=2)
    assert pool._condition.acquire(timeout=1)
    assert slot._submission_gate.acquire(timeout=1)
    allow_configure.set()
    try:
        worker.join(timeout=0.3)
        assert not worker.is_alive()
        assert len(result) == 1 and isinstance(result[0], BaseException)
        attempt = pool._startup_attempt
        assert attempt is not None
        assert attempt.broker is not None and attempt.push_buffer is not None
        assert len(attempt.configured) == 1
    finally:
        slot._submission_gate.release()
        pool._condition.release()
        worker.join(timeout=2)

    pool.close()
    assert pool._state is PoolState.FAILED_CLOSED
    assert pool._startup_attempt is None
    assert pool._runtime_guard._state is pool_module.GuardState.INACTIVE
    assert slot._fixed_runtime_epoch is None
    assert slot._shared_push_buffer is None
    assert slot._runtime_started_callback is None


def test_never_started_pool_close_timeout_retries_to_failed_closed() -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_condition() -> None:
        with pool._condition:
            condition_held.set()
            release_condition.wait(timeout=1.5)

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)
    try:
        with pytest.raises(TransportCloseTimeoutError, match="shutdown claim"):
            pool.close()
        assert pool._shutdown_failed.is_set()
    finally:
        release_condition.set()
        holder.join(timeout=2)

    pool.close()
    assert pool._state is PoolState.FAILED_CLOSED


def test_normal_pool_close_rotates_failure_event_before_stale_cleanup() -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    pool._ensure_started()
    old_failure = pool._shutdown_failed
    old_retire = pool._epoch_retire_event
    assert old_retire is not None
    pool.close()
    current_failure = pool._shutdown_failed

    old_failure.set()
    old_retire.set()
    new_broker, _ = pool._ensure_started()

    assert current_failure is not old_failure
    assert not current_failure.is_set()
    assert pool._state is PoolState.RUNNING
    assert pool._broker is new_broker
    pool.close()


def test_successful_connect_cannot_return_after_pool_close(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    wait_return_ready = threading.Event()
    allow_wait_return = threading.Event()
    result: list[object] = []
    real_wait = pool_module.wait
    wait_calls = 0

    monkeypatch.setattr(pool._transports[0], "_connect_with_deadline", lambda **_kwargs: None)

    def gated_wait(*args, **kwargs):
        nonlocal wait_calls
        value = real_wait(*args, **kwargs)
        wait_calls += 1
        if wait_calls == 1:
            wait_return_ready.set()
            assert allow_wait_return.wait(timeout=2)
        return value

    monkeypatch.setattr(pool_module, "wait", gated_wait)
    connector = threading.Thread(target=lambda: _capture_result(pool.connect, result))
    connector.start()
    assert wait_return_ready.wait(timeout=2)

    pool.close()
    assert pool._state is PoolState.STOPPED
    allow_wait_return.set()
    connector.join(timeout=2)

    assert not connector.is_alive()
    assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)


def test_connect_shutdown_claim_error_fails_exact_epoch_closed(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=0.2, heartbeat_interval=None)
    original_claim = pool._claim_shutdown_attempt

    def failed_connect(**_kwargs) -> None:
        raise ConnectionClosedError("connect failed")

    def fail_expected_claim(**kwargs):
        if kwargs.get("expected_broker") is not None:
            raise TransportCloseTimeoutError("claim injection")
        return original_claim(**kwargs)

    monkeypatch.setattr(pool._transports[0], "_connect_with_deadline", failed_connect)
    monkeypatch.setattr(pool, "_claim_shutdown_attempt", fail_expected_claim)

    with pytest.raises(TransportCloseTimeoutError, match="claim injection"):
        pool.connect()

    assert pool._shutdown_failed.is_set()
    assert pool._epoch_retire_event is not None and pool._epoch_retire_event.is_set()
    with pytest.raises(ConnectionClosedError, match="FAILED_CLOSING"):
        pool._ensure_started()
    pool.close()
    assert pool._state is PoolState.FAILED_CLOSED


def test_multislot_startup_attempt_cleans_post_config_failure(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=2, timeout=0.08, heartbeat_interval=None)
    second = pool._transports[1]
    configured = threading.Event()
    allow_failure = threading.Event()
    result: list[object] = []
    original_configure = second._configure_pool_runtime

    def fail_after_configure(**kwargs) -> None:
        original_configure(**kwargs)
        configured.set()
        assert allow_failure.wait(timeout=2)
        raise RuntimeError("post-config injection")

    monkeypatch.setattr(second, "_configure_pool_runtime", fail_after_configure)
    worker = threading.Thread(
        target=lambda: _capture_result(
            lambda: pool.execute(TYPE_SECURITY_COUNT, {"market": "sz"}),
            result,
        )
    )
    worker.start()
    assert configured.wait(timeout=2)
    assert second._submission_gate.acquire(timeout=1)
    allow_failure.set()
    worker.join(timeout=0.3)
    try:
        assert not worker.is_alive()
        assert len(result) == 1 and isinstance(result[0], RuntimeError)
        assert pool._startup_attempt is not None
        assert len(pool._startup_attempt.configured) == 2
    finally:
        second._submission_gate.release()
        worker.join(timeout=2)

    try:
        pool.close()
    except BaseException:
        pass
    pool.close()

    assert pool._state is PoolState.FAILED_CLOSED
    assert pool._startup_attempt is None
    assert pool._runtime_guard._state is pool_module.GuardState.INACTIVE
    for slot in pool._transports:
        assert slot._fixed_runtime_epoch is None
        assert slot._shared_push_buffer is None
        assert slot._runtime_started_callback is None


def test_stale_fatal_timeout_cannot_retire_reconfigured_guard() -> None:
    guard = PoolRuntimeGuard()
    old_broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    old_retire = threading.Event()
    guard.configure(old_broker, PushBuffer(1), retire_event=old_retire)
    old_handle = pool_module.ActorFatalHandle(
        weakref.ref(guard),
        1,
        weakref.ref(old_broker),
        old_retire,
    )
    guard.seal(pool_epoch=1, broker=old_broker)
    guard.finish_epoch(pool_epoch=1, broker=old_broker)

    new_broker = LeaseBroker(2, pool_size=1, max_pending_requests=1)
    new_retire = threading.Event()
    guard.configure(new_broker, PushBuffer(2), retire_event=new_retire)
    runtime = ActorRuntime(1, ())
    lock_held = threading.Event()
    release_lock = threading.Event()
    results: list[object] = []
    error = ResponseTimeoutError("stale fatal")
    error._eltdx_deadline = time.monotonic() + 0.05  # type: ignore[attr-defined]

    def hold_guard() -> None:
        with guard._lock:
            lock_held.set()
            release_lock.wait(timeout=0.5)

    holder = threading.Thread(target=hold_guard)
    holder.start()
    assert lock_held.wait(timeout=2)
    caller = threading.Thread(target=lambda: _capture_result(lambda: old_handle(runtime, error), results))
    caller.start()
    caller.join(timeout=0.2)
    try:
        assert not caller.is_alive()
        assert len(results) == 1 and isinstance(results[0], ResponseTimeoutError)
        assert old_retire.is_set()
        assert not new_retire.is_set()
    finally:
        release_lock.set()
        holder.join(timeout=2)
        caller.join(timeout=2)

    assert guard.is_active(pool_epoch=2, broker=new_broker)


def test_push_cleanup_error_does_not_skip_actor_close(monkeypatch) -> None:
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    push_buffer = PushBuffer(9)
    runtime = ActorRuntime(9, (), push_buffer=push_buffer)
    runtime.state = RuntimeState.STOPPED
    runtime.stopped.set()
    with transport._lifecycle:
        transport._runtime = runtime
        transport._push_buffer = push_buffer
    closed: list[ActorRuntime] = []

    def fail_push_cleanup(*_args, **_kwargs) -> None:
        raise RuntimeError("push cleanup publication injection")

    monkeypatch.setattr(socket_module, "_clear_resolved_push_cleanup", fail_push_cleanup)
    monkeypatch.setattr(socket_module, "close_actor", lambda item, timeout: closed.append(item))

    with pytest.raises(RuntimeError, match="push cleanup publication injection"):
        transport._close_with_timeout(0.1)

    assert closed == [runtime]


def test_pool_runtime_fatal_cleanup_is_best_effort_after_broker_deadline() -> None:
    broker = LeaseBroker(1, pool_size=2, max_pending_requests=2)
    push = PushBuffer(1)
    guard = PoolRuntimeGuard()
    guard.configure(broker, push)
    failed = ActorRuntime(1, ())
    sibling = ActorRuntime(1, ())
    assert guard.add_runtime(failed, pool_epoch=1, broker=broker)
    assert guard.add_runtime(sibling, pool_epoch=1, broker=broker)
    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_condition() -> None:
        with broker._condition:
            condition_held.set()
            release_condition.wait()

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)
    try:
        guard.fail(
            failed,
            RuntimeError("fatal"),
            pool_epoch=1,
            broker=broker,
            deadline=time.monotonic() + 0.03,
        )
    finally:
        release_condition.set()
        holder.join(timeout=2)

    assert push.snapshot().closed
    assert failed.stop_requested and sibling.stop_requested


def test_pool_runtime_abandon_attempts_every_cleanup_stage(monkeypatch) -> None:
    broker = LeaseBroker(1, pool_size=2, max_pending_requests=2)
    push = PushBuffer(1)
    guard = PoolRuntimeGuard()
    guard.configure(broker, push)
    first = ActorRuntime(1, ())
    second = ActorRuntime(1, ())
    assert guard.add_runtime(first, pool_epoch=1, broker=broker)
    assert guard.add_runtime(second, pool_epoch=1, broker=broker)
    original_stop = pool_module.request_actor_stop

    monkeypatch.setattr(broker, "close", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("broker close")))

    def stop(runtime, **kwargs) -> None:
        original_stop(runtime, **kwargs)
        if runtime is first:
            raise RuntimeError("first stop")

    monkeypatch.setattr(pool_module, "request_actor_stop", stop)
    guard.abandon()

    assert push.snapshot().closed
    assert first.stop_requested and second.stop_requested


def test_standalone_abandon_is_nonblocking_under_control_contention() -> None:
    runtime = actor_module.start_actor(901, ())
    assert runtime.control_lock.acquire(timeout=1)
    done = threading.Event()
    thread = threading.Thread(target=lambda: (actor_module.abandon_actor(runtime), done.set()))
    thread.start()
    try:
        assert done.wait(timeout=0.08)
        assert runtime.stop_requested
    finally:
        runtime.control_lock.release()
        thread.join(timeout=2)
    assert runtime.stopped.wait(timeout=2)
    assert runtime.actor_thread is not None and not runtime.actor_thread.is_alive()


def test_pool_abandon_is_nonblocking_under_broker_contention() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    push = PushBuffer(1)
    guard = PoolRuntimeGuard()
    guard.configure(broker, push)
    runtime = actor_module.start_actor(1, ())
    assert guard.add_runtime(runtime, pool_epoch=1, broker=broker)
    assert broker._condition.acquire(timeout=1)
    assert runtime.control_lock.acquire(timeout=1)
    done = threading.Event()
    thread = threading.Thread(target=lambda: (guard.abandon(), done.set()))
    thread.start()
    try:
        assert done.wait(timeout=0.08)
    finally:
        runtime.control_lock.release()
        broker._condition.release()
        thread.join(timeout=2)
    assert broker.snapshot().closed
    assert runtime.stop_requested
    assert runtime.stopped.wait(timeout=2)
    assert runtime.actor_thread is not None and not runtime.actor_thread.is_alive()


def test_pool_fatal_deadline_error_does_not_poison_actor_cleanup() -> None:
    broker = LeaseBroker(1, pool_size=1, max_pending_requests=1)
    push = PushBuffer(1)
    retire = threading.Event()
    guard = PoolRuntimeGuard()
    guard.configure(broker, push, retire_event=retire)
    handle = pool_module.ActorFatalHandle(weakref.ref(guard), 1, weakref.ref(broker), retire)
    runtime = ActorRuntime(1, (), fatal_callback=handle)
    error = RuntimeError("startup fatal")
    error._eltdx_deadline = time.monotonic() + 0.03  # type: ignore[attr-defined]
    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_condition() -> None:
        with broker._condition:
            condition_held.set()
            release_condition.wait()

    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)
    try:
        actor_module._fail_actor_startup(runtime, error)
    finally:
        release_condition.set()
        holder.join(timeout=2)

    assert runtime.cleanup_error is None
    assert runtime.stopped.is_set() and runtime.stop_requested
    assert push.snapshot().closed
    actor_module.close_actor(runtime)
    assert runtime.state is RuntimeState.FAILED_CLOSED


def test_standalone_close_interrupt_releases_submission_gate_owner() -> None:
    class InterruptAfterClaimGate(socket_module._RequestGate):
        def __init__(self) -> None:
            super().__init__()
            self.interrupted = False

        def acquire_token(self, token: object, deadline: float | None) -> bool:
            acquired = super().acquire_token(token, deadline)
            if acquired and not self.interrupted:
                self.interrupted = True
                raise KeyboardInterrupt
            return acquired

    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    gate = InterruptAfterClaimGate()
    transport._submission_gate = gate

    with pytest.raises(KeyboardInterrupt):
        transport.close()

    assert gate.acquire(blocking=False)
    gate.release()
    transport.close()
