from __future__ import annotations

import gc
import selectors
import socket
import threading
import time
import weakref

import pytest

from actor_support import Scripted7709Server, handshake_payload, read_request, response_bytes
from eltdx.exceptions import ConnectionClosedError, TransportCloseTimeoutError
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
            lambda: (stop_seen.set(), release_slow.set()),
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

    def observing_stop(runtime: ActorRuntime) -> None:
        assert runtime is slow._candidate
        real_request_stop(runtime)
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


def test_normal_pool_close_clears_epoch_configuration_and_reopens() -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=2, timeout=1, heartbeat_interval=None)
    old_broker, old_push = pool._ensure_started()
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

    del old_broker, old_push
    gc.collect()
    assert broker_ref() is None
    assert push_ref() is None

    new_broker, new_push = pool._ensure_started()
    try:
        assert new_broker.pool_epoch > 1
        assert new_push.owner_epoch == new_broker.pool_epoch
    finally:
        pool.close()


def test_shutdown_owner_exception_publishes_failure_and_allows_retry(monkeypatch) -> None:
    pool = PooledSocketTransport(["127.0.0.1:9"], pool_size=1, timeout=1, heartbeat_interval=None)
    pool._ensure_started()
    slot = pool._transports[0]
    real_request_stop = slot._request_stop
    stop_calls = 0

    def fail_first_stop() -> None:
        nonlocal stop_calls
        stop_calls += 1
        if stop_calls == 1:
            raise RuntimeError("stop injection")
        real_request_stop()

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
        thread.start()
        assert future_entered.wait(timeout=2)
        pool.close()
        assert pool._state is PoolState.STOPPED
        allow_future.set()
        thread.join(timeout=2)

        assert not thread.is_alive()
        assert len(result) == 1 and isinstance(result[0], ConnectionClosedError)
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

    def gated_begin(broker, push_buffer) -> None:
        original_begin(broker, push_buffer)
        rollback_entered.set()
        assert allow_rollback.wait(timeout=2)

    def observed_attempt_wait(attempt) -> None:
        close_waiting.set()
        original_wait_attempt(attempt)

    monkeypatch.setattr(pool._transports[0], "_connect_with_deadline", slow_connect)
    monkeypatch.setattr(pool._transports[1], "_connect_with_deadline", failed_connect)
    for slot in pool._transports:
        monkeypatch.setattr(slot, "_request_stop", release_slow.set)
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

    def gated_active(expected_runtime_epoch=None):
        ident = threading.get_ident()
        active_calls[ident] = active_calls.get(ident, 0) + 1
        if threading.current_thread().name == "old-startup-waiter" and active_calls[ident] >= 2:
            waiter_gated.set()
            assert allow_waiter.wait(timeout=2)
        return original_active(expected_runtime_epoch)

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

    def fail_first_retire(registration) -> bool:
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

    def fail_first_retire(registration) -> bool:
        nonlocal retire_calls
        retire_calls += 1
        if retire_calls == 1:
            raise RuntimeError("publish cleanup injection")
        return original_retire(registration)

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

    def gated_stop(item: ActorRuntime) -> None:
        stop_entered.set()
        assert allow_stop.wait(timeout=2)
        real_stop(item)

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
