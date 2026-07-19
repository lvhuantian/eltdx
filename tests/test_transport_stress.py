from __future__ import annotations

import threading
from contextlib import nullcontext

import scripts.stress_actor_transport as stress_module
import pytest
from scripts.stress_actor_transport import (
    run_close_samples,
    run_generation_stress,
    run_heartbeat_impact,
    run_idle_cpu_sample,
    run_mixed_stress,
    run_warmed_resource_stress,
)


def test_heartbeat_interval_publication_rebases_activity_before_enabling(monkeypatch) -> None:
    expected_now = 123.5

    class Generation:
        last_activity_at = 0.0

    class Runtime:
        def __init__(self) -> None:
            self.control_lock = nullcontext()
            self.generation = Generation()
            self._heartbeat_interval = None

        @property
        def heartbeat_interval(self):
            return self._heartbeat_interval

        @heartbeat_interval.setter
        def heartbeat_interval(self, value) -> None:
            if value is not None:
                assert self.generation.last_activity_at == expected_now
            self._heartbeat_interval = value

    class Transport:
        _runtime = Runtime()

    class Pool:
        _transports = [Transport()]

    monkeypatch.setattr(stress_module.time, "monotonic", lambda: expected_now)

    stress_module._set_heartbeat_pool_interval(Pool(), 0.02)


def test_heartbeat_interval_publication_holds_every_runtime_lock(monkeypatch) -> None:
    held = [False, False]

    class Lock:
        def __init__(self, index: int) -> None:
            self.index = index

        def __enter__(self) -> None:
            held[self.index] = True

        def __exit__(self, *_args) -> None:
            held[self.index] = False

    class Generation:
        last_activity_at = 0.0

    class Runtime:
        def __init__(self, index: int) -> None:
            self.control_lock = Lock(index)
            self.generation = Generation()
            self._heartbeat_interval = None

        @property
        def heartbeat_interval(self):
            return self._heartbeat_interval

        @heartbeat_interval.setter
        def heartbeat_interval(self, value) -> None:
            assert all(held)
            self._heartbeat_interval = value

    class Transport:
        def __init__(self, index: int) -> None:
            self._runtime = Runtime(index)

    class Pool:
        _transports = [Transport(0), Transport(1)]

    monkeypatch.setattr(stress_module.time, "monotonic", lambda: 50.0)

    stress_module._set_heartbeat_pool_interval(Pool(), 0.02)


def test_heartbeat_configuration_ack_runs_disabled_before_target_publication() -> None:
    class Decoder:
        buffered_bytes = 0

    class Generation:
        last_activity_at = 0.0
        generation_id = 1
        state = stress_module.TcpState.READY
        active_exchange = None
        tx_bytes = b""
        tx_offset = 0
        decoded_frames = ()
        receive_drained = True
        decoder = Decoder()

    class Runtime:
        control_lock = nullcontext()
        heartbeat_interval = 0.02
        generation = Generation()
        active_task = None
        pending_task = None
        cancel_requests = {}

    class Transport:
        _runtime = Runtime()

    class Pool:
        _transports = [Transport()]

        def connect(self) -> None:
            assert self._transports[0]._runtime.heartbeat_interval is None

    pool = Pool()

    assert stress_module._synchronize_heartbeat_pool(pool, 1, 0.02) == 1
    assert pool._transports[0]._runtime.heartbeat_interval == 0.02


def test_heartbeat_phase_counter_starts_before_target_publication(monkeypatch) -> None:
    class Server:
        heartbeat_requests = 0

    server = Server()

    class Runtime:
        control_lock = nullcontext()
        generation = None
        _heartbeat_interval = None

        @property
        def heartbeat_interval(self):
            return self._heartbeat_interval

        @heartbeat_interval.setter
        def heartbeat_interval(self, value) -> None:
            self._heartbeat_interval = value
            if value is not None:
                server.heartbeat_requests += 1

    class Transport:
        _runtime = Runtime()

    class Pool:
        _transports = [Transport()]

    heartbeat_before = stress_module._publish_heartbeat_phase(Pool(), server, 0.02)

    assert heartbeat_before == 0
    assert server.heartbeat_requests - heartbeat_before == 1


def test_heartbeat_after_final_business_response_is_outside_business_window() -> None:
    server = stress_module.StressServer()
    phase_id = "deterministic-post-response-heartbeat"
    server.begin_heartbeat_business_phase(
        phase_id,
        start_business_requests=0,
        target_business_requests=1,
    )

    sequence = server._record_business_request_started()
    heartbeat_done = threading.Event()

    with server._heartbeat_phase_wire_lock:
        heartbeat_thread = threading.Thread(
            target=lambda: (server._record_heartbeat_request(connection_id=1), heartbeat_done.set())
        )
        heartbeat_thread.start()
        assert not heartbeat_done.wait(timeout=0.05)
        server._record_business_request_finished(sequence, response_sent=True)

    heartbeat_thread.join(timeout=2)
    assert not heartbeat_thread.is_alive()

    phase = server.heartbeat_business_phase_snapshot(phase_id)
    assert server.heartbeat_requests == 1
    assert phase == {
        "phase_id": phase_id,
        "start_business_requests": 0,
        "target_business_requests": 1,
        "business_requests_started": 1,
        "business_responses_sent": 1,
        "business_window_open": False,
        "heartbeat_requests": 0,
    }


def test_failed_final_heartbeat_response_releases_active_business_count() -> None:
    server = stress_module.StressServer()
    phase_id = "deterministic-failed-final-response"
    server.begin_heartbeat_business_phase(
        phase_id,
        start_business_requests=0,
        target_business_requests=1,
    )

    sequence = server._record_business_request_started()
    with server._heartbeat_phase_wire_lock:
        server._record_business_request_finished(sequence, response_sent=False)

    phase = server.heartbeat_business_phase_snapshot(phase_id)
    assert server.active_business == 0
    assert phase["business_requests_started"] == 1
    assert phase["business_responses_sent"] == 0
    assert phase["business_window_open"] is True


def test_out_of_order_final_heartbeat_response_is_wire_fenced(monkeypatch) -> None:
    server = stress_module.StressServer()
    phase_id = "deterministic-out-of-order-final-response"
    server.begin_heartbeat_business_phase(
        phase_id,
        start_business_requests=0,
        target_business_requests=2,
    )

    first_started = threading.Event()
    release_first = threading.Event()
    release_first_send = threading.Event()
    first_progress = threading.Event()
    first_lock_attempted = threading.Event()
    lock_attempts: dict[str, threading.Event] = {}
    real_record_started = server._record_business_request_started

    def record_started() -> int:
        sequence = real_record_started()
        if sequence == 1:
            first_started.set()
            assert release_first.wait(timeout=2)
        return sequence

    server._record_business_request_started = record_started

    class TrackingLock:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self.track_attempts = False

        def __enter__(self):
            if self.track_attempts:
                name = threading.current_thread().name
                if name == "out-of-order-first-response":
                    first_lock_attempted.set()
                    first_progress.set()
                if name in lock_attempts:
                    lock_attempts[name].set()
            self._lock.acquire()
            return self

        def __exit__(self, *_args) -> None:
            self._lock.release()

    tracking_lock = TrackingLock()
    server._heartbeat_phase_wire_lock = tracking_lock

    class Connection:
        def __init__(self, *, track_progress: bool = False) -> None:
            self.read = False
            self.sent = threading.Event()
            self.track_progress = track_progress

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def sendall(self, _wire: bytes) -> None:
            self.sent.set()
            if self.track_progress:
                first_progress.set()
                assert release_first_send.wait(timeout=2)

    def read_request(connection: Connection):
        if connection.read:
            raise EOFError
        connection.read = True
        return 1, stress_module.TYPE_SECURITY_COUNT, b""

    monkeypatch.setattr(stress_module, "_read_request", read_request)
    first = Connection(track_progress=True)
    second = Connection()
    first_thread = threading.Thread(
        target=server._serve,
        args=(first, 1),
        name="out-of-order-first-response",
    )
    second_thread = threading.Thread(target=server._serve, args=(second, 2))

    first_thread.start()
    assert first_started.wait(timeout=2)
    second_thread.start()
    assert second.sent.wait(timeout=2)
    second_thread.join(timeout=2)
    assert not second_thread.is_alive()

    with tracking_lock:
        tracking_lock.track_attempts = True
        release_first.set()
        assert first_progress.wait(timeout=2)
        first_send_was_fenced = first_lock_attempted.is_set() and not first.sent.is_set()

    if not first_send_was_fenced:
        release_first_send.set()
        first_thread.join(timeout=2)
        assert first_send_was_fenced

    assert first.sent.wait(timeout=2)
    heartbeat_attempted = threading.Event()
    snapshot_attempted = threading.Event()
    heartbeat_done = threading.Event()
    snapshot_done = threading.Event()
    snapshot: list[dict[str, object]] = []
    lock_attempts["out-of-order-heartbeat"] = heartbeat_attempted
    lock_attempts["out-of-order-snapshot"] = snapshot_attempted
    heartbeat_thread = threading.Thread(
        target=lambda: (
            server._record_heartbeat_request(connection_id=1),
            heartbeat_done.set(),
        ),
        name="out-of-order-heartbeat",
    )
    snapshot_thread = threading.Thread(
        target=lambda: (
            snapshot.append(server.heartbeat_business_phase_snapshot(phase_id)),
            snapshot_done.set(),
        ),
        name="out-of-order-snapshot",
    )
    heartbeat_thread.start()
    snapshot_thread.start()
    assert heartbeat_attempted.wait(timeout=2)
    assert snapshot_attempted.wait(timeout=2)
    assert not heartbeat_done.is_set()
    assert not snapshot_done.is_set()

    release_first_send.set()
    first_thread.join(timeout=2)
    heartbeat_thread.join(timeout=2)
    snapshot_thread.join(timeout=2)
    assert not first_thread.is_alive()
    assert not heartbeat_thread.is_alive()
    assert not snapshot_thread.is_alive()
    assert heartbeat_done.is_set()
    assert snapshot_done.is_set()
    assert snapshot[0]["business_responses_sent"] == 2
    assert snapshot[0]["business_window_open"] is False
    phase = server.heartbeat_business_phase_snapshot(phase_id)
    assert phase["business_responses_sent"] == 2
    assert phase["business_window_open"] is False
    assert phase["heartbeat_requests"] == 0
    assert server.heartbeat_requests == 1


def test_failed_phase_response_cleans_active_count_through_real_serve(monkeypatch) -> None:
    server = stress_module.StressServer()
    phase_id = "deterministic-real-failed-response"
    server.begin_heartbeat_business_phase(
        phase_id,
        start_business_requests=0,
        target_business_requests=1,
    )

    class FailingConnection:
        def __init__(self) -> None:
            self.read = False

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def sendall(self, _wire: bytes) -> None:
            raise OSError("injected phase response failure")

    def read_request(connection: FailingConnection):
        if connection.read:
            raise EOFError
        connection.read = True
        return 1, stress_module.TYPE_SECURITY_COUNT, b""

    monkeypatch.setattr(stress_module, "_read_request", read_request)
    worker = threading.Thread(target=server._serve, args=(FailingConnection(), 1))
    worker.start()
    worker.join(timeout=2)
    assert not worker.is_alive()

    phase = server.heartbeat_business_phase_snapshot(phase_id)
    assert server.active_business == 0
    assert phase["business_requests_started"] == 1
    assert phase["business_responses_sent"] == 0
    assert phase["business_window_open"] is True


def test_heartbeat_timed_publication_runs_only_after_every_worker_is_ready(monkeypatch) -> None:
    worker_ready = 0
    publication_ready_counts: list[int] = []
    real_barrier = stress_module.threading.Barrier

    class TrackingBarrier:
        def __init__(self, parties, action=None) -> None:
            self._action = action

            def tracked_action() -> None:
                publication_ready_counts.append(worker_ready)
                if self._action is not None:
                    self._action()

            self._barrier = real_barrier(parties, action=tracked_action)

        def wait(self, timeout=None):
            nonlocal worker_ready
            if threading.current_thread().name.startswith("eltdx-heartbeat-test"):
                worker_ready += 1
            return self._barrier.wait(timeout)

    class Executor(stress_module.ThreadPoolExecutor):
        def __init__(self, *args, **kwargs) -> None:
            kwargs["thread_name_prefix"] = "eltdx-heartbeat-test"
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(stress_module.threading, "Barrier", TrackingBarrier)
    monkeypatch.setattr(stress_module, "ThreadPoolExecutor", Executor)

    impact = stress_module.run_heartbeat_impact(100, blocks=3)

    expected_workloads = 8 + impact["phases"] * 2
    assert len(publication_ready_counts) == expected_workloads
    assert publication_ready_counts == [5 * (index + 1) for index in range(expected_workloads)]
    assert all(
        sample["launch_boundary"]
        == "worker barrier action: phase, counter, interval, timer, release"
        for condition in ("without_heartbeat", "with_heartbeat")
        for sample in impact[condition]["samples"]
    )


def test_heartbeat_cleanup_cause_chain_cannot_cycle_on_reused_exception() -> None:
    primary = RuntimeError("shared primary")
    cleanup = RuntimeError("cleanup")
    cleanup.__cause__ = primary

    with pytest.raises(RuntimeError, match="shared primary") as raised:
        stress_module._raise_primary_with_cleanup(primary, cleanup)

    seen = set()
    current = raised.value
    while current is not None:
        assert id(current) not in seen
        seen.add(id(current))
        current = current.__cause__
    assert cleanup in (raised.value.__cause__, raised.value.__context__)


def test_heartbeat_cleanup_cause_chain_cannot_cycle_on_shared_descendant() -> None:
    shared = RuntimeError("shared descendant")
    previous = RuntimeError("previous")
    previous.__cause__ = shared
    primary = RuntimeError("primary")
    primary.__cause__ = previous
    cleanup = RuntimeError("cleanup")
    cleanup.__cause__ = shared

    with pytest.raises(RuntimeError, match="primary") as raised:
        stress_module._raise_primary_with_cleanup(primary, cleanup)

    seen = set()
    current = raised.value
    while current is not None:
        assert id(current) not in seen
        seen.add(id(current))
        current = current.__cause__
    assert raised.value.__cause__ is cleanup
    assert cleanup.__cause__ is shared
    assert shared.__cause__ is None


def test_heartbeat_cleanup_cause_chain_cannot_self_cycle_on_reused_error() -> None:
    shared = RuntimeError("shared phase and barrier failure")

    with pytest.raises(RuntimeError, match="shared phase and barrier failure") as raised:
        stress_module._raise_primary_with_cleanup(shared, shared)

    assert raised.value is shared
    assert raised.value.__cause__ is not shared


def test_heartbeat_quiescence_rejects_missing_generation(monkeypatch) -> None:
    class Runtime:
        control_lock = nullcontext()
        heartbeat_interval = None
        generation = None
        active_task = None
        pending_task = None
        cancel_requests = {}

    class Transport:
        _runtime = Runtime()

    class Pool:
        _transports = [Transport()]

        def connect(self) -> None:
            pass

    clock = [0.0]

    class AdvancingEvent:
        def wait(self, timeout) -> None:
            clock[0] += timeout

    monkeypatch.setattr(stress_module.threading, "Event", AdvancingEvent)
    monkeypatch.setattr(stress_module.time, "monotonic", lambda: clock[0])

    with pytest.raises(AssertionError, match="wire quiescence"):
        stress_module._synchronize_disabled_heartbeat_pool(Pool(), 1, timeout=0.002)


@pytest.mark.parametrize(
    "dirty_state",
    ["active_exchange", "tx_bytes", "tx_offset", "decoded_frames", "buffered_bytes", "receive_drained"],
)
def test_heartbeat_quiescence_waits_for_every_wire_and_receive_state(monkeypatch, dirty_state: str) -> None:
    class Decoder:
        buffered_bytes = 0

    class Generation:
        generation_id = 1
        state = stress_module.TcpState.READY
        active_exchange = None
        tx_bytes = b""
        tx_offset = 0
        decoded_frames = []
        receive_drained = True
        decoder = Decoder()

    generation = Generation()
    if dirty_state == "active_exchange":
        generation.active_exchange = object()
    elif dirty_state == "tx_bytes":
        generation.tx_bytes = b"pending"
    elif dirty_state == "tx_offset":
        generation.tx_offset = 1
    elif dirty_state == "decoded_frames":
        generation.decoded_frames = [object()]
    elif dirty_state == "buffered_bytes":
        generation.decoder.buffered_bytes = 1
    else:
        generation.receive_drained = False

    class Runtime:
        control_lock = nullcontext()
        heartbeat_interval = None
        active_task = None
        pending_task = None
        cancel_requests = {}

        def __init__(self) -> None:
            self.generation = generation

    class Transport:
        _runtime = Runtime()

    class Pool:
        _transports = [Transport()]

        def connect(self) -> None:
            pass

    waits = 0

    class ClearingEvent:
        def wait(self, _timeout) -> None:
            nonlocal waits
            waits += 1
            generation.active_exchange = None
            generation.tx_bytes = b""
            generation.tx_offset = 0
            generation.decoded_frames = []
            generation.decoder.buffered_bytes = 0
            generation.receive_drained = True

    monkeypatch.setattr(stress_module.threading, "Event", ClearingEvent)

    assert stress_module._synchronize_disabled_heartbeat_pool(Pool(), 1, timeout=0.1) == 1
    assert waits == 1


def test_heartbeat_barrier_closes_every_warmup_and_timed_phase(monkeypatch) -> None:
    completed = 0
    connect_points: list[int] = []
    slot_connect_points: list[tuple[int, int]] = []
    lock = threading.Lock()
    real_execute_unique = stress_module._execute_unique
    real_connect = stress_module.PooledSocketTransport.connect
    real_slot_connect = stress_module.SocketTransport._connect_with_deadline

    def tracked_execute_unique(transport, token):
        nonlocal completed
        result = real_execute_unique(transport, token)
        with lock:
            completed += 1
        return result

    def tracked_connect(pool) -> None:
        with lock:
            connect_points.append(completed)
        real_connect(pool)

    def tracked_slot_connect(slot, *args, **kwargs):
        result = real_slot_connect(slot, *args, **kwargs)
        with lock:
            slot_connect_points.append((id(slot), completed))
        return result

    monkeypatch.setattr(stress_module, "_execute_unique", tracked_execute_unique)
    monkeypatch.setattr(stress_module.PooledSocketTransport, "connect", tracked_connect)
    monkeypatch.setattr(stress_module.SocketTransport, "_connect_with_deadline", tracked_slot_connect)

    requests = 101
    blocks = 3
    impact = run_heartbeat_impact(requests, blocks=blocks)
    expected_connect_points = [0, 32]
    completed_at_barrier = 32
    for _ in range(impact["phases"]):
        completed_at_barrier += 100
        expected_connect_points.append(completed_at_barrier)
        completed_at_barrier += requests
        expected_connect_points.append(completed_at_barrier)

    assert connect_points == expected_connect_points
    expected_slot_ids = {slot_id for slot_id, point in slot_connect_points if point == 0}
    assert len(expected_slot_ids) == impact["pool_size"]
    for point in expected_connect_points:
        assert {slot_id for slot_id, completed_at in slot_connect_points if completed_at == point} == expected_slot_ids
    assert len(slot_connect_points) == len(expected_connect_points) * impact["pool_size"]
    assert impact["configuration_slot_barriers"] == impact["pool_size"] * (
        1 + impact["phases"] * 2
    )
    base_order = [
        "without_heartbeat",
        "with_heartbeat",
        "with_heartbeat",
        "without_heartbeat",
        "with_heartbeat",
        "without_heartbeat",
        "without_heartbeat",
        "with_heartbeat",
    ]
    assert impact["phase_schedule"] == [
        {
            "block": block,
            "phase": phase,
            "phase_id": f"block-{block}-phase-{phase}",
            "condition": condition,
        }
        for block in range(blocks)
        for phase, condition in enumerate(base_order if block % 2 == 0 else reversed(base_order))
    ]
    assert all(
        sample["generation_ids_before"] == sample["generation_ids_after"]
        and sample["accept_count_before"] == sample["accept_count_after"]
        for condition in ("without_heartbeat", "with_heartbeat")
        for sample in impact[condition]["samples"]
    )


def test_heartbeat_configuration_lock_and_quiescence_are_observed() -> None:
    class TrackingLock:
        def __init__(self, runtime) -> None:
            self.runtime = runtime
            self.held = False

        def __enter__(self) -> None:
            assert not self.held
            self.held = True
            if self.runtime.pool.connect_calls:
                self.runtime.snapshot_entries += 1
                if self.runtime.snapshot_entries >= 2:
                    self.runtime.active_task = None
                    self.runtime.pending_task = None
                    self.runtime.cancel_requests.clear()

        def __exit__(self, *_args) -> None:
            self.held = False

    class Generation:
        def __init__(self, runtime) -> None:
            self.runtime = runtime
            self._last_activity_at = 0.0
            self.generation_id = 1
            self.state = stress_module.TcpState.READY
            self.active_exchange = None
            self.tx_bytes = b""
            self.tx_offset = 0
            self.decoded_frames = ()
            self.receive_drained = True

            class Decoder:
                buffered_bytes = 0

            self.decoder = Decoder()

        @property
        def last_activity_at(self) -> float:
            return self._last_activity_at

        @last_activity_at.setter
        def last_activity_at(self, value: float) -> None:
            assert self.runtime.control_lock.held
            self._last_activity_at = value

    class Runtime:
        def __init__(self, state: str) -> None:
            self.pool = None
            self.snapshot_entries = 0
            self.active_task = object() if state == "active" else None
            self.pending_task = object() if state == "pending" else None
            self.cancel_requests = {1: object()} if state == "cancel" else {}
            self.control_lock = TrackingLock(self)
            self._heartbeat_interval = 1.0
            self.generation = Generation(self)

        @property
        def heartbeat_interval(self) -> float | None:
            return self._heartbeat_interval

        @heartbeat_interval.setter
        def heartbeat_interval(self, value: float | None) -> None:
            assert self.control_lock.held
            self._heartbeat_interval = value

    class Transport:
        def __init__(self, runtime) -> None:
            self._runtime = runtime

    class Pool:
        def __init__(self) -> None:
            self.connect_calls = 0
            runtimes = [Runtime("active"), Runtime("pending"), Runtime("cancel")]
            for runtime in runtimes:
                runtime.pool = self
            self._transports = [Transport(runtime) for runtime in runtimes]

        def connect(self) -> None:
            self.connect_calls += 1

    pool = Pool()

    assert stress_module._synchronize_disabled_heartbeat_pool(pool, 3, timeout=0.1) == 3
    assert pool.connect_calls == 1
    assert all(transport._runtime.snapshot_entries >= 2 for transport in pool._transports)
    assert all(transport._runtime.heartbeat_interval is None for transport in pool._transports)


def test_heartbeat_initial_connect_failure_preserves_primary_and_closes_pool(monkeypatch) -> None:
    instances = []

    class Transport:
        _runtime = None

    class FailingPool:
        def __init__(self, *_args, **_kwargs) -> None:
            self.closed = False
            self._transports = [Transport()]
            instances.append(self)

        def connect(self) -> None:
            raise RuntimeError("injected heartbeat pool connect failure")

        def close(self) -> None:
            self.closed = True
            raise RuntimeError("injected heartbeat pool close failure")

    monkeypatch.setattr(stress_module, "PooledSocketTransport", FailingPool)

    with pytest.raises(RuntimeError, match="injected heartbeat pool connect failure") as raised:
        stress_module.run_heartbeat_impact(100, blocks=3)

    assert len(instances) == 1
    assert instances[0].closed
    assert raised.value.__cause__ is not None
    assert "interval reset" in str(raised.value.__cause__)
    assert "pool close" in str(raised.value.__cause__)


def test_heartbeat_probe_failure_after_connect_preserves_primary_and_closes_pool(monkeypatch) -> None:
    instances = []

    class ProbeServer:
        host = "127.0.0.1:1"
        heartbeat_responses = 0
        heartbeat_responses_by_connection = {}

        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            pass

        def wait_for_heartbeat_response_connections(self, *_args, **_kwargs) -> bool:
            raise RuntimeError("injected heartbeat probe failure")

    class Transport:
        _runtime = None

    class ConnectedPool:
        def __init__(self, *_args, **_kwargs) -> None:
            self.closed = False
            self._transports = [Transport()]
            instances.append(self)

        def connect(self) -> None:
            pass

        def close(self) -> None:
            self.closed = True
            raise RuntimeError("injected heartbeat probe close failure")

    monkeypatch.setattr(stress_module, "StressServer", ProbeServer)
    monkeypatch.setattr(stress_module, "PooledSocketTransport", ConnectedPool)

    with pytest.raises(RuntimeError, match="injected heartbeat probe failure") as raised:
        stress_module.run_heartbeat_impact(100, blocks=3)

    assert len(instances) == 1
    assert instances[0].closed
    assert raised.value.__cause__ is not None
    assert "interval reset" in str(raised.value.__cause__)
    assert "pool close" in str(raised.value.__cause__)


@pytest.mark.parametrize("reuse_phase_and_barrier", [False, True])
def test_heartbeat_timed_failure_preserves_primary_when_phase_barrier_also_fails(
    monkeypatch,
    reuse_phase_and_barrier: bool,
) -> None:
    failure_state = {"armed": False, "reset_failures": 0}
    shared_phase_error = RuntimeError("injected shared timed phase failure")

    class Generation:
        last_activity_at = 0.0
        generation_id = 1
        state = stress_module.TcpState.READY
        active_exchange = None
        tx_bytes = b""
        tx_offset = 0
        decoded_frames = ()
        receive_drained = True

        class Decoder:
            buffered_bytes = 0

        decoder = Decoder()

    class Runtime:
        def __init__(self) -> None:
            self.control_lock = nullcontext()
            self._heartbeat_interval = None
            self.generation = Generation()
            self.active_task = None
            self.pending_task = None
            self.cancel_requests = {}

        @property
        def heartbeat_interval(self):
            return self._heartbeat_interval

        @heartbeat_interval.setter
        def heartbeat_interval(self, value) -> None:
            if value is None and failure_state["armed"] and not reuse_phase_and_barrier:
                failure_state["reset_failures"] += 1
                raise RuntimeError(f"injected interval reset failure {failure_state['reset_failures']}")
            self._heartbeat_interval = value

    class Transport:
        _runtime = Runtime()

    class Server:
        host = "127.0.0.1:1"
        accept_count = 4
        heartbeat_responses = 4
        heartbeat_responses_by_connection = {1: 1, 2: 1, 3: 1, 4: 1}
        heartbeat_requests = 0
        heartbeat_during_business = 0
        business_requests = 0

        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            pass

        def wait_for_heartbeat_response_connections(self, *_args, **_kwargs) -> bool:
            return True

        def heartbeat_response_snapshot(self):
            return {}

        def wait_for_heartbeat_response_round(self, *_args, **_kwargs) -> bool:
            return True

        def begin_heartbeat_business_phase(self, *_args, **_kwargs) -> None:
            pass

    class Pool:
        def __init__(self, *_args, **_kwargs) -> None:
            self._transports = [Transport() for _ in range(4)]
            self.connect_calls = 0

        def connect(self) -> None:
            self.connect_calls += 1
            if self.connect_calls == 4:
                failure_state["armed"] = True
                if reuse_phase_and_barrier:
                    raise shared_phase_error
                raise RuntimeError("injected timed phase barrier failure")

        def close(self) -> None:
            pass

    calls = 0

    def execute(_pool, token):
        nonlocal calls
        calls += 1
        if calls > 32 + 100:
            if reuse_phase_and_barrier:
                raise shared_phase_error
            raise RuntimeError("injected timed business failure")
        return {"requested_token": token}

    monkeypatch.setattr(stress_module, "StressServer", Server)
    monkeypatch.setattr(stress_module, "PooledSocketTransport", Pool)
    monkeypatch.setattr(stress_module, "_execute_unique", execute)

    expected = "injected shared timed phase failure" if reuse_phase_and_barrier else "injected timed business failure"
    with pytest.raises(RuntimeError, match=expected) as raised:
        stress_module.run_heartbeat_impact(100, blocks=3)

    causes = []
    seen = {id(raised.value)}
    cause = raised.value.__cause__
    while cause is not None:
        assert id(cause) not in seen
        seen.add(id(cause))
        causes.append(str(cause))
        cause = cause.__cause__
    if reuse_phase_and_barrier:
        assert raised.value is shared_phase_error
    else:
        assert any("injected timed phase barrier failure" in item for item in causes)
        assert any("injected interval reset failure" in item for item in causes)


def test_one_thousand_generation_changes_keep_one_actor_and_no_resources() -> None:
    result = run_generation_stress(1000)

    assert result["generation_counter"] >= 1000
    assert sum(result["server_accepts"]) >= 1000
    assert all(count > 0 for count in result["server_requests"])
    assert result["servers_used"] == 2
    assert result["ledger"]["retried_requests"] > 0
    assert result["ledger"]["cross_endpoint_retried_requests"] == result["ledger"]["retried_requests"]
    assert result["ledger"]["same_endpoint_retried_requests"] == 0
    assert result["unique_responses"] == 1000
    assert result["duplicate_responses"] == 0
    assert result["missing_responses"] == 0
    assert result["unexpected_responses"] == 0
    assert result["cross_request_completions"] == 0
    assert result["cross_generation_completions"] == 0
    assert result["stale_events"] == 0
    assert result["cleanup"]["saved_selector_present"] is True
    assert result["cleanup"]["saved_wake_reader_present"] is True
    assert result["cleanup"]["saved_wake_writer_present"] is True
    assert result["cleanup"]["saved_generation_present"] is True
    assert result["cleanup"]["saved_tcp_present"] is True
    assert result["cleanup"]["all_owned_resources_closed"] is True
    assert result["actor_threads_after"] == result["actor_threads_before"]


def test_bounded_mixed_stress_has_exact_pool_concurrency_and_clean_close() -> None:
    result = run_mixed_stress(
        2000,
        pool_size=4,
        concurrency=20,
        push_every=37,
        close_every=500,
        response_delay=0.001,
    )

    assert sum(result["server_requests"]) >= 2000
    assert all(count > 0 for count in result["server_requests"])
    assert result["servers_used"] == 2
    assert result["server_max_active"] == 4
    assert result["unique_responses"] == 2000
    assert result["duplicate_responses"] == 0
    assert result["missing_responses"] == 0
    assert result["unexpected_responses"] == 0
    assert result["cross_request_completions"] == 0
    assert result["cross_generation_completions"] == 0
    assert result["stale_events"] == 0
    assert result["ledger"]["retried_requests"] > 0
    assert result["ledger"]["cross_endpoint_retried_requests"] == result["ledger"]["retried_requests"]
    assert result["ledger"]["same_endpoint_retried_requests"] == 0
    assert result["broker_after_close"] == {
        "idle_slots": 0,
        "waiters": 0,
        "pin_waiters": 0,
        "leases": 0,
        "closed": True,
    }
    assert result["push_after_close"]["frames"] == 0
    assert result["push_after_close"]["bytes"] == 0
    assert result["push_after_close"]["closed"] is True
    assert result["push_after_close"]["configured_max_frames"] == 1024
    assert result["push_after_close"]["configured_max_bytes"] == 8 * 1024 * 1024
    assert result["push_after_close"]["max_frames_observed"] <= 1024
    assert result["push_after_close"]["max_bytes_observed"] <= 8 * 1024 * 1024
    assert result["push_after_close"]["dropped_total"] == result["push_dropped"]
    assert all(item["saved_selector_present"] for item in result["cleanup"])
    assert all(item["saved_wake_reader_present"] for item in result["cleanup"])
    assert all(item["saved_wake_writer_present"] for item in result["cleanup"])
    assert all(item["saved_generation_present"] for item in result["cleanup"])
    assert all(item["saved_tcp_present"] for item in result["cleanup"])
    assert all(item["all_owned_resources_closed"] for item in result["cleanup"])
    assert result["actor_threads_after"] == 0


def test_warmed_resource_rounds_reach_an_exact_plateau() -> None:
    result = run_warmed_resource_stress(warmup_rounds=2, measured_rounds=5, generations_per_round=20)

    assert result["resource_counter_supported"] is True
    assert result["exact_plateau"] is True
    assert result["monotonic_growth"] is False
    assert result["all_actor_threads_closed"] is True
    assert result["all_owned_resources_closed"] is True
    assert result["cross_request_completions"] == 0
    assert result["cross_generation_completions"] == 0


def test_close_latency_is_bounded_idle_and_under_load() -> None:
    idle = run_close_samples(10, pool_size=2)
    loaded = run_close_samples(10, pool_size=2, loaded=True)

    assert idle["p99_ms"] < 100
    assert loaded["p99_ms"] < 250
    assert idle["all_futures_terminal_within_timeout"] is True
    assert loaded["all_futures_terminal_within_timeout"] is True
    assert idle["all_tickets_terminal_at_close"] is True
    assert loaded["all_tickets_terminal_at_close"] is True
    assert idle["all_owned_resources_closed"] is True
    assert loaded["all_owned_resources_closed"] is True
    assert loaded["completed_results"] + sum(loaded["expected_request_errors"].values()) == 20


def test_idle_actor_blocks_and_heartbeat_defers_under_continuous_work() -> None:
    idle = run_idle_cpu_sample(0.2)
    impact = run_heartbeat_impact(4000)

    assert idle["cpu_ratio"] < 0.1
    assert impact["requests_per_phase"] == 4000
    assert impact["blocks"] == 4
    assert impact["phases"] == 32
    assert impact["idle_probe_heartbeats"] >= 4
    assert impact["idle_probe_connections"] == 4
    assert impact["paced_heartbeat_requests"] >= 32
    assert impact["paced_business_requests"] == 32
    assert impact["configuration_slot_barriers"] == impact["pool_size"] * (1 + impact["phases"] * 2)
    assert impact["heartbeat_during_business"] == 0
    assert impact["unique_responses"] == impact["business_requests"]
    assert impact["duplicate_responses"] == 0
    assert impact["missing_responses"] == 0
    assert impact["unexpected_responses"] == 0
    assert impact["cross_request_completions"] == 0
    assert impact["cross_generation_completions"] == 0
    assert impact["without_heartbeat"]["business_window_heartbeat_requests_total"] == 0
    assert impact["with_heartbeat"]["business_window_heartbeat_requests_total"] == 0
    assert all(
        sample["target_business_requests"] == impact["requests_per_phase"]
        and sample["business_window_heartbeat_requests"] == 0
        for condition in ("without_heartbeat", "with_heartbeat")
        for sample in impact[condition]["samples"]
    )
    expected_phase_positions = [phase for phase in range(8) for _ in range(2)]
    assert sorted(sample["phase"] for sample in impact["without_heartbeat"]["samples"]) == expected_phase_positions
    assert sorted(sample["phase"] for sample in impact["with_heartbeat"]["samples"]) == expected_phase_positions
    assert impact["throughput_estimator"] == "aggregate_elapsed_ratio"
    assert all(
        sample["generation_ids_before"] == sample["generation_ids_after"]
        and sample["accept_count_before"] == sample["accept_count_after"]
        for condition in ("without_heartbeat", "with_heartbeat")
        for sample in impact[condition]["samples"]
    )
    assert impact["median_block_throughput_ratio"] > 0
    assert impact["throughput_ratio"] > 0.99, {
        "throughput_ratio": impact["throughput_ratio"],
        "block_throughput_ratios": impact["block_throughput_ratios"],
        "without_heartbeat_seconds": [
            sample["seconds"] for sample in impact["without_heartbeat"]["samples"]
        ],
        "with_heartbeat_seconds": [
            sample["seconds"] for sample in impact["with_heartbeat"]["samples"]
        ],
        "without_heartbeat_total_requests": impact["without_heartbeat"]["heartbeat_requests_total"],
        "with_heartbeat_total_requests": impact["with_heartbeat"]["heartbeat_requests_total"],
        "without_heartbeat_business_window": impact["without_heartbeat"][
            "business_window_heartbeat_requests_total"
        ],
        "with_heartbeat_business_window": impact["with_heartbeat"][
            "business_window_heartbeat_requests_total"
        ],
    }
