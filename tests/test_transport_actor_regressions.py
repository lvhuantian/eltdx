from __future__ import annotations

import threading
import time

import pytest

from actor_support import Scripted7709Server, handshake_payload, read_request, response_bytes
from eltdx.exceptions import ProtocolError, ResponseTimeoutError, UnsupportedCommandError
from eltdx.hosts import resolve_hosts
from eltdx.protocol.commands import parse_command_response
from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_SECURITY_COUNT, TYPE_SECURITY_LIST
from eltdx.protocol.frame import decode_response
from eltdx.transport.actor import (
    ConnectTicket,
    FrameEnvelope,
    RuntimeState,
    cancel_ticket,
    close_actor,
    start_actor,
    submit_request,
    wait_ticket,
)
from eltdx.transport import actor as actor_module
from eltdx.transport.push import PushBuffer
from eltdx.transport.socket import SocketTransport


def _count(envelope: FrameEnvelope) -> int:
    return parse_command_response(envelope.command, envelope.response, envelope.request_payload_snapshot)


def test_old_decoded_batch_cannot_complete_next_request_after_64_frame_budget() -> None:
    b_received = threading.Event()
    release_server = threading.Event()
    b_created = threading.Event()
    b_tickets = []

    def handler(conn) -> None:
        msg_id, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))

        a_msg_id, msg_type, _ = read_request(conn)
        b_msg_id = a_msg_id + 1
        batch = [response_bytes(a_msg_id, msg_type, (111).to_bytes(2, "little"))]
        batch.extend(
            response_bytes(10_000 + index, msg_type, index.to_bytes(2, "little"))
            for index in range(63)
        )
        batch.append(response_bytes(b_msg_id, msg_type, (999).to_bytes(2, "little")))
        conn.sendall(b"".join(batch))

        observed_msg_id, observed_type, _ = read_request(conn)
        assert (observed_msg_id, observed_type) == (b_msg_id, msg_type)
        b_received.set()
        assert release_server.wait(timeout=2)

    push_buffer = PushBuffer(101, max_frames=128)
    with Scripted7709Server([handler]) as server:
        runtime = start_actor(101, resolve_hosts([server.host]), push_buffer=push_buffer)

        def submit_b(_ticket) -> None:
            b_tickets.append(
                submit_request(
                    runtime,
                    lease_id=0,
                    command=TYPE_SECURITY_COUNT,
                    payload={"market": "sz"},
                    deadline=time.monotonic() + 0.5,
                    retry_safe=False,
                )
            )
            b_created.set()

        try:
            a_ticket = submit_request(
                runtime,
                lease_id=0,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 2,
                retry_safe=False,
                completion=submit_b,
            )
            assert _count(wait_ticket(a_ticket)) == 111
            assert b_created.wait(timeout=2)
            assert b_received.wait(timeout=2)
            with pytest.raises(ResponseTimeoutError, match="timed out"):
                wait_ticket(b_tickets[0])

            pushed_values = [int.from_bytes(item.response.data[:2], "little") for item in push_buffer.drain()]
            assert 999 in pushed_values
            assert runtime.state is RuntimeState.RUNNING
        finally:
            release_server.set()
            close_actor(runtime)


def test_handshake_batch_tail_cannot_complete_business_exchange() -> None:
    request_received = threading.Event()
    release_server = threading.Event()

    def handler(conn) -> None:
        handshake_id, handshake_type, _ = read_request(conn)
        assert handshake_type == TYPE_HANDSHAKE
        business_id = handshake_id + 1
        conn.sendall(
            response_bytes(handshake_id, handshake_type, handshake_payload())
            + response_bytes(business_id, TYPE_SECURITY_COUNT, (999).to_bytes(2, "little"))
        )
        observed_id, observed_type, _ = read_request(conn)
        assert (observed_id, observed_type) == (business_id, TYPE_SECURITY_COUNT)
        request_received.set()
        assert release_server.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        runtime = start_actor(102, resolve_hosts([server.host]))
        try:
            ticket = submit_request(
                runtime,
                lease_id=0,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 0.5,
                retry_safe=False,
            )
            assert request_received.wait(timeout=2)
            with pytest.raises(ResponseTimeoutError, match="timed out"):
                wait_ticket(ticket)
            assert runtime.state is RuntimeState.RUNNING
        finally:
            release_server.set()
            close_actor(runtime)


def test_partial_handshake_batch_tail_is_classified_before_business(monkeypatch) -> None:
    partial_seen = threading.Event()
    send_tail = threading.Event()
    request_received = threading.Event()
    release_server = threading.Event()
    original_feed = actor_module.ResponseFrameDecoder.feed

    def observed_feed(decoder, data):
        frames = original_feed(decoder, data)
        if decoder.buffered_bytes:
            partial_seen.set()
        return frames

    monkeypatch.setattr(actor_module.ResponseFrameDecoder, "feed", observed_feed)

    def handler(conn) -> None:
        handshake_id, handshake_type, _ = read_request(conn)
        business_id = handshake_id + 1
        collision = response_bytes(business_id, TYPE_SECURITY_COUNT, (999).to_bytes(2, "little"))
        conn.sendall(response_bytes(handshake_id, handshake_type, handshake_payload()) + collision[:8])
        assert send_tail.wait(timeout=2)
        conn.sendall(collision[8:])
        observed_id, observed_type, _ = read_request(conn)
        assert (observed_id, observed_type) == (business_id, TYPE_SECURITY_COUNT)
        request_received.set()
        assert release_server.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        runtime = start_actor(105, resolve_hosts([server.host]))
        try:
            ticket = submit_request(
                runtime,
                lease_id=0,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 0.6,
                retry_safe=False,
            )
            assert partial_seen.wait(timeout=2)
            send_tail.set()
            assert request_received.wait(timeout=2)
            with pytest.raises(ResponseTimeoutError, match="timed out"):
                wait_ticket(ticket)
            assert runtime.state is RuntimeState.RUNNING
        finally:
            send_tail.set()
            release_server.set()
            close_actor(runtime)


def test_unsolicited_handshake_before_send_cannot_complete_handshake(monkeypatch) -> None:
    handshake_received = threading.Event()
    business_seen = threading.Event()
    probe_done = threading.Event()
    release_server = threading.Event()
    finish_entered = threading.Event()
    unsolicited_sent = threading.Event()
    allow_finish = threading.Event()
    original_finish = actor_module._finish_connect

    def controlled_finish(runtime, generation) -> None:
        finish_entered.set()
        assert allow_finish.wait(timeout=2)
        original_finish(runtime, generation)

    monkeypatch.setattr(actor_module, "_finish_connect", controlled_finish)

    def handler(conn) -> None:
        conn.sendall(response_bytes(1, TYPE_HANDSHAKE, handshake_payload()))
        unsolicited_sent.set()
        _, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        handshake_received.set()
        conn.settimeout(0.1)
        try:
            if conn.recv(1):
                business_seen.set()
        except TimeoutError:
            pass
        finally:
            conn.settimeout(2)
            probe_done.set()
        assert release_server.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        runtime = start_actor(106, resolve_hosts([server.host]))
        try:
            ticket = submit_request(
                runtime,
                lease_id=0,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 0.4,
                retry_safe=False,
            )
            assert finish_entered.wait(timeout=2)
            assert unsolicited_sent.wait(timeout=2)
            allow_finish.set()
            assert handshake_received.wait(timeout=2)
            assert probe_done.wait(timeout=2)
            with pytest.raises(ResponseTimeoutError, match="timed out"):
                wait_ticket(ticket)
            assert ticket.completed.wait(timeout=1)
            assert not business_seen.is_set()
            assert runtime.state is RuntimeState.RUNNING
        finally:
            allow_finish.set()
            release_server.set()
            close_actor(runtime)


def test_decoded_backlog_then_eof_reconnects_instead_of_failing_actor() -> None:
    b_created = threading.Event()
    b_tickets = []
    release_server = threading.Event()

    def first(conn) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        a_msg_id, msg_type, _ = read_request(conn)
        batch = [response_bytes(a_msg_id, msg_type, (111).to_bytes(2, "little"))]
        batch.extend(
            response_bytes(20_000 + index, msg_type, index.to_bytes(2, "little"))
            for index in range(64)
        )
        conn.sendall(b"".join(batch))

    def second(conn) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, (222).to_bytes(2, "little")))
        assert release_server.wait(timeout=2)

    with Scripted7709Server([first, second]) as server:
        runtime = start_actor(104, resolve_hosts([server.host]))

        def submit_b(_ticket) -> None:
            b_tickets.append(
                submit_request(
                    runtime,
                    lease_id=0,
                    command=TYPE_SECURITY_COUNT,
                    payload={"market": "sz"},
                    deadline=time.monotonic() + 2,
                    retry_safe=True,
                )
            )
            b_created.set()

        try:
            a_ticket = submit_request(
                runtime,
                lease_id=0,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 2,
                retry_safe=False,
                completion=submit_b,
            )
            assert _count(wait_ticket(a_ticket)) == 111
            assert b_created.wait(timeout=2)
            assert _count(wait_ticket(b_tickets[0])) == 222
            assert runtime.state is RuntimeState.RUNNING
            assert runtime.generation_counter == 2
        finally:
            release_server.set()
            close_actor(runtime)


@pytest.mark.parametrize(("rx_sequence", "rx_boundary", "tx_offset"), [(5, 5, 3), (6, 5, 2)])
def test_response_requires_new_receive_identity_and_complete_send(
    rx_sequence: int,
    rx_boundary: int,
    tx_offset: int,
) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    ticket = actor_module.RequestTicket(
        1,
        1,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        True,
        request_id=1,
    )
    raw = response_bytes(7, TYPE_SECURITY_COUNT, (999).to_bytes(2, "little"))
    generation = actor_module.TcpGeneration(1, object(), endpoint, actor_module.TcpState.READY)
    generation.tx_bytes = b"abc"
    generation.tx_offset = tx_offset
    generation.active_exchange = actor_module.WireExchange(
        ticket,
        TYPE_SECURITY_COUNT,
        7,
        TYPE_SECURITY_COUNT,
        b"abc",
        False,
        rx_boundary=rx_boundary,
    )
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.active_task = ticket
    runtime.generation = generation

    actor_module._route_frame(
        runtime,
        generation,
        actor_module.ReceivedFrame(rx_sequence, decode_response(raw)),
    )

    assert not ticket.completed.is_set()
    assert runtime.active_task is ticket
    assert runtime.stale_event_count == 1


def test_matching_response_after_absolute_deadline_cannot_succeed() -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    sock, peer = actor_module.socket.socketpair()
    ticket = actor_module.RequestTicket(
        1,
        1,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() - 1,
        False,
        request_id=1,
    )
    ticket.attempts = 1
    ticket.state = actor_module.RequestState.WAITING_RESPONSE
    raw = response_bytes(7, TYPE_SECURITY_COUNT, (999).to_bytes(2, "little"))
    generation = actor_module.TcpGeneration(1, sock, endpoint, actor_module.TcpState.READY)
    generation.tx_bytes = b"abc"
    generation.tx_offset = 3
    generation.active_exchange = actor_module.WireExchange(
        ticket,
        TYPE_SECURITY_COUNT,
        7,
        TYPE_SECURITY_COUNT,
        b"abc",
        False,
    )
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.active_task = ticket
    runtime.generation = generation
    try:
        actor_module._route_frame(
            runtime,
            generation,
            actor_module.ReceivedFrame(1, decode_response(raw)),
        )
        assert ticket.state is actor_module.RequestState.FAILED
        assert isinstance(ticket.error, ResponseTimeoutError)
        assert ticket.result is None
        assert runtime.generation is None
    finally:
        peer.close()


class WouldBlockSocket:
    def recv(self, _size: int) -> bytes:
        raise BlockingIOError


def test_heartbeat_uses_receive_quiescence_gate() -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    generation = actor_module.TcpGeneration(1, WouldBlockSocket(), endpoint, actor_module.TcpState.READY)
    generation.decoder.feed(b"\xb1")
    generation.receive_drained = True
    generation.last_activity_at = 0
    runtime = actor_module.ActorRuntime(1, (endpoint,), heartbeat_interval=1, request_timeout=1)
    runtime.generation = generation

    actor_module._schedule_heartbeat(runtime)

    assert isinstance(runtime.active_task, actor_module.RequestTicket)
    assert runtime.active_task.internal
    assert generation.active_exchange is None


def test_completion_callback_exception_isolated_from_actor_runtime() -> None:
    release = threading.Event()

    def handler(conn) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, (333).to_bytes(2, "little")))
        assert release.wait(timeout=2)

    def fail_completion(_ticket) -> None:
        raise ValueError("completion failed")

    with Scripted7709Server([handler]) as server:
        runtime = start_actor(107, resolve_hosts([server.host]))
        try:
            ticket = submit_request(
                runtime,
                lease_id=0,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 1,
                retry_safe=False,
                completion=fail_completion,
            )
            assert _count(wait_ticket(ticket)) == 333
            assert isinstance(ticket.completion_error, ValueError)
            assert runtime.state is RuntimeState.RUNNING
        finally:
            release.set()
            close_actor(runtime)


def test_late_cancel_of_completed_ticket_does_not_cancel_next_lease_zero_request() -> None:
    b_received = threading.Event()
    respond_to_b = threading.Event()
    release_server = threading.Event()

    def handler(conn) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))

        a_msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(a_msg_id, msg_type, (111).to_bytes(2, "little")))

        b_msg_id, msg_type, _ = read_request(conn)
        b_received.set()
        assert respond_to_b.wait(timeout=2)
        conn.sendall(response_bytes(b_msg_id, msg_type, (222).to_bytes(2, "little")))
        assert release_server.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        runtime = start_actor(103, resolve_hosts([server.host]))
        try:
            a_ticket = submit_request(
                runtime,
                lease_id=0,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 2,
                retry_safe=False,
            )
            assert _count(wait_ticket(a_ticket)) == 111
            b_ticket = submit_request(
                runtime,
                lease_id=0,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 2,
                retry_safe=False,
            )
            assert b_received.wait(timeout=2)
            cancel_ticket(runtime, a_ticket)
            respond_to_b.set()
            assert _count(wait_ticket(b_ticket)) == 222
        finally:
            respond_to_b.set()
            release_server.set()
            close_actor(runtime)


@pytest.mark.parametrize(
    ("command", "payload", "error_type", "match"),
    [
        (0x9999, {}, UnsupportedCommandError, "not migrated"),
        (TYPE_SECURITY_COUNT, {"market": "invalid"}, ProtocolError, "invalid market"),
        (TYPE_SECURITY_LIST, {"market": "sz", "start": -1}, ValueError, "start must be"),
    ],
)
def test_ready_actor_survives_request_build_errors(
    command: int,
    payload: dict,
    error_type: type[Exception],
    match: str,
) -> None:
    release_server = threading.Event()

    def handler(conn) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        for value in (101, 202):
            msg_id, msg_type, _ = read_request(conn)
            assert msg_type == TYPE_SECURITY_COUNT
            conn.sendall(response_bytes(msg_id, msg_type, value.to_bytes(2, "little")))
        assert release_server.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        transport = SocketTransport([server.host], timeout=1, heartbeat_interval=None)
        try:
            assert transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 101
            runtime = transport._runtime
            assert runtime is not None
            generation = runtime.generation
            with pytest.raises(error_type, match=match):
                transport.execute(command, payload)
            assert runtime.state is RuntimeState.RUNNING
            assert runtime.generation is generation
            assert transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 202
        finally:
            release_server.set()
            transport.close()


class RecordingEvent:
    def __init__(self) -> None:
        self.timeout: float | None = None

    def wait(self, timeout: float | None = None) -> bool:
        self.timeout = timeout
        return False

    def is_set(self) -> bool:
        return False


def test_wait_ticket_uses_only_absolute_deadline_no_fixed_grace() -> None:
    deadline = time.monotonic() + 0.2
    event = RecordingEvent()
    ticket = ConnectTicket(runtime_epoch=1, deadline=deadline)
    ticket.completed = event  # type: ignore[assignment]
    before_wait = time.monotonic()

    with pytest.raises(ResponseTimeoutError, match="during connect"):
        wait_ticket(ticket)

    assert event.timeout is not None
    assert event.timeout <= deadline - before_wait + 0.005


def test_execute_timeout_retains_slot_until_exact_cancel_ack(monkeypatch) -> None:
    first_request = threading.Event()
    terminal_entered = threading.Event()
    allow_terminal = threading.Event()
    second_done = threading.Event()
    second_result: list[object] = []

    def first(conn) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        read_request(conn)
        first_request.set()
        while conn.recv(4096):
            pass

    def second(conn) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, (222).to_bytes(2, "little")))

    original_expire = actor_module._expire_active_task
    original_apply_cancel = actor_module._apply_cancel

    def controlled_expire(runtime) -> None:
        ticket = runtime.active_task
        if ticket is not None and time.monotonic() >= ticket.deadline and not allow_terminal.is_set():
            terminal_entered.set()
            assert allow_terminal.wait(timeout=2)
        original_expire(runtime)

    def controlled_apply_cancel(runtime, token) -> None:
        terminal_entered.set()
        assert allow_terminal.wait(timeout=2)
        original_apply_cancel(runtime, token)

    monkeypatch.setattr(actor_module, "_expire_active_task", controlled_expire)
    monkeypatch.setattr(actor_module, "_apply_cancel", controlled_apply_cancel)

    with Scripted7709Server([first, second]) as server:
        transport = SocketTransport([server.host], timeout=0.2, heartbeat_interval=None)
        try:
            with pytest.raises(ResponseTimeoutError, match="during response"):
                transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"})
            assert first_request.is_set()
            assert terminal_entered.wait(timeout=2)

            def run_second() -> None:
                try:
                    second_result.append(transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}))
                except BaseException as exc:
                    second_result.append(exc)
                finally:
                    second_done.set()

            thread = threading.Thread(target=run_second)
            thread.start()
            assert not second_done.wait(timeout=0.05)
            allow_terminal.set()
            assert second_done.wait(timeout=2)
            thread.join(timeout=2)
            assert second_result == [222]
        finally:
            allow_terminal.set()
            transport.close()
