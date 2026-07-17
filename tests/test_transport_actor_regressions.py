from __future__ import annotations

import errno
import inspect
import socket
import sys
import threading
import time
from collections import deque

import pytest

from actor_support import Scripted7709Server, handshake_payload, read_request, response_bytes
from eltdx.exceptions import ConnectionClosedError, ProtocolError, ResponseTimeoutError, UnsupportedCommandError
from eltdx.hosts import resolve_hosts
from eltdx.protocol.commands import parse_command_response
from eltdx.protocol.constants import TYPE_HANDSHAKE, TYPE_HEARTBEAT, TYPE_SECURITY_COUNT, TYPE_SECURITY_LIST
from eltdx.protocol.frame import decode_response
from eltdx.transport.actor import (
    ConnectTicket,
    FrameEnvelope,
    RuntimeState,
    cancel_ticket,
    close_actor,
    start_actor,
    submit_connect,
    submit_request,
    wait_ticket,
)
from eltdx.transport import actor as actor_module
from eltdx.transport import socket as socket_module
from eltdx.transport.push import PushBuffer
from eltdx.transport.socket import SocketTransport


def _count(envelope: FrameEnvelope) -> int:
    return parse_command_response(envelope.command, envelope.response, envelope.request_payload_snapshot)


def test_message_ids_are_keyed_nonrepeating_exchange_tokens() -> None:
    runtime = actor_module.ActorRuntime(1, ())
    runtime.msg_id_key = bytes(range(16))

    message_ids = [actor_module._next_message_id(runtime) for _ in range(10_000)]

    assert len(set(message_ids)) == len(message_ids)
    assert message_ids[:3] == [394095216, 979823185, 424604770]
    assert all(right != (left + 1) & 0xFFFFFFFF for left, right in zip(message_ids, message_ids[1:]))

    zero_preimage = actor_module.ActorRuntime(1, ())
    zero_preimage.msg_id_key = bytes(range(16))
    zero_preimage.msg_id_counter = 1_765_785_446
    assert actor_module._next_message_id(zero_preimage) != 0


@pytest.mark.parametrize(
    ("wake_seen", "tcp_seen", "blocked_by_frames", "blocked_by_buffer", "expected_advances"),
    (
        (True, False, False, False, 1),
        (False, False, False, False, 0),
        (True, True, False, False, 0),
        (True, False, True, False, 0),
        (True, False, False, True, 0),
    ),
)
def test_wake_only_batch_advances_only_without_tcp_or_receive_backlog(
    monkeypatch,
    wake_seen: bool,
    tcp_seen: bool,
    blocked_by_frames: bool,
    blocked_by_buffer: bool,
    expected_advances: int,
) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    sock = socket.socket()
    generation = actor_module.TcpGeneration(1, sock, endpoint, actor_module.TcpState.READY)
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.generation = generation
    advances: list[object] = []
    monkeypatch.setattr(actor_module, "_advance_active_task", lambda candidate: advances.append(candidate))
    if blocked_by_frames:
        generation.decoded_frames.append(object())  # type: ignore[arg-type]
    if blocked_by_buffer:
        generation.decoder.feed(response_bytes(1, TYPE_SECURITY_COUNT, b"\x00\x00")[:8])
    try:
        actor_module._advance_wake_only_batch(
            runtime,
            wake_seen=wake_seen,
            tcp_seen=tcp_seen,
        )
    finally:
        sock.close()

    assert advances == [runtime] * expected_advances


def test_old_decoded_batch_cannot_complete_next_request_after_64_frame_budget() -> None:
    b_received = threading.Event()
    release_server = threading.Event()
    b_created = threading.Event()
    b_tickets = []
    future_ids: list[int] = []

    def handler(conn) -> None:
        msg_id, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))

        a_msg_id, msg_type, _ = read_request(conn)
        b_msg_id = future_ids[0]
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
        future_ids.append(actor_module._message_id_for_counter(runtime, 3))

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
    future_ids: list[int] = []

    def handler(conn) -> None:
        handshake_id, handshake_type, _ = read_request(conn)
        assert handshake_type == TYPE_HANDSHAKE
        business_id = future_ids[0]
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
        future_ids.append(actor_module._message_id_for_counter(runtime, 2))
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
    future_ids: list[int] = []

    def observed_feed(decoder, data):
        frames = original_feed(decoder, data)
        if decoder.buffered_bytes:
            partial_seen.set()
        return frames

    monkeypatch.setattr(actor_module.ResponseFrameDecoder, "feed", observed_feed)

    def handler(conn) -> None:
        handshake_id, handshake_type, _ = read_request(conn)
        business_id = future_ids[0]
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
        future_ids.append(actor_module._message_id_for_counter(runtime, 2))
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
        sent_any=True,
        rx_boundary=rx_boundary,
    )
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.active_task = ticket
    runtime.generation = generation

    actor_module._route_frame(
        runtime,
        generation,
        actor_module.ReceivedFrame(
            rx_sequence,
            decode_response(raw),
            exchange_id=generation.active_exchange.exchange_id,
            send_complete=tx_offset == len(generation.tx_bytes),
        ),
    )

    assert not ticket.completed.is_set()
    assert runtime.active_task is ticket
    assert runtime.stale_event_count == 1


class ReadablePartialSocket:
    def __init__(self, recv_outcomes: list[bytes | BaseException]) -> None:
        self.recv_outcomes = recv_outcomes
        self.send_calls = 0
        self.closed = False

    def recv(self, _size: int) -> bytes:
        if not self.recv_outcomes:
            raise BlockingIOError()
        outcome = self.recv_outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def send(self, data) -> int:
        self.send_calls += 1
        return len(data)

    def close(self) -> None:
        self.closed = True


class RecordingInterestSelector:
    def __init__(self) -> None:
        self.events: list[int] = []

    def modify(self, _sock, events: int, _token) -> None:
        self.events.append(events)

    def unregister(self, _sock) -> None:
        return None


def _partial_exchange_runtime(
    recv_outcomes: list[bytes | BaseException],
) -> tuple[actor_module.ActorRuntime, actor_module.TcpGeneration, actor_module.RequestTicket, ReadablePartialSocket]:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    ticket = actor_module.RequestTicket(
        1,
        1,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        False,
        request_id=1,
    )
    ticket.state = actor_module.RequestState.SENDING
    sock = ReadablePartialSocket(recv_outcomes)
    generation = actor_module.TcpGeneration(1, sock, endpoint, actor_module.TcpState.READY)
    generation.tx_bytes = b"abc"
    generation.tx_offset = 1
    generation.selector_events = actor_module.selectors.EVENT_READ | actor_module.selectors.EVENT_WRITE
    generation.active_exchange = actor_module.WireExchange(
        ticket,
        TYPE_SECURITY_COUNT,
        7,
        TYPE_SECURITY_COUNT,
        b"abc",
        False,
        sent_any=True,
    )
    runtime = actor_module.ActorRuntime(1, (endpoint,), push_buffer=PushBuffer(1, max_frames=128))
    runtime.selector = RecordingInterestSelector()
    runtime.active_task = ticket
    runtime.generation = generation
    return runtime, generation, ticket, sock


def test_presend_read_batch_over_64_is_classified_before_write_without_starvation() -> None:
    old_frames = [
        response_bytes(10_000 + index, TYPE_SECURITY_COUNT, index.to_bytes(2, "little"))
        for index in range(64)
    ]
    collision = response_bytes(7, TYPE_SECURITY_COUNT, (999).to_bytes(2, "little"))
    runtime, generation, ticket, sock = _partial_exchange_runtime(
        [b"".join(old_frames) + collision, BlockingIOError()]
    )
    token = actor_module.SelectorToken("tcp", 1, 1, sock)

    actor_module._handle_tcp_event(
        runtime,
        token,
        actor_module.selectors.EVENT_READ | actor_module.selectors.EVENT_WRITE,
    )

    assert sock.send_calls == 0
    assert not ticket.completed.is_set()
    assert len(generation.decoded_frames) == 1

    actor_module._receive_generation_safely(runtime, generation)
    pushed = runtime.push_buffer.drain()
    assert any(int.from_bytes(item.response.data[:2], "little") == 999 for item in pushed)
    assert not ticket.completed.is_set()

    actor_module._handle_tcp_event(runtime, token, actor_module.selectors.EVENT_WRITE)
    assert sock.send_calls == 1
    real = response_bytes(7, TYPE_SECURITY_COUNT, (777).to_bytes(2, "little"))
    sock.recv_outcomes.extend((real, BlockingIOError()))
    actor_module._handle_tcp_event(runtime, token, actor_module.selectors.EVENT_READ)
    assert _count(wait_ticket(ticket)) == 777


def test_partial_collision_write_only_between_head_and_tail_cannot_match() -> None:
    collision = response_bytes(7, TYPE_SECURITY_COUNT, (999).to_bytes(2, "little"))
    runtime, generation, ticket, sock = _partial_exchange_runtime(
        [collision[:8], BlockingIOError(), collision[8:], BlockingIOError()]
    )
    token = actor_module.SelectorToken("tcp", 1, 1, sock)

    actor_module._handle_tcp_event(
        runtime,
        token,
        actor_module.selectors.EVENT_READ | actor_module.selectors.EVENT_WRITE,
    )
    assert sock.send_calls == 0
    assert generation.decoder.buffered_bytes == 8
    assert generation.selector_events == actor_module.selectors.EVENT_READ

    actor_module._handle_tcp_event(runtime, token, actor_module.selectors.EVENT_WRITE)
    assert sock.send_calls == 0
    assert generation.decoder.buffered_bytes == 8
    assert generation.selector_events == actor_module.selectors.EVENT_READ

    actor_module._handle_tcp_event(
        runtime,
        token,
        actor_module.selectors.EVENT_READ | actor_module.selectors.EVENT_WRITE,
    )
    assert sock.send_calls == 1
    assert not ticket.completed.is_set()
    pushed = runtime.push_buffer.drain()
    assert [int.from_bytes(item.response.data[:2], "little") for item in pushed] == [999]

    real = response_bytes(7, TYPE_SECURITY_COUNT, (777).to_bytes(2, "little"))
    sock.recv_outcomes.extend((real, BlockingIOError()))
    actor_module._handle_tcp_event(runtime, token, actor_module.selectors.EVENT_READ)
    assert _count(wait_ticket(ticket)) == 777


def test_partial_tail_over_fairness_budget_resumes_read_only_send() -> None:
    old_batch = b"".join(
        response_bytes(20_000 + index, TYPE_SECURITY_COUNT, index.to_bytes(2, "little"))
        for index in range(65)
    )
    runtime, generation, ticket, sock = _partial_exchange_runtime(
        [old_batch[:8], BlockingIOError(), old_batch[8:], BlockingIOError()]
    )
    token = actor_module.SelectorToken("tcp", 1, 1, sock)

    actor_module._handle_tcp_event(
        runtime,
        token,
        actor_module.selectors.EVENT_READ | actor_module.selectors.EVENT_WRITE,
    )
    assert generation.decoder.buffered_bytes == 8
    assert generation.selector_events == actor_module.selectors.EVENT_READ

    actor_module._handle_tcp_event(runtime, token, actor_module.selectors.EVENT_READ)
    assert sock.send_calls == 0
    assert len(generation.decoded_frames) == 1
    assert generation.selector_events == actor_module.selectors.EVENT_READ

    assert actor_module._receive_generation_safely(runtime, generation)
    assert not generation.decoded_frames
    assert generation.decoder.buffered_bytes == 0
    actor_module._advance_active_task(runtime)

    assert sock.send_calls == 1
    assert not ticket.completed.is_set()
    assert runtime.push_buffer.snapshot().frame_count == 65


def test_write_only_snapshot_drains_collision_that_arrived_before_send() -> None:
    collision = response_bytes(7, TYPE_SECURITY_COUNT, (999).to_bytes(2, "little"))
    runtime, generation, ticket, sock = _partial_exchange_runtime(
        [collision, BlockingIOError()]
    )
    token = actor_module.SelectorToken("tcp", 1, 1, sock)

    actor_module._handle_tcp_event(runtime, token, actor_module.selectors.EVENT_WRITE)

    assert sock.send_calls == 1
    assert not ticket.completed.is_set()
    pushed = runtime.push_buffer.drain()
    assert [int.from_bytes(item.response.data[:2], "little") for item in pushed] == [999]

    real = response_bytes(7, TYPE_SECURITY_COUNT, (777).to_bytes(2, "little"))
    sock.recv_outcomes.extend((real, BlockingIOError()))
    actor_module._handle_tcp_event(runtime, token, actor_module.selectors.EVENT_READ)
    assert _count(wait_ticket(ticket)) == 777


class SizeRespectingBurstSocket:
    def __init__(self, data: bytes) -> None:
        self._buffer = bytearray(data)
        self.closed = False

    def recv(self, size: int) -> bytes:
        if not self._buffer:
            raise BlockingIOError()
        chunk = bytes(self._buffer[:size])
        del self._buffer[:size]
        return chunk

    def close(self) -> None:
        self.closed = True


def test_legal_push_burst_above_decoded_queue_limit_keeps_response_live() -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    ticket = actor_module.RequestTicket(
        1,
        1,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 2,
        False,
        request_id=1,
    )
    ticket.state = actor_module.RequestState.WAITING_RESPONSE
    pushes = b"".join(
        response_bytes(10_000 + index, TYPE_SECURITY_COUNT, b"\x93\x93")
        for index in range(1_100)
    )
    matching = response_bytes(7, TYPE_SECURITY_COUNT, (777).to_bytes(2, "little"))
    sock = SizeRespectingBurstSocket(pushes + matching)
    generation = actor_module.TcpGeneration(1, sock, endpoint, actor_module.TcpState.READY)
    generation.tx_bytes = b"sent"
    generation.tx_offset = len(generation.tx_bytes)
    generation.exchange_counter = 1
    generation.active_exchange = actor_module.WireExchange(
        ticket,
        TYPE_SECURITY_COUNT,
        7,
        TYPE_SECURITY_COUNT,
        generation.tx_bytes,
        False,
        rx_boundary=0,
        exchange_id=1,
        sent_any=True,
        send_claimed=True,
    )
    push_buffer = PushBuffer(1, max_frames=10, max_bytes=1024)
    runtime = actor_module.ActorRuntime(1, (endpoint,), push_buffer=push_buffer)
    runtime.active_task = ticket
    runtime.generation = generation

    for _ in range(64):
        actor_module._receive_generation_safely(runtime, generation)
        if ticket.completed.is_set():
            break

    assert _count(wait_ticket(ticket)) == 777
    assert runtime.state is actor_module.RuntimeState.STARTING
    assert runtime.generation is generation
    snapshot = push_buffer.snapshot()
    assert snapshot.max_frames_observed <= 10
    assert snapshot.max_bytes_observed <= 1024
    assert snapshot.dropped_total == 1_090


def test_presend_drain_crossing_deadline_does_not_write(monkeypatch) -> None:
    runtime, generation, ticket, sock = _partial_exchange_runtime([BlockingIOError()])
    token = actor_module.SelectorToken("tcp", 1, 1, sock)
    now = [0.0]
    ticket.deadline = 5.0

    def drain(_runtime, _generation) -> bool:
        now[0] = 10.0
        return True

    monkeypatch.setattr(actor_module, "_receive_generation_safely", drain)
    monkeypatch.setattr(actor_module.time, "monotonic", lambda: now[0])

    actor_module._handle_tcp_event(
        runtime,
        token,
        actor_module.selectors.EVENT_READ | actor_module.selectors.EVENT_WRITE,
    )

    assert sock.send_calls == 0
    assert ticket.state is actor_module.RequestState.FAILED
    assert isinstance(ticket.error, ResponseTimeoutError)
    assert runtime.generation is None


@pytest.mark.parametrize("control", ("cancel", "stop"))
def test_control_arriving_during_presend_drain_prevents_write(monkeypatch, control: str) -> None:
    runtime, generation, ticket, sock = _partial_exchange_runtime([BlockingIOError()])
    token = actor_module.SelectorToken("tcp", 1, 1, sock)
    drain_entered = threading.Event()
    allow_drain = threading.Event()

    def drain(_runtime, _generation) -> bool:
        drain_entered.set()
        assert allow_drain.wait(timeout=2)
        return True

    monkeypatch.setattr(actor_module, "_receive_generation_safely", drain)
    thread = threading.Thread(
        target=actor_module._handle_tcp_event,
        args=(runtime, token, actor_module.selectors.EVENT_READ | actor_module.selectors.EVENT_WRITE),
    )
    thread.start()
    assert drain_entered.wait(timeout=2)
    if control == "cancel":
        cancel_ticket(runtime, ticket)
    else:
        actor_module.request_actor_stop(runtime)
    allow_drain.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert sock.send_calls == 0
    if control == "cancel":
        assert ticket.state is actor_module.RequestState.CANCELLED
        assert runtime.generation is None
    else:
        assert runtime.stop_requested
        assert ticket.state is actor_module.RequestState.SENDING


def test_cancel_before_wire_send_claim_prevents_new_bytes(monkeypatch) -> None:
    runtime, generation, ticket, sock = _partial_exchange_runtime([BlockingIOError()])
    token = actor_module.SelectorToken("tcp", 1, 1, sock)
    expiry_entered = threading.Event()
    allow_expiry = threading.Event()
    original_expire = actor_module._expire_active_task

    monkeypatch.setattr(actor_module, "_receive_generation_safely", lambda *_args: True)

    def controlled_expire(target_runtime) -> None:
        expiry_entered.set()
        assert allow_expiry.wait(timeout=2)
        original_expire(target_runtime)

    monkeypatch.setattr(actor_module, "_expire_active_task", controlled_expire)
    thread = threading.Thread(
        target=actor_module._handle_tcp_event,
        args=(runtime, token, actor_module.selectors.EVENT_READ | actor_module.selectors.EVENT_WRITE),
    )
    thread.start()
    assert expiry_entered.wait(timeout=2)
    cancel_ticket(runtime, ticket)
    allow_expiry.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert sock.send_calls == 0
    assert ticket.state is actor_module.RequestState.CANCELLED
    assert runtime.generation is None


def test_deadline_crossing_while_send_claim_waits_does_not_write(monkeypatch) -> None:
    runtime, generation, ticket, sock = _partial_exchange_runtime([BlockingIOError()])
    exchange = generation.active_exchange
    assert exchange is not None
    now = [0.0]
    ticket.deadline = 1.0
    claim_started = threading.Event()
    claimed: list[bool] = []
    monkeypatch.setattr(actor_module.time, "monotonic", lambda: now[0])
    runtime.control_lock.acquire()

    def claim() -> None:
        claim_started.set()
        claimed.append(actor_module._claim_generation_send(runtime, generation, exchange))

    thread = threading.Thread(target=claim)
    thread.start()
    assert claim_started.wait(timeout=2)
    now[0] = 2.0
    runtime.control_lock.release()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert claimed == [False]
    assert sock.send_calls == 0
    assert ticket.state is actor_module.RequestState.FAILED
    assert isinstance(ticket.error, ResponseTimeoutError)
    assert runtime.generation is None


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
            actor_module.ReceivedFrame(1, decode_response(raw), exchange_id=0, send_complete=True),
        )
        assert ticket.state is actor_module.RequestState.FAILED
        assert isinstance(ticket.error, ResponseTimeoutError)
        assert ticket.result is None
        assert runtime.generation is None
    finally:
        peer.close()


@pytest.mark.parametrize("control", ("cancel", "stop"))
def test_control_winner_prevents_decoded_response_success(monkeypatch, control: str) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    sock, peer = socket.socketpair()
    sock.setblocking(False)
    ticket = actor_module.RequestTicket(
        1,
        0,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        False,
        request_id=1,
    )
    ticket.state = actor_module.RequestState.WAITING_RESPONSE
    raw = response_bytes(7, TYPE_SECURITY_COUNT, (999).to_bytes(2, "little"))
    generation = actor_module.TcpGeneration(1, sock, endpoint, actor_module.TcpState.READY)
    generation.tx_bytes = b"abc"
    generation.tx_offset = len(generation.tx_bytes)
    generation.active_exchange = actor_module.WireExchange(
        ticket,
        TYPE_SECURITY_COUNT,
        7,
        TYPE_SECURITY_COUNT,
        generation.tx_bytes,
        False,
        exchange_id=1,
        sent_any=True,
    )
    generation.decoded_frames.append(
        actor_module.ReceivedFrame(1, decode_response(raw), exchange_id=1, send_complete=True)
    )
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    runtime.active_task = ticket
    runtime.generation = generation
    route_entered = threading.Event()
    release_route = threading.Event()
    original_route = actor_module._route_frame

    def controlled_route(target_runtime, target_generation, received) -> None:
        route_entered.set()
        assert release_route.wait(timeout=2)
        original_route(target_runtime, target_generation, received)

    monkeypatch.setattr(actor_module, "_route_frame", controlled_route)
    receiver = threading.Thread(target=actor_module._receive_generation_safely, args=(runtime, generation))
    receiver.start()
    try:
        assert route_entered.wait(timeout=2)
        if control == "cancel":
            assert cancel_ticket(runtime, ticket)
        else:
            actor_module.request_actor_stop(runtime)
    finally:
        release_route.set()
        receiver.join(timeout=2)

    try:
        assert not receiver.is_alive()
        if control == "stop":
            actor_module._finish_runtime(runtime)
        assert ticket.state is actor_module.RequestState.CANCELLED
        assert ticket.result is None
        assert actor_module._exact_cancel_for_ticket(runtime, ticket) is None
    finally:
        if runtime.generation is not None:
            actor_module._drop_generation(runtime, ConnectionClosedError("test cleanup"))
        peer.close()


@pytest.mark.parametrize("control", ("cancel", "stop"))
def test_control_winner_prevents_handshake_phase_advance(monkeypatch, control: str) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    sock, peer = socket.socketpair()
    sock.setblocking(False)
    ticket = actor_module.RequestTicket(
        1,
        0,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        False,
        request_id=1,
    )
    raw = response_bytes(7, TYPE_HANDSHAKE, handshake_payload())
    generation = actor_module.TcpGeneration(1, sock, endpoint, actor_module.TcpState.HANDSHAKING)
    generation.tx_bytes = b"abc"
    generation.tx_offset = len(generation.tx_bytes)
    exchange = actor_module.WireExchange(
        ticket,
        TYPE_HANDSHAKE,
        7,
        TYPE_HANDSHAKE,
        generation.tx_bytes,
        True,
        exchange_id=1,
        sent_any=True,
    )
    generation.active_exchange = exchange
    generation.decoded_frames.append(
        actor_module.ReceivedFrame(1, decode_response(raw), exchange_id=1, send_complete=True)
    )
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    runtime.active_task = ticket
    runtime.generation = generation
    route_entered = threading.Event()
    release_route = threading.Event()
    original_route = actor_module._route_frame

    def controlled_route(target_runtime, target_generation, received) -> None:
        route_entered.set()
        assert release_route.wait(timeout=2)
        original_route(target_runtime, target_generation, received)

    monkeypatch.setattr(actor_module, "_route_frame", controlled_route)
    receiver = threading.Thread(target=actor_module._receive_generation_safely, args=(runtime, generation))
    receiver.start()
    try:
        assert route_entered.wait(timeout=2)
        if control == "cancel":
            assert cancel_ticket(runtime, ticket)
        else:
            actor_module.request_actor_stop(runtime)
    finally:
        release_route.set()
        receiver.join(timeout=2)

    try:
        assert not receiver.is_alive()
        assert runtime.last_handshake is None
        if control == "stop":
            assert runtime.generation is generation
            assert generation.active_exchange is exchange
            assert not ticket.completed.is_set()
            actor_module._finish_runtime(runtime)
        assert ticket.state is actor_module.RequestState.CANCELLED
        assert actor_module._exact_cancel_for_ticket(runtime, ticket) is None
    finally:
        if runtime.generation is not None:
            actor_module._drop_generation(runtime, ConnectionClosedError("test cleanup"))
        peer.close()


def test_success_response_keeps_existing_read_interest_without_redundant_modify(monkeypatch) -> None:
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
    ticket.state = actor_module.RequestState.WAITING_RESPONSE
    raw = response_bytes(7, TYPE_SECURITY_COUNT, (999).to_bytes(2, "little"))
    generation = actor_module.TcpGeneration(1, object(), endpoint, actor_module.TcpState.READY)
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
    interest_changes: list[int] = []
    grace_states: list[tuple[float | None, object | None, bool]] = []
    monkeypatch.setattr(
        actor_module,
        "_set_generation_interest",
        lambda _runtime, _generation, events: interest_changes.append(events),
    )
    runtime.successor_grace = 0.0005
    monkeypatch.setattr(
        runtime.control_ready,
        "wait",
        lambda timeout=None: grace_states.append((timeout, runtime.active_task, ticket.completed.is_set())),
    )

    actor_module._route_frame(
        runtime,
        generation,
        actor_module.ReceivedFrame(1, decode_response(raw), exchange_id=0, send_complete=True),
    )

    assert ticket.state is actor_module.RequestState.SUCCESS
    assert runtime.active_task is None
    assert generation.state is actor_module.TcpState.READY
    assert interest_changes == []
    assert grace_states == [(0.0005, None, True)]


@pytest.mark.parametrize("mode", ("grace", "yield"))
@pytest.mark.parametrize("blocked_by", ("internal", "pending", "cancel", "stop"))
def test_actor_cooperation_skips_when_control_work_is_already_visible(
    monkeypatch, mode: str, blocked_by: str
) -> None:
    runtime = actor_module.ActorRuntime(1, ())
    ticket = actor_module.RequestTicket(
        1,
        1,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        False,
        request_id=1,
        internal=blocked_by == "internal",
    )
    if blocked_by == "pending":
        runtime.pending_task = ticket
    elif blocked_by == "cancel":
        runtime.cancel_requests[ticket.request_id] = actor_module.CancelToken(1, ticket.request_id, ticket.lease_id)
    elif blocked_by == "stop":
        runtime.stop_requested = True
    if mode == "grace":
        runtime.successor_grace = 0.1
    else:
        runtime.terminal_yield = True
    monkeypatch.setattr(
        runtime.control_ready,
        "wait",
        lambda _timeout=None: (_ for _ in ()).throw(AssertionError("Actor cooperation waited")),
    )
    monkeypatch.setattr(
        actor_module.time,
        "sleep",
        lambda _delay: (_ for _ in ()).throw(AssertionError("Actor cooperation yielded")),
    )

    actor_module._wait_for_successor(runtime, ticket)


def test_actor_notify_signals_successor_grace_before_wakeup_send() -> None:
    runtime = actor_module.ActorRuntime(1, ())
    runtime.successor_grace = 0.1
    observations: list[bool] = []

    class Writer:
        def send(self, _data: bytes) -> int:
            observations.append(runtime.control_ready.is_set())
            return 1

    actor_module._notify_actor(runtime, Writer())

    assert observations == [True]


def test_terminal_yield_is_external_only(monkeypatch) -> None:
    runtime = actor_module.ActorRuntime(1, (), terminal_yield=True)
    external = actor_module.RequestTicket(
        1,
        1,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        False,
        request_id=1,
    )
    internal = actor_module.RequestTicket(
        1,
        -1,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        False,
        request_id=2,
        internal=True,
    )
    sleeps: list[float] = []
    monkeypatch.setattr(actor_module.time, "sleep", sleeps.append)

    actor_module._wait_for_successor(runtime, external)
    actor_module._wait_for_successor(runtime, internal)

    assert sleeps == [0]


def test_successor_grace_cannot_lose_concurrent_control_signal(monkeypatch) -> None:
    clear_entered = threading.Event()
    allow_clear = threading.Event()
    wait_returned = threading.Event()
    wait_results: list[bool] = []

    class CoordinatedEvent:
        def __init__(self) -> None:
            self._event = threading.Event()

        def clear(self) -> None:
            clear_entered.set()
            assert allow_clear.wait(timeout=2)
            self._event.clear()

        def wait(self, timeout: float | None = None) -> bool:
            result = self._event.wait(timeout)
            wait_results.append(result)
            wait_returned.set()
            return result

        def set(self) -> None:
            self._event.set()

    runtime = actor_module.ActorRuntime(1, ())
    runtime.control_ready = CoordinatedEvent()
    current = actor_module.RequestTicket(
        1,
        1,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        False,
        request_id=1,
    )
    successor = actor_module.RequestTicket(
        1,
        2,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        False,
        request_id=2,
    )
    runtime.successor_grace = 0.5

    grace = threading.Thread(target=actor_module._wait_for_successor, args=(runtime, current))

    def publish() -> None:
        with runtime.control_lock:
            runtime.pending_task = successor
        actor_module._notify_actor(runtime)

    notifier = threading.Thread(target=publish)
    grace.start()
    assert clear_entered.wait(timeout=2)
    notifier.start()
    allow_clear.set()
    assert wait_returned.wait(timeout=2)
    grace.join(timeout=2)
    notifier.join(timeout=2)

    assert not grace.is_alive() and not notifier.is_alive()
    assert runtime.pending_task is successor
    assert wait_results == [True]


def test_successor_grace_cannot_clear_finalizer_stop_signal() -> None:
    clear_entered = threading.Event()
    allow_clear = threading.Event()
    wait_calls: list[float | None] = []

    class CoordinatedEvent:
        def __init__(self) -> None:
            self._event = threading.Event()

        def clear(self) -> None:
            clear_entered.set()
            assert allow_clear.wait(timeout=2)
            self._event.clear()

        def wait(self, timeout: float | None = None) -> bool:
            wait_calls.append(timeout)
            return self._event.wait(timeout)

        def set(self) -> None:
            self._event.set()

    runtime = actor_module.ActorRuntime(1, (), successor_grace=0.5)
    runtime.control_ready = CoordinatedEvent()

    class Writer:
        def send(self, _data: bytes) -> int:
            return 1

    runtime.wake_writer = Writer()
    ticket = actor_module.RequestTicket(
        1,
        1,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        False,
        request_id=1,
    )
    grace = threading.Thread(target=actor_module._wait_for_successor, args=(runtime, ticket))
    grace.start()
    assert clear_entered.wait(timeout=2)
    actor_module.abandon_actor(runtime)
    allow_clear.set()
    grace.join(timeout=2)

    assert not grace.is_alive()
    assert runtime.stop_requested
    assert wait_calls == []


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


def test_business_submission_wins_heartbeat_admission_race(monkeypatch) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    generation = actor_module.TcpGeneration(1, WouldBlockSocket(), endpoint, actor_module.TcpState.READY)
    generation.last_activity_at = 0
    guard_entered = threading.Event()
    release_guard = threading.Event()

    def heartbeat_allowed() -> bool:
        guard_entered.set()
        assert release_guard.wait(timeout=2)
        return True

    runtime = actor_module.ActorRuntime(
        1,
        (endpoint,),
        heartbeat_interval=1,
        heartbeat_allowed=heartbeat_allowed,
        request_timeout=1,
    )
    runtime.state = RuntimeState.RUNNING
    runtime.generation = generation
    advanced: list[object] = []
    monkeypatch.setattr(actor_module, "_advance_active_task", lambda candidate: advanced.append(candidate.active_task))

    scheduler = threading.Thread(target=actor_module._schedule_heartbeat, args=(runtime,))
    scheduler.start()
    business = None
    try:
        assert guard_entered.wait(timeout=2)
        business = submit_request(
            runtime,
            lease_id=0,
            command=TYPE_SECURITY_COUNT,
            payload={"market": "sz"},
            deadline=time.monotonic() + 1,
            retry_safe=False,
        )
        assert runtime.pending_task is business
    finally:
        release_guard.set()
        scheduler.join(timeout=2)

    assert not scheduler.is_alive()
    assert business is not None
    assert runtime.active_task is None
    assert runtime.pending_task is business
    assert runtime.request_id_counter == business.request_id
    assert advanced == []


@pytest.mark.parametrize("control", ("cancel", "stop", "disable"))
def test_control_change_wins_heartbeat_admission_race(monkeypatch, control: str) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    generation = actor_module.TcpGeneration(1, WouldBlockSocket(), endpoint, actor_module.TcpState.READY)
    generation.last_activity_at = 0
    guard_entered = threading.Event()
    release_guard = threading.Event()

    def heartbeat_allowed() -> bool:
        guard_entered.set()
        assert release_guard.wait(timeout=2)
        return True

    runtime = actor_module.ActorRuntime(
        1,
        (endpoint,),
        heartbeat_interval=1,
        heartbeat_allowed=heartbeat_allowed,
        request_timeout=1,
    )
    runtime.state = RuntimeState.RUNNING
    runtime.generation = generation
    advanced: list[object] = []
    monkeypatch.setattr(actor_module, "_advance_active_task", lambda candidate: advanced.append(candidate.active_task))

    scheduler = threading.Thread(target=actor_module._schedule_heartbeat, args=(runtime,))
    scheduler.start()
    business = None
    try:
        assert guard_entered.wait(timeout=2)
        if control == "cancel":
            business = submit_request(
                runtime,
                lease_id=0,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 1,
                retry_safe=False,
            )
            assert cancel_ticket(runtime, business)
        elif control == "stop":
            actor_module.request_actor_stop(runtime)
        else:
            with runtime.control_lock:
                runtime.heartbeat_interval = None
    finally:
        release_guard.set()
        scheduler.join(timeout=2)

    assert not scheduler.is_alive()
    assert runtime.active_task is None
    assert advanced == []
    if control == "cancel":
        assert business is not None
        assert runtime.pending_task is business
        assert runtime.request_id_counter == business.request_id
        assert actor_module._exact_cancel_for_ticket(runtime, business) is not None
    else:
        assert runtime.request_id_counter == 0


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


class StalledConnectSocket:
    def __init__(self, family: int, socktype: int, proto: int) -> None:
        self._socket, self._peer = socket.socketpair()

    def setblocking(self, value: bool) -> None:
        self._socket.setblocking(value)

    def connect_ex(self, _address) -> int:
        return errno.EINPROGRESS

    def getsockopt(self, _level: int, _option: int) -> int:
        return errno.EINPROGRESS

    def fileno(self) -> int:
        return self._socket.fileno()

    def close(self) -> None:
        self._socket.close()
        self._peer.close()


class ImmediateConnectSocket(StalledConnectSocket):
    def connect_ex(self, _address) -> int:
        return 0


@pytest.mark.parametrize("control", ("cancel", "stop"))
def test_control_winner_prevents_new_generation_connect(monkeypatch, control: str) -> None:
    created: list[StalledConnectSocket] = []

    def factory(family: int, socktype: int, proto: int) -> StalledConnectSocket:
        sock = StalledConnectSocket(family, socktype, proto)
        created.append(sock)
        return sock

    start_entered = threading.Event()
    release_start = threading.Event()
    original_start = actor_module._start_next_endpoint

    def controlled_start(runtime) -> None:
        start_entered.set()
        assert release_start.wait(timeout=2)
        original_start(runtime)

    monkeypatch.setattr(actor_module, "_start_next_endpoint", controlled_start)
    runtime = start_actor(108, resolve_hosts(["127.0.0.1:9"]), socket_factory=factory)
    ticket = submit_request(
        runtime,
        lease_id=0,
        command=TYPE_SECURITY_COUNT,
        payload={"market": "sz"},
        deadline=time.monotonic() + 1,
        retry_safe=False,
    )
    try:
        assert start_entered.wait(timeout=2)
        if control == "cancel":
            assert cancel_ticket(runtime, ticket)
        else:
            actor_module.request_actor_stop(runtime)
    finally:
        release_start.set()

    try:
        with pytest.raises(ConnectionClosedError, match="cancelled|stopped"):
            wait_ticket(ticket)
        assert created == []
        if control == "cancel":
            assert runtime.state is RuntimeState.RUNNING
        else:
            assert runtime.stopped.wait(timeout=2)
    finally:
        close_actor(runtime)


@pytest.mark.parametrize("entrypoint", ("ready", "finish_connect"))
@pytest.mark.parametrize("control", ("cancel", "stop"))
def test_control_winner_prevents_connect_ticket_success(entrypoint: str, control: str) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    sock, peer = socket.socketpair()
    state = actor_module.TcpState.READY if entrypoint == "ready" else actor_module.TcpState.CONNECTING
    generation = actor_module.TcpGeneration(1, sock, endpoint, state)
    ticket = ConnectTicket(
        runtime_epoch=1,
        deadline=time.monotonic() + 1,
        lease_id=0,
        request_id=1,
    )
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    runtime.generation = generation
    runtime.active_task = ticket
    try:
        if control == "cancel":
            assert cancel_ticket(runtime, ticket)
        else:
            actor_module.request_actor_stop(runtime)

        if entrypoint == "ready":
            actor_module._advance_active_task(runtime)
        else:
            actor_module._finish_connect(runtime, generation)
        if control == "stop":
            actor_module._finish_runtime(runtime)

        assert ticket.state is actor_module.RequestState.CANCELLED
        assert ticket.connected_host is None
        assert actor_module._exact_cancel_for_ticket(runtime, ticket) is None
    finally:
        if runtime.generation is not None:
            actor_module._drop_generation(runtime, ConnectionClosedError("test cleanup"))
        peer.close()


@pytest.mark.parametrize(
    "failure",
    ("request_deadline", "request_build", "request_retry", "request_final", "connect_final"),
)
@pytest.mark.parametrize("control", ("cancel", "stop"))
def test_control_winner_prevents_terminal_failure(failure: str, control: str) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    peer = None
    if failure == "connect_final":
        ticket: ConnectTicket | actor_module.RequestTicket = ConnectTicket(
            runtime_epoch=1,
            deadline=time.monotonic() + 1,
            lease_id=0,
            request_id=1,
        )
    else:
        ticket = actor_module.RequestTicket(
            1,
            0,
            0x9999 if failure == "request_build" else TYPE_SECURITY_COUNT,
            {"market": "sz"},
            time.monotonic() - 1 if failure == "request_deadline" else time.monotonic() + 1,
            False,
            request_id=1,
        )
        if failure == "request_build":
            sock, peer = socket.socketpair()
            runtime.generation = actor_module.TcpGeneration(1, sock, endpoint, actor_module.TcpState.READY)
        elif failure == "request_retry":
            ticket.attempts = 1
            ticket.state = actor_module.RequestState.WAITING_RESPONSE
    runtime.active_task = ticket
    try:
        if control == "cancel":
            assert cancel_ticket(runtime, ticket)
        else:
            actor_module.request_actor_stop(runtime)

        if failure == "request_deadline":
            actor_module._start_request_attempt(runtime)
        elif failure == "request_build":
            assert not actor_module._begin_exchange(runtime, ticket, ticket.command, handshake=False)
        else:
            actor_module._fail_active_task(
                runtime,
                ConnectionClosedError("injected failure"),
                retryable=failure == "request_retry",
            )

        if failure == "request_retry":
            assert ticket.attempts == 1
            assert runtime.generation is None
        if control == "stop":
            assert ticket.state not in actor_module.TERMINAL_REQUEST_STATES
            actor_module._finish_runtime(runtime)
        assert ticket.state is actor_module.RequestState.CANCELLED
        assert actor_module._exact_cancel_for_ticket(runtime, ticket) is None
    finally:
        if runtime.generation is not None:
            actor_module._drop_generation(runtime, ConnectionClosedError("test cleanup"))
        if peer is not None:
            peer.close()


def test_late_cancel_after_terminal_claim_is_noop_without_token() -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    ticket = actor_module.RequestTicket(
        1,
        0,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        False,
        request_id=1,
    )
    ticket.state = actor_module.RequestState.WAITING_RESPONSE
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    runtime.active_task = ticket

    assert actor_module._claim_active_ticket_terminal(runtime, ticket)
    assert ticket.terminal_claimed
    assert ticket.state is actor_module.RequestState.WAITING_RESPONSE
    assert cancel_ticket(runtime, ticket)
    assert actor_module._exact_cancel_for_ticket(runtime, ticket) is None
    assert runtime.active_task is ticket

    assert actor_module._complete_ticket(
        ticket,
        actor_module.RequestState.SUCCESS,
        result="claimed",
        terminal_claimed=True,
    )
    runtime.active_task = None
    assert wait_ticket(ticket) == "claimed"


def test_cancel_request_during_connect_drops_generation_before_terminal() -> None:
    created = 0

    def factory(family: int, socktype: int, proto: int):
        nonlocal created
        created += 1
        socket_type = StalledConnectSocket if created == 1 else ImmediateConnectSocket
        return socket_type(family, socktype, proto)

    runtime = start_actor(108, resolve_hosts(["127.0.0.1:9"]), socket_factory=factory)
    try:
        ticket = submit_request(
            runtime,
            lease_id=0,
            command=TYPE_SECURITY_COUNT,
            payload={"market": "sz"},
            deadline=time.monotonic() + 1,
            retry_safe=False,
        )
        assert runtime.generation_started.wait(timeout=2)
        cancel_ticket(runtime, ticket)

        with pytest.raises(ConnectionClosedError, match="cancelled"):
            wait_ticket(ticket)
        assert runtime.generation is None
        assert wait_ticket(submit_connect(runtime, time.monotonic() + 1)) == "127.0.0.1:9"
        assert runtime.state is RuntimeState.RUNNING
    finally:
        close_actor(runtime)


def test_cancel_arriving_during_control_drain_prevents_pending_promotion(monkeypatch) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    pending = actor_module.RequestTicket(
        1,
        0,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        False,
        request_id=2,
    )
    runtime.pending_task = pending
    old_cancel = actor_module.CancelToken(1, 1, 99)
    runtime.cancel_requests[old_cancel.request_id] = old_cancel
    apply_entered = threading.Event()
    allow_apply = threading.Event()
    original_apply = actor_module._apply_cancel

    def controlled_apply(target_runtime, token) -> None:
        if token is old_cancel:
            apply_entered.set()
            assert allow_apply.wait(timeout=2)
        original_apply(target_runtime, token)

    monkeypatch.setattr(actor_module, "_apply_cancel", controlled_apply)
    thread = threading.Thread(target=actor_module._drain_control, args=(runtime,))
    thread.start()
    assert apply_entered.wait(timeout=2)
    cancel_ticket(runtime, pending)
    allow_apply.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert pending.state is actor_module.RequestState.CANCELLED
    assert pending.completed.is_set()
    assert runtime.active_task is None
    assert runtime.pending_task is None
    assert not runtime.cancel_requests


def test_connect_interrupt_enqueues_exact_cancel_and_holds_lock_until_actor_ack(monkeypatch) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    transport._runtime = runtime
    transport._resolved_endpoints = (endpoint,)
    tickets: list[ConnectTicket] = []
    original_submit = socket_module.submit_connect

    def capture_submit(*args, **kwargs):
        ticket = original_submit(*args, **kwargs)
        tickets.append(ticket)
        return ticket

    monkeypatch.setattr(socket_module, "submit_connect", capture_submit)
    monkeypatch.setattr(
        socket_module,
        "wait_ticket",
        lambda _ticket: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        transport._connect_with_deadline(
            deadline=time.monotonic() + 1,
            completion=None,
            runtime=runtime,
        )

    assert len(tickets) == 1
    ticket = tickets[0]
    token = runtime.cancel_requests[ticket.request_id]
    assert (token.runtime_epoch, token.request_id, token.lease_id) == (
        ticket.runtime_epoch,
        ticket.request_id,
        ticket.lease_id,
    )
    assert not transport._request_lock.acquire(blocking=False)

    actor_module._drain_control(runtime)
    assert ticket.state is actor_module.RequestState.CANCELLED
    assert ticket.completed.is_set()
    assert transport._request_lock.acquire(blocking=False)
    transport._request_lock.release()


@pytest.mark.parametrize("ticket_kind", ("connect", "request"))
def test_interrupt_after_ticket_publication_retains_terminal_ownership(monkeypatch, ticket_kind: str) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    transport._runtime = runtime
    transport._resolved_endpoints = (endpoint,)
    tickets: list[ConnectTicket | actor_module.RequestTicket] = []

    if ticket_kind == "connect":
        original_submit = socket_module.submit_connect

        def interrupted_submit(*args, **kwargs):
            ticket = original_submit(*args, **kwargs)
            tickets.append(ticket)
            raise KeyboardInterrupt()

        monkeypatch.setattr(socket_module, "submit_connect", interrupted_submit)
    else:
        original_submit = socket_module.submit_request

        def interrupted_submit(*args, **kwargs):
            ticket = original_submit(*args, **kwargs)
            tickets.append(ticket)
            raise KeyboardInterrupt()

        monkeypatch.setattr(socket_module, "submit_request", interrupted_submit)

    with pytest.raises(KeyboardInterrupt):
        if ticket_kind == "connect":
            transport._connect_with_deadline(
                deadline=time.monotonic() + 1,
                completion=None,
                runtime=runtime,
            )
        else:
            transport._execute_with_lease(
                TYPE_SECURITY_COUNT,
                {"market": "sz"},
                lease_id=0,
                deadline=time.monotonic() + 1,
                completion=None,
                runtime=runtime,
            )

    assert len(tickets) == 1
    ticket = tickets[0]
    assert runtime.pending_task is ticket
    token = runtime.cancel_requests[ticket.request_id]
    assert (token.runtime_epoch, token.request_id, token.lease_id) == (
        ticket.runtime_epoch,
        ticket.request_id,
        ticket.lease_id,
    )
    assert not transport._request_lock.acquire(blocking=False)

    actor_module._drain_control(runtime)
    assert ticket.state is actor_module.RequestState.CANCELLED
    assert ticket.completed.is_set()
    assert transport._request_lock.acquire(blocking=False)
    transport._request_lock.release()


@pytest.mark.parametrize("ticket_kind", ("connect", "request"))
def test_interrupt_after_request_lock_before_submission_releases_lock(monkeypatch, ticket_kind: str) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    transport._runtime = runtime
    transport._resolved_endpoints = (endpoint,)
    monkeypatch.setattr(
        socket_module,
        "_TerminalCompletion",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        if ticket_kind == "connect":
            transport._connect_with_deadline(
                deadline=time.monotonic() + 1,
                completion=None,
                runtime=runtime,
            )
        else:
            transport._execute_with_lease(
                TYPE_SECURITY_COUNT,
                {"market": "sz"},
                lease_id=0,
                deadline=time.monotonic() + 1,
                completion=None,
                runtime=runtime,
            )

    assert runtime.pending_task is None and runtime.active_task is None
    assert transport._request_lock.acquire(blocking=False)
    transport._request_lock.release()


@pytest.mark.parametrize("ticket_kind", ("connect", "request"))
def test_interrupt_after_request_lock_acquired_before_return_releases_lock(monkeypatch, ticket_kind: str) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    transport._runtime = runtime
    transport._resolved_endpoints = (endpoint,)
    original_acquire = transport._acquire_request_lock

    def interrupted_acquire(deadline: float, *, ownership=None) -> bool:
        assert original_acquire(deadline, ownership=ownership)
        raise KeyboardInterrupt()

    monkeypatch.setattr(transport, "_acquire_request_lock", interrupted_acquire)
    with pytest.raises(KeyboardInterrupt):
        if ticket_kind == "connect":
            transport._connect_with_deadline(
                deadline=time.monotonic() + 1,
                completion=None,
                runtime=runtime,
            )
        else:
            transport._execute_with_lease(
                TYPE_SECURITY_COUNT,
                {"market": "sz"},
                lease_id=0,
                deadline=time.monotonic() + 1,
                completion=None,
                runtime=runtime,
            )

    assert runtime.pending_task is None and runtime.active_task is None
    assert transport._request_lock.acquire(blocking=False)
    transport._request_lock.release()


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


def test_malformed_heartbeat_response_releases_owner_for_next_request() -> None:
    release_server = threading.Event()

    def malformed(conn) -> None:
        msg_id, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_HEARTBEAT
        conn.sendall(response_bytes(msg_id, msg_type, b"\x00"))

    def healthy(conn) -> None:
        msg_id, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_HANDSHAKE
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        assert msg_type == TYPE_SECURITY_COUNT
        conn.sendall(response_bytes(msg_id, msg_type, (303).to_bytes(2, "little")))
        assert release_server.wait(timeout=2)

    with Scripted7709Server([malformed, healthy]) as server:
        transport = SocketTransport([server.host], timeout=1, heartbeat_interval=None)
        try:
            with pytest.raises(ProtocolError, match="invalid heartbeat payload length"):
                transport.execute(TYPE_HEARTBEAT, {})
            assert transport._request_lock.acquire(blocking=False)
            transport._request_lock.release()
            assert transport.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 303
            assert server.wait_for_connections(2)
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
    monkeypatch.setattr(socket_module, "_retry_safe", lambda _command: False)

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


@pytest.mark.parametrize("ticket_kind", ("connect", "request"))
def test_interrupt_inside_request_gate_claim_releases_exact_owner(ticket_kind: str) -> None:
    class InterruptAfterClaimGate:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self._owner: object | None = None
            self._interrupted = False

        def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
            acquired = self._lock.acquire(blocking) if timeout < 0 else self._lock.acquire(blocking, timeout)
            if acquired and not self._interrupted:
                self._interrupted = True
                raise KeyboardInterrupt
            return acquired

        def acquire_token(self, token: object, deadline: float) -> bool:
            acquired = self._lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
            if acquired:
                self._owner = token
            if acquired and not self._interrupted:
                self._interrupted = True
                raise KeyboardInterrupt
            return acquired

        def release_token(self, token: object) -> bool:
            if self._owner is not token:
                return False
            self._owner = None
            self._lock.release()
            return True

        def release(self) -> None:
            self._lock.release()

    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    transport._runtime = runtime
    transport._resolved_endpoints = (endpoint,)
    gate = InterruptAfterClaimGate()
    transport._request_lock = gate

    with pytest.raises(KeyboardInterrupt):
        if ticket_kind == "connect":
            transport._connect_with_deadline(
                deadline=time.monotonic() + 1,
                completion=None,
                runtime=runtime,
            )
        else:
            transport._execute_with_lease(
                TYPE_SECURITY_COUNT,
                {"market": "sz"},
                lease_id=0,
                deadline=time.monotonic() + 1,
                completion=None,
                runtime=runtime,
            )

    assert gate.acquire(blocking=False)
    gate.release()


@pytest.mark.parametrize("ticket_kind", ("connect", "request"))
def test_interrupt_inside_submission_gate_claim_releases_exact_owner(ticket_kind: str) -> None:
    class InterruptAfterClaimGate:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self._owner: object | None = None
            self._interrupted = False

        def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
            acquired = self._lock.acquire(blocking) if timeout < 0 else self._lock.acquire(blocking, timeout)
            if acquired and not self._interrupted:
                self._interrupted = True
                raise KeyboardInterrupt
            return acquired

        def acquire_token(self, token: object, deadline: float) -> bool:
            acquired = self._lock.acquire(timeout=max(0.0, deadline - time.monotonic()))
            if acquired:
                self._owner = token
            if acquired and not self._interrupted:
                self._interrupted = True
                raise KeyboardInterrupt
            return acquired

        def release_token(self, token: object) -> bool:
            if self._owner is not token:
                return False
            self._owner = None
            self._lock.release()
            return True

        def release(self) -> None:
            self._lock.release()

    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    transport = SocketTransport(["127.0.0.1:9"], timeout=1, heartbeat_interval=None)
    transport._runtime = runtime
    transport._resolved_endpoints = (endpoint,)
    gate = InterruptAfterClaimGate()
    transport._submission_gate = gate

    with pytest.raises(KeyboardInterrupt):
        if ticket_kind == "connect":
            transport._connect_with_deadline(
                deadline=time.monotonic() + 1,
                completion=None,
                runtime=runtime,
            )
        else:
            transport._execute_with_lease(
                TYPE_SECURITY_COUNT,
                {"market": "sz"},
                lease_id=0,
                deadline=time.monotonic() + 1,
                completion=None,
                runtime=runtime,
            )

    assert gate.acquire(blocking=False)
    gate.release()
    assert transport._request_lock.acquire(blocking=False)
    transport._request_lock.release()


@pytest.mark.parametrize("ticket_kind", ("connect", "request"))
def test_interrupt_inside_actor_control_claim_releases_exact_owner(ticket_kind: str) -> None:
    class InterruptAfterClaimGate:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self._owner: object | None = None
            self._interrupted = False

        def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
            acquired = self._lock.acquire(blocking) if timeout < 0 else self._lock.acquire(blocking, timeout)
            if acquired and not self._interrupted:
                self._interrupted = True
                raise KeyboardInterrupt
            return acquired

        def acquire_token(self, token: object, deadline: float | None) -> bool:
            timeout = -1 if deadline is None else max(0.0, deadline - time.monotonic())
            acquired = self._lock.acquire() if timeout < 0 else self._lock.acquire(timeout=timeout)
            if acquired:
                self._owner = token
            if acquired and not self._interrupted:
                self._interrupted = True
                raise KeyboardInterrupt
            return acquired

        def release_token(self, token: object) -> bool:
            if self._owner is not token:
                return False
            self._owner = None
            self._lock.release()
            return True

        def release(self) -> None:
            self._lock.release()

    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    gate = InterruptAfterClaimGate()
    runtime.control_lock = gate

    with pytest.raises(KeyboardInterrupt):
        if ticket_kind == "connect":
            submit_connect(runtime, time.monotonic() + 1)
        else:
            submit_request(
                runtime,
                lease_id=0,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 1,
                retry_safe=False,
            )

    assert gate.acquire(blocking=False)
    gate.release()
    assert runtime.pending_task is None


def test_request_build_failure_keeps_actor_owner_until_ticket_terminal(monkeypatch) -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    ticket = actor_module.RequestTicket(
        1,
        0,
        0x9999,
        {},
        time.monotonic() + 1,
        False,
        request_id=1,
    )
    generation = actor_module.TcpGeneration(1, object(), endpoint, actor_module.TcpState.READY)
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    runtime.generation = generation
    runtime.active_task = ticket
    original_complete = actor_module._complete_ticket
    owner_observed: list[bool] = []

    def observe_owner(target, *args, **kwargs):
        owner_observed.append(runtime.active_task is target)
        return original_complete(target, *args, **kwargs)

    monkeypatch.setattr(actor_module, "_complete_ticket", observe_owner)
    assert not actor_module._begin_exchange(runtime, ticket, ticket.command, handshake=False)

    assert owner_observed == [True]
    assert ticket.state is actor_module.RequestState.FAILED
    assert runtime.active_task is None


@pytest.mark.parametrize("gate_factory", (actor_module.IdentityGate, socket_module._RequestGate))
def test_identity_gate_recovers_physical_condition_acquire_interrupt(gate_factory) -> None:
    gate = gate_factory()
    original = gate._condition

    class InterruptingCondition:
        def __init__(self) -> None:
            self.interrupted = False

        def acquire(self, *args, **kwargs):
            acquired = original.acquire(*args, **kwargs)
            if acquired and not self.interrupted:
                self.interrupted = True
                raise KeyboardInterrupt
            return acquired

        def release(self) -> None:
            original.release()

        def wait(self, *args, **kwargs):
            return original.wait(*args, **kwargs)

        def notify(self, *args, **kwargs) -> None:
            original.notify(*args, **kwargs)

        def __enter__(self):
            return original.__enter__()

        def __exit__(self, *args):
            return original.__exit__(*args)

    gate._condition = InterruptingCondition()
    first_token = object()
    with pytest.raises(KeyboardInterrupt):
        gate.acquire_token(first_token, time.monotonic() + 1)

    acquired: list[bool] = []
    second_token = object()

    def acquire_from_other_thread() -> None:
        result = gate.acquire_token(second_token, time.monotonic() + 0.1)
        acquired.append(result)
        if result:
            gate.release_token(second_token)

    thread = threading.Thread(target=acquire_from_other_thread)
    thread.start()
    thread.join(timeout=0.2)
    try:
        assert acquired == [True]
    finally:
        try:
            original.release()
        except RuntimeError:
            pass
        thread.join(timeout=2)


@pytest.mark.parametrize("gate_factory", (actor_module.IdentityGate, socket_module._RequestGate))
def test_identity_gate_release_recovers_physical_condition_interrupt(gate_factory) -> None:
    gate = gate_factory()
    token = object()
    assert gate.acquire_token(token, time.monotonic() + 1)
    original = gate._condition

    class InterruptingCondition:
        def acquire(self, *args, **kwargs):
            acquired = original.acquire(*args, **kwargs)
            if acquired:
                raise KeyboardInterrupt
            return acquired

        def release(self) -> None:
            original.release()

        def notify(self, *args, **kwargs) -> None:
            original.notify(*args, **kwargs)

        def __enter__(self):
            self.acquire()
            return self

        def __exit__(self, *_args):
            self.release()

    gate._condition = InterruptingCondition()
    assert gate.release_token(token)

    gate._condition = original
    replacement = object()
    assert gate.acquire_token(replacement, time.monotonic() + 0.1)
    assert gate.release_token(replacement)


@pytest.mark.parametrize("gate_factory", (actor_module.IdentityGate, socket_module._RequestGate))
def test_identity_gate_release_wakes_existing_unbounded_waiter(gate_factory) -> None:
    class RecordingWaiters(deque):
        def __init__(self) -> None:
            super().__init__()
            self.registered = threading.Event()

        def append(self, item) -> None:
            super().append(item)
            if self:
                self.registered.set()

    gate = gate_factory()
    gate._waiters = RecordingWaiters()
    owner = object()
    assert gate.acquire_token(owner, time.monotonic() + 1)
    gate._waiters.registered.clear()
    acquired: list[bool] = []
    waiter_token = object()

    def wait_for_gate() -> None:
        acquired.append(gate.acquire_token(waiter_token, None))
        gate.release_token(waiter_token)

    thread = threading.Thread(target=wait_for_gate)
    thread.start()
    assert gate._waiters.registered.wait(timeout=2)
    assert gate.release_token(owner)
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert acquired == [True]


@pytest.mark.parametrize("gate_factory", (actor_module.IdentityGate, socket_module._RequestGate))
def test_identity_gate_hands_owner_to_waiters_in_registration_order(gate_factory) -> None:
    class ControlledWaiters(deque):
        def __init__(self) -> None:
            super().__init__()
            self.registered = [threading.Event(), threading.Event()]
            self.waiting = [threading.Event(), threading.Event()]
            self.allow_first = threading.Event()

        def _record(self, item) -> None:
            index = len(self)
            event = getattr(item, "event", item)
            original_wait = event.wait

            def wait(timeout=None):
                self.waiting[index].set()
                signalled = original_wait(timeout)
                if index == 0 and signalled:
                    assert self.allow_first.wait(timeout=2)
                return signalled

            event.wait = wait
            self.registered[index].set()

        def append(self, item) -> None:
            self._record(item)
            super().append(item)

        def add(self, item) -> None:
            self.append(item)

        def discard(self, item) -> None:
            try:
                self.remove(item)
            except ValueError:
                pass

    gate = gate_factory()
    gate._waiters = ControlledWaiters()
    owner = object()
    assert gate.acquire_token(owner, time.monotonic() + 1)
    order: list[str] = []
    order_lock = threading.Lock()
    second_done = threading.Event()

    def wait_for_gate(label: str, token: object) -> None:
        assert gate.acquire_token(token, None)
        with order_lock:
            order.append(label)
        assert gate.release_token(token)
        if label == "second":
            second_done.set()

    first = threading.Thread(target=wait_for_gate, args=("first", object()))
    second = threading.Thread(target=wait_for_gate, args=("second", object()))
    first.start()
    assert gate._waiters.registered[0].wait(timeout=2)
    assert gate._waiters.waiting[0].wait(timeout=2)
    second.start()
    assert gate._waiters.registered[1].wait(timeout=2)
    assert gate._waiters.waiting[1].wait(timeout=2)

    assert gate.release_token(owner)
    second_done.wait(timeout=0.1)
    gate._waiters.allow_first.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert order == ["first", "second"]


@pytest.mark.parametrize("gate_factory", (actor_module.IdentityGate, socket_module._RequestGate))
def test_identity_gate_concurrent_stale_release_cannot_clear_new_owner(gate_factory) -> None:
    gate = gate_factory()
    old_token = object()
    new_token = object()
    assert gate.acquire_token(old_token, time.monotonic() + 1)
    handoff = gate._handoff_locked
    source, first_line = inspect.getsourcelines(handoff)
    clear_line = first_line + next(index for index, line in enumerate(source) if "self._owner = None" in line)
    paused = threading.Event()
    resume = threading.Event()
    stale_result: list[bool] = []
    competing_result: list[bool] = []

    def trace(frame, event, _arg):
        if event == "line" and frame.f_code is handoff.__func__.__code__ and frame.f_lineno == clear_line:
            paused.set()
            assert resume.wait(timeout=2)
        return trace

    def stale_release() -> None:
        sys.settrace(trace)
        try:
            stale_result.append(gate.release_token(old_token))
        finally:
            sys.settrace(None)

    def competing_release() -> None:
        competing_result.append(gate.release_token(old_token))

    stale = threading.Thread(target=stale_release)
    competing = threading.Thread(target=competing_release)
    stale.start()
    assert paused.wait(timeout=2)
    competing.start()
    competing.join(timeout=0.1)

    acquired_before_resume = False
    if not competing.is_alive():
        acquired_before_resume = gate.acquire_token(new_token, time.monotonic() + 0.1)
        assert acquired_before_resume
    resume.set()
    stale.join(timeout=2)
    competing.join(timeout=2)

    assert not stale.is_alive()
    assert not competing.is_alive()
    assert stale_result.count(True) + competing_result.count(True) == 1
    if not acquired_before_resume:
        assert gate.acquire_token(new_token, time.monotonic() + 0.1)
    assert gate._owner is new_token
    assert gate.release_token(new_token)


@pytest.mark.parametrize("gate_factory", (actor_module.IdentityGate, socket_module._RequestGate))
def test_identity_gate_does_not_handoff_to_waiter_after_deadline(gate_factory) -> None:
    class DelayedTimeoutWaiters(deque):
        def __init__(self) -> None:
            super().__init__()
            self.registered = threading.Event()
            self.waiting = threading.Event()
            self.allow_timeout = threading.Event()

        def append(self, item) -> None:
            event = item.event

            def delayed_timeout(_timeout=None):
                self.waiting.set()
                assert self.allow_timeout.wait(timeout=2)
                return False

            event.wait = delayed_timeout
            super().append(item)
            self.registered.set()

    gate = gate_factory()
    gate._waiters = DelayedTimeoutWaiters()
    owner = object()
    waiter_token = object()
    assert gate.acquire_token(owner, time.monotonic() + 1)
    deadline = time.monotonic() + 0.05
    result: list[bool] = []
    waiter = threading.Thread(target=lambda: result.append(gate.acquire_token(waiter_token, deadline)))
    waiter.start()
    assert gate._waiters.registered.wait(timeout=2)
    assert gate._waiters.waiting.wait(timeout=2)
    while time.monotonic() <= deadline:
        time.sleep(0.001)

    assert gate.release_token(owner)
    gate._waiters.allow_timeout.set()
    waiter.join(timeout=2)

    assert not waiter.is_alive()
    assert result == [False]
    assert gate._owner is None


@pytest.mark.parametrize("gate_factory", (actor_module.IdentityGate, socket_module._RequestGate))
def test_identity_gate_failed_waiter_wakeup_is_revoked_and_next_waiter_advances(gate_factory) -> None:
    class FailingWakeWaiters(deque):
        def __init__(self) -> None:
            super().__init__()
            self.registered = [threading.Event(), threading.Event()]
            self.failed_event = None
            self.original_set = None

        def append(self, item) -> None:
            index = len(self)
            if index == 0:
                self.failed_event = item.event
                self.original_set = item.event.set
                item.event.set = lambda: (_ for _ in ()).throw(KeyboardInterrupt())
            super().append(item)
            self.registered[index].set()

    gate = gate_factory()
    gate._waiters = FailingWakeWaiters()
    owner = object()
    assert gate.acquire_token(owner, time.monotonic() + 1)
    results: dict[str, bool] = {}

    def wait_for_gate(label: str, token: object) -> None:
        acquired = gate.acquire_token(token, None)
        results[label] = acquired
        if acquired:
            assert gate.release_token(token)

    first = threading.Thread(target=wait_for_gate, args=("first", object()))
    second = threading.Thread(target=wait_for_gate, args=("second", object()))
    first.start()
    assert gate._waiters.registered[0].wait(timeout=2)
    second.start()
    assert gate._waiters.registered[1].wait(timeout=2)

    release_error = None
    try:
        released = gate.release_token(owner)
    except BaseException as exc:
        release_error = exc
        released = False
    finally:
        if gate._waiters.failed_event is not None and gate._waiters.original_set is not None:
            gate._waiters.failed_event.set = gate._waiters.original_set
            gate._waiters.original_set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert release_error is None
    assert released
    assert not first.is_alive()
    assert not second.is_alive()
    assert results == {"first": False, "second": True}
    assert gate._owner is None


@pytest.mark.parametrize("gate_factory", (actor_module.IdentityGate, socket_module._RequestGate))
def test_identity_gate_recovers_physical_state_lock_acquire_interrupt(gate_factory) -> None:
    gate = gate_factory()
    original = gate._state_lock

    class InterruptingStateLock:
        def __init__(self) -> None:
            self.interrupted = False

        def acquire(self, *args, **kwargs):
            acquired = original.acquire(*args, **kwargs)
            if acquired and not self.interrupted:
                self.interrupted = True
                raise KeyboardInterrupt
            return acquired

        def release(self) -> None:
            original.release()

        def _is_owned(self) -> bool:
            checker = getattr(original, "_is_owned", None)
            return bool(checker()) if checker is not None else original.locked()

        def __enter__(self):
            self.acquire()
            return self

        def __exit__(self, *_args):
            self.release()

    gate._state_lock = InterruptingStateLock()
    with pytest.raises(KeyboardInterrupt):
        gate.acquire_token(object(), time.monotonic() + 1)

    acquired: list[bool] = []
    replacement = object()

    def acquire_replacement() -> None:
        result = gate.acquire_token(replacement, time.monotonic() + 0.1)
        acquired.append(result)
        if result:
            gate.release_token(replacement)

    thread = threading.Thread(target=acquire_replacement)
    thread.start()
    thread.join(timeout=0.2)
    try:
        assert not thread.is_alive()
        assert acquired == [True]
    finally:
        if getattr(original, "locked", lambda: False)():
            original.release()
        thread.join(timeout=2)


@pytest.mark.parametrize("gate_factory", (actor_module.IdentityGate, socket_module._RequestGate))
def test_identity_gate_compat_acquire_survives_condition_release_interrupt(gate_factory) -> None:
    gate = gate_factory()
    original = gate._condition

    class InterruptAfterReleaseCondition:
        def __init__(self) -> None:
            self.interrupted = False

        def acquire(self, *args, **kwargs):
            return original.acquire(*args, **kwargs)

        def release(self) -> None:
            original.release()
            if not self.interrupted:
                self.interrupted = True
                raise KeyboardInterrupt

    gate._condition = InterruptAfterReleaseCondition()
    error = None
    try:
        acquired = gate.acquire(timeout=0.1)
    except BaseException as exc:
        error = exc
        acquired = False

    try:
        assert error is None
        assert acquired
        gate.release()
        assert gate.acquire(blocking=False)
        gate.release()
    finally:
        owner = gate._owner
        if owner is not None:
            gate.release_token(owner)


@pytest.mark.parametrize("gate_factory", (actor_module.IdentityGate, socket_module._RequestGate))
def test_identity_gate_waiter_registration_survives_condition_release_interrupt(gate_factory) -> None:
    gate = gate_factory()
    owner = object()
    orphan = object()
    assert gate.acquire_token(owner, time.monotonic() + 1)
    original = gate._condition

    class InterruptAfterReleaseCondition:
        def __init__(self) -> None:
            self.interrupted = False

        def acquire(self, *args, **kwargs):
            return original.acquire(*args, **kwargs)

        def release(self) -> None:
            original.release()
            if not self.interrupted:
                self.interrupted = True
                raise KeyboardInterrupt

    gate._condition = InterruptAfterReleaseCondition()
    error = None
    try:
        acquired = gate.acquire_token(orphan, time.monotonic() + 0.05)
    except BaseException as exc:
        error = exc
        acquired = False

    assert error is None
    assert not acquired
    assert not gate._waiters
    assert gate.release_token(owner)
    replacement = object()
    assert gate.acquire_token(replacement, time.monotonic() + 0.1)
    assert gate.release_token(replacement)


@pytest.mark.parametrize("gate_factory", (actor_module.IdentityGate, socket_module._RequestGate))
def test_identity_gate_release_does_not_wait_for_condition_owner(gate_factory) -> None:
    gate = gate_factory()
    token = object()
    assert gate.acquire_token(token, time.monotonic() + 1)
    condition_held = threading.Event()
    release_condition = threading.Event()
    released: list[bool] = []

    def hold_condition() -> None:
        with gate._condition:
            condition_held.set()
            release_condition.wait()

    def release_token() -> None:
        released.append(gate.release_token(token))

    holder = threading.Thread(target=hold_condition)
    releaser = threading.Thread(target=release_token)
    holder.start()
    assert condition_held.wait(timeout=2)
    releaser.start()
    releaser.join(timeout=0.1)
    try:
        assert not releaser.is_alive()
        assert released == [True]
        assert gate._owner is None
    finally:
        release_condition.set()
        holder.join(timeout=2)
        releaser.join(timeout=2)


def test_terminal_completion_publishes_while_request_gate_condition_is_held() -> None:
    gate = socket_module._RequestGate()
    token = object()
    assert gate.acquire_token(token, time.monotonic() + 1)
    ticket = actor_module.RequestTicket(
        runtime_epoch=1,
        lease_id=1,
        command=TYPE_SECURITY_COUNT,
        request_payload_snapshot={},
        deadline=time.monotonic() + 1,
        retry_safe=True,
        completion=socket_module._TerminalCompletion(request_gate=gate, request_token=token),
    )
    condition_held = threading.Event()
    release_condition = threading.Event()

    def hold_condition() -> None:
        with gate._condition:
            condition_held.set()
            release_condition.wait()

    completer = threading.Thread(
        target=actor_module._complete_ticket,
        args=(ticket, actor_module.RequestState.SUCCESS),
    )
    holder = threading.Thread(target=hold_condition)
    holder.start()
    assert condition_held.wait(timeout=2)
    completer.start()
    try:
        assert ticket.completed.wait(timeout=0.1)
        assert ticket.state is actor_module.RequestState.SUCCESS
        assert gate._owner is None
    finally:
        release_condition.set()
        holder.join(timeout=2)
        completer.join(timeout=2)


def test_pending_cancel_claims_terminal_before_completion_submits_next_ticket() -> None:
    endpoint = resolve_hosts(["127.0.0.1:9"])[0]
    runtime = actor_module.ActorRuntime(1, (endpoint,))
    runtime.state = RuntimeState.RUNNING
    created: list[actor_module.RequestTicket] = []

    def submit_next(_ticket) -> None:
        created.append(
            submit_request(
                runtime,
                lease_id=0,
                command=TYPE_SECURITY_COUNT,
                payload={"market": "sz"},
                deadline=time.monotonic() + 1,
                retry_safe=False,
            )
        )

    pending = actor_module.RequestTicket(
        1,
        0,
        TYPE_SECURITY_COUNT,
        {"market": "sz"},
        time.monotonic() + 1,
        False,
        request_id=1,
        completion=submit_next,
    )
    runtime.pending_task = pending
    actor_module._apply_cancel(runtime, actor_module.CancelToken(1, 1, 0))

    assert pending.state is actor_module.RequestState.CANCELLED
    assert pending.completion_error is None
    assert len(created) == 1 and runtime.pending_task is created[0]
