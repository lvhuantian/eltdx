from __future__ import annotations

import socket
import threading

from actor_support import Scripted7709Server, handshake_payload, read_request, response_bytes
from eltdx.protocol.constants import TYPE_SECURITY_COUNT
from eltdx.transport import socket as socket_module
from eltdx.transport.actor import cancel_ticket
from eltdx.transport.pool import PooledSocketTransport


def test_late_cancel_on_reused_pinned_lease_is_noop(monkeypatch) -> None:
    b_received = threading.Event()
    respond_to_b = threading.Event()
    release_server = threading.Event()
    captured_tickets = []
    original_submit = socket_module.submit_request

    def capture_submit(*args, **kwargs):
        ticket = original_submit(*args, **kwargs)
        captured_tickets.append(ticket)
        return ticket

    monkeypatch.setattr(socket_module, "submit_request", capture_submit)

    def handler(conn: socket.socket) -> None:
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, handshake_payload()))
        msg_id, msg_type, _ = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, (111).to_bytes(2, "little")))
        msg_id, msg_type, _ = read_request(conn)
        b_received.set()
        assert respond_to_b.wait(timeout=2)
        conn.sendall(response_bytes(msg_id, msg_type, (222).to_bytes(2, "little")))
        assert release_server.wait(timeout=2)

    with Scripted7709Server([handler]) as server:
        pool = PooledSocketTransport([server.host], pool_size=1, timeout=1, heartbeat_interval=None)
        try:
            with pool.pin() as pinned:
                assert pinned.execute(TYPE_SECURITY_COUNT, {"market": "sz"}) == 111
                a_ticket = captured_tickets[0]
                result: list[object] = []

                def run_b() -> None:
                    try:
                        result.append(pinned.execute(TYPE_SECURITY_COUNT, {"market": "sz"}))
                    except BaseException as exc:
                        result.append(exc)

                thread = threading.Thread(target=run_b)
                thread.start()
                assert b_received.wait(timeout=2)
                runtime = pinned._slot._runtime
                assert runtime is not None
                cancel_ticket(runtime, a_ticket)
                respond_to_b.set()
                thread.join(timeout=2)
                assert not thread.is_alive()
                assert result == [222]
                assert captured_tickets[1].request_id != a_ticket.request_id
        finally:
            respond_to_b.set()
            release_server.set()
            pool.close()
