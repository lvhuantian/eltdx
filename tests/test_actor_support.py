from __future__ import annotations

import socket
import threading

from actor_support import Scripted7709Server, read_request, response_bytes


def test_scripted_server_uses_events_for_parallel_connection_control() -> None:
    release = threading.Event()
    entered = threading.Barrier(3)

    def handler(conn: socket.socket) -> None:
        entered.wait(timeout=2)
        if not release.wait(timeout=2):
            raise AssertionError("handler release was not signaled")
        msg_id, msg_type, payload = read_request(conn)
        conn.sendall(response_bytes(msg_id, msg_type, payload))

    with Scripted7709Server([handler, handler]) as server:
        address, port_text = server.host.rsplit(":", 1)
        clients = [socket.create_connection((address, int(port_text)), timeout=2) for _ in range(2)]
        try:
            assert server.wait_for_connections(2)
            entered.wait(timeout=2)
            for index, client in enumerate(clients, start=1):
                client.sendall(bytes((0x0C,)) + index.to_bytes(4, "little") + b"\x01\x02\x00\x02\x00\x04\x00")
            release.set()
            for index, client in enumerate(clients, start=1):
                response = client.recv(16)
                assert response[5:9] == index.to_bytes(4, "little")
            assert server.wait_for_handlers(2)
        finally:
            release.set()
            for client in clients:
                client.close()
