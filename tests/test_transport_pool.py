from __future__ import annotations

from eltdx.transport import PooledSocketTransport


def test_pooled_socket_transport_rotates_hosts_per_connection() -> None:
    transport = PooledSocketTransport(hosts=["127.0.0.1:1", "127.0.0.1:2"], timeout=0.1, pool_size=3, heartbeat_interval=None)

    assert transport.hosts == ("127.0.0.1:1", "127.0.0.1:2")
    assert transport.pool_size == 3
    assert transport.heartbeat_interval is None
    assert [item._hosts for item in transport._transports] == [
        ["127.0.0.1:1", "127.0.0.1:2"],
        ["127.0.0.1:2", "127.0.0.1:1"],
        ["127.0.0.1:1", "127.0.0.1:2"],
    ]


def test_pooled_socket_transport_round_robins_execute(monkeypatch) -> None:
    transport = PooledSocketTransport(hosts=["127.0.0.1:1"], timeout=0.1, pool_size=2, heartbeat_interval=None)
    calls: list[int] = []

    for index, item in enumerate(transport._transports):
        monkeypatch.setattr(item, "execute", lambda command, payload=None, index=index: calls.append(index) or index)

    assert [transport.execute(0x0004), transport.execute(0x0004), transport.execute(0x0004)] == [0, 1, 0]
    assert calls == [0, 1, 0]
