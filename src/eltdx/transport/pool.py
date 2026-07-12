"""Connection pool transport for the 7709 quote protocol."""

from __future__ import annotations

import itertools
import threading
from contextlib import contextmanager
from collections.abc import Sequence
from typing import Any, Iterator

from eltdx.hosts import DEFAULT_HOSTS, DEFAULT_PROBE_TIMEOUT, DEFAULT_PROBE_WORKERS, sort_hosts_by_latency, unique_hosts

from .socket import SocketTransport

DEFAULT_HEARTBEAT_INTERVAL = 30.0


class PooledSocketTransport:
    """Small round-robin pool of ``SocketTransport`` instances."""

    def __init__(
        self,
        hosts: Sequence[str] | None = None,
        *,
        timeout: float = 8.0,
        pool_size: int = 2,
        probe_hosts: bool = False,
        probe_timeout: float = DEFAULT_PROBE_TIMEOUT,
        probe_workers: int = DEFAULT_PROBE_WORKERS,
        heartbeat_interval: float | None = DEFAULT_HEARTBEAT_INTERVAL,
    ) -> None:
        resolved_hosts = unique_hosts(list(hosts or DEFAULT_HOSTS))
        if not resolved_hosts:
            raise ValueError("at least one host is required")
        if probe_hosts and len(resolved_hosts) > 1:
            resolved_hosts = sort_hosts_by_latency(resolved_hosts, timeout=probe_timeout, max_workers=probe_workers)

        self._hosts = resolved_hosts
        self._timeout = timeout
        self._pool_size = max(1, int(pool_size))
        self._heartbeat_interval = heartbeat_interval
        self._transports = [
            SocketTransport(
                hosts=_rotate_hosts(resolved_hosts, index),
                timeout=timeout,
                heartbeat_interval=heartbeat_interval,
            )
            for index in range(self._pool_size)
        ]
        self._round_robin = itertools.cycle(range(self._pool_size))
        self._round_robin_lock = threading.Lock()

    @property
    def hosts(self) -> tuple[str, ...]:
        return tuple(self._hosts)

    @property
    def pool_size(self) -> int:
        return self._pool_size

    @property
    def heartbeat_interval(self) -> float | None:
        return self._heartbeat_interval

    @property
    def connected_hosts(self) -> tuple[str | None, ...]:
        return tuple(transport.connected_host for transport in self._transports)

    @property
    def connected_host(self) -> str | None:
        for transport in self._transports:
            if transport.connected_host is not None:
                return transport.connected_host
        return None

    @property
    def pending_push_count(self) -> int:
        return sum(transport.pending_push_count for transport in self._transports)

    def connect(self) -> None:
        for transport in self._transports:
            transport.connect()

    def close(self) -> None:
        for transport in self._transports:
            transport.close()

    def execute(self, command: int, payload: dict[str, Any] | None = None) -> Any:
        return self._pick_transport().execute(command, payload)

    @contextmanager
    def pin(self) -> Iterator[SocketTransport]:
        """Keep a multi-request operation on one underlying connection."""

        yield self._pick_transport()

    def request(self, command: str) -> str:
        if command == "ping":
            return "pong"
        return self._pick_transport().request(command)

    def poll_push(self, timeout: float | None = 0.0, *, parse: bool = False) -> Any:
        for transport in self._transports:
            item = transport.poll_push(timeout=0.0, parse=parse)
            if item is not None:
                return item
        if timeout is None or timeout > 0:
            return self._pick_transport().poll_push(timeout=timeout, parse=parse)
        return None

    def drain_pushes(self, *, parse: bool = False) -> list[Any]:
        items: list[Any] = []
        for transport in self._transports:
            items.extend(transport.drain_pushes(parse=parse))
        return items

    def _pick_transport(self) -> SocketTransport:
        with self._round_robin_lock:
            index = next(self._round_robin)
        return self._transports[index]


def _rotate_hosts(hosts: list[str], offset: int) -> list[str]:
    if not hosts:
        return []
    index = offset % len(hosts)
    return hosts[index:] + hosts[:index]
