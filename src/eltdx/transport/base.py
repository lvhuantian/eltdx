"""Transport interface definitions."""

from __future__ import annotations

from typing import Any, Protocol


class Transport(Protocol):
    """Request/response transport used by API services."""

    def connect(self) -> None:
        """Open transport resources."""

    def close(self) -> None:
        """Close transport resources."""

    def execute(self, command: int, payload: dict[str, Any] | None = None) -> Any:
        """Execute one logical protocol command."""

    def request(self, command: str) -> str:
        """Legacy text request used by early health tests."""

    def poll_push(self, timeout: float | None = 0.0, *, parse: bool = False) -> Any:
        """Return one queued push frame if the transport supports pushes."""
