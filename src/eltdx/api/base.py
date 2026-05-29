"""Shared API helpers."""

from __future__ import annotations

from typing import Any

from eltdx.protocol.commands import command_code
from eltdx.transport import Transport


class ApiBase:
    """Base class for capability-specific APIs."""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    def _execute(self, command_name: str, **payload: Any) -> Any:
        return self._transport.execute(command_code(command_name), payload)
