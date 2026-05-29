"""测试和示例使用的内存 transport。"""

from __future__ import annotations

from typing import Any


class InMemoryTransport:
    """不依赖 socket 的确定性 transport。

    用于在接入真实 7709 服务器前验证对外 API 和单元测试。
    """

    def __init__(self) -> None:
        self.connected = False
        self.calls: list[tuple[int, dict[str, Any]]] = []
        self.responses: dict[int, Any] = {}

    def connect(self) -> None:
        self.connected = True

    def close(self) -> None:
        self.connected = False

    def register_response(self, command: int, response: Any) -> None:
        self.responses[command] = response

    def execute(self, command: int, payload: dict[str, Any] | None = None) -> Any:
        request_payload = dict(payload or {})
        self.calls.append((command, request_payload))
        if command in self.responses:
            return self.responses[command]
        return {
            "command": f"0x{command:04x}",
            "payload": request_payload,
            "status": "memory_transport",
        }

    def request(self, command: str) -> str:
        if command == "ping":
            return "pong"
        raise ValueError(f"unsupported command: {command}")

    def poll_push(self, timeout: float | None = 0.0, *, parse: bool = False):
        return None

    def drain_pushes(self, *, parse: bool = False) -> list:
        return []
