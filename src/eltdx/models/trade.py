"""Trade tick models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class TradeTick:
    index: int
    absolute_index: int
    time_minutes: int
    time_label: str
    trade_datetime: datetime | None
    price: float
    price_milli: int
    volume: int
    order_count: int
    status_raw: int
    side: str
    price_delta_raw: int
    price_acc_raw: int
    unknown_tail_raw: int | None = None
    reserved_zero: int | None = None
    record_hex: str = ""

    @property
    def trade_amount_yuan(self) -> float:
        return self.price * self.volume * 100.0


@dataclass(frozen=True, slots=True)
class TradePage:
    exchange: str
    market_id: int
    code: str
    start: int
    request_count: int
    ticks: tuple[TradeTick, ...]
    trading_date: date | None = None
    price_base_raw_f32: float | None = None
    raw_payload: bytes = b""

    @property
    def full_code(self) -> str:
        return f"{self.exchange}{self.code}"

    @property
    def count(self) -> int:
        return len(self.ticks)

    @property
    def has_more(self) -> bool:
        return self.count >= self.request_count
