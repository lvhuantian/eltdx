"""Models for the 7615 TQLEX / F10 HTTP gateway."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class F10Cell:
    """A single value with its raw column name and position."""

    name: str
    value: Any
    index: int


@dataclass(frozen=True, slots=True)
class F10ResultSet:
    """One ResultSet table returned by the TQLEX gateway."""

    key: str | None
    columns: tuple[str, ...]
    rows: tuple[dict[str, Any], ...]
    row_cells: tuple[tuple[F10Cell, ...], ...]
    raw: dict[str, Any] = field(repr=False)

    @property
    def count(self) -> int:
        """Number of rows in this result set."""

        return len(self.rows)

    def first(self) -> dict[str, Any] | None:
        """Return the first row, or None when the table is empty."""

        return self.rows[0] if self.rows else None


@dataclass(frozen=True, slots=True)
class F10Response:
    """Parsed response from a 7615 TQLEX Entry."""

    entry: str
    request_body: Any
    error_code: int | None
    result_sets: tuple[F10ResultSet, ...]
    raw: dict[str, Any] = field(repr=False)

    @property
    def ok(self) -> bool:
        """Whether the gateway reported success."""

        return self.error_code in (None, 0)

    @property
    def tables(self) -> tuple[F10ResultSet, ...]:
        """Alias for result_sets, nicer for product-level examples."""

        return self.result_sets

    @property
    def first_table(self) -> F10ResultSet | None:
        """Return the first table, or None when the response has no table."""

        return self.result_sets[0] if self.result_sets else None

    @property
    def rows(self) -> tuple[dict[str, Any], ...]:
        """Rows from the first table. Empty tuple if no table exists."""

        first = self.first_table
        return first.rows if first is not None else ()

    def first_row(self) -> dict[str, Any] | None:
        """Return the first row in the first table."""

        first = self.first_table
        return first.first() if first is not None else None
