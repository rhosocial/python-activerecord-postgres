# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/column.py
"""PostgreSQL-specific column definition expressions.

PostgreSQL extends the standard column definition with per-column storage
attributes that have no generic equivalent:

* ``COMPRESSION <method>`` — column compression (PG 14+): ``pglz`` / ``lz4``.
* ``STORAGE {PLAIN|EXTERNAL|EXTENDED|MAIN|DEFAULT}`` — TOAST storage strategy.
* ``STATISTICS <n>`` — per-column statistics target (0..10000).

These live on ``PostgresColumnDefinition`` (deriving the generic
``ColumnDefinition``) and are rendered by the PostgreSQL
``format_column_definition`` override. They are declared through
``PostgresColumnOptions`` (deriving the generic ``ColumnOptions``).
"""

from enum import Enum
from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.statements import ColumnDefinition
from rhosocial.activerecord.base.ddl.options import ColumnOptions

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresColumnStorage",
    "PostgresColumnDefinition",
    "PostgresColumnOptions",
]


class PostgresColumnStorage(Enum):
    """PostgreSQL TOAST ``STORAGE`` strategies."""

    PLAIN = "PLAIN"
    EXTERNAL = "EXTERNAL"
    EXTENDED = "EXTENDED"
    MAIN = "MAIN"
    DEFAULT = "DEFAULT"


class PostgresColumnDefinition(ColumnDefinition):
    """A PostgreSQL column definition extending the generic one.

    Adds PostgreSQL-only typed attributes: ``compression``, ``storage`` and
    ``statistics``.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: str,
        data_type,
        constraints=None,
        comment: Optional[str] = None,
        generated_expression=None,
        attributes=None,
        *,
        compression: Optional[str] = None,
        storage: Optional[PostgresColumnStorage] = None,
        statistics: Optional[int] = None,
    ):
        super().__init__(
            dialect,
            name,
            data_type,
            constraints=constraints,
            comment=comment,
            generated_expression=generated_expression,
            attributes=attributes,
        )
        if storage is not None and not isinstance(storage, PostgresColumnStorage):
            raise TypeError(
                "storage must be a PostgresColumnStorage value, "
                f"got {type(storage).__name__}"
            )
        if statistics is not None and (
            not isinstance(statistics, int) or isinstance(statistics, bool)
            or statistics < 0 or statistics > 10000
        ):
            raise ValueError("statistics must be an integer in 0..10000")
        self.compression = compression
        self.storage = storage
        self.statistics = statistics


class PostgresColumnOptions(ColumnOptions):
    """PostgreSQL per-column options declaration."""

    def __init__(
        self,
        *,
        compression: Optional[str] = None,
        storage: Optional[PostgresColumnStorage] = None,
        statistics: Optional[int] = None,
    ):
        super().__init__()
        if storage is not None and not isinstance(storage, PostgresColumnStorage):
            raise TypeError(
                "storage must be a PostgresColumnStorage value, "
                f"got {type(storage).__name__}"
            )
        if statistics is not None and (
            not isinstance(statistics, int) or isinstance(statistics, bool)
            or statistics < 0 or statistics > 10000
        ):
            raise ValueError("statistics must be an integer in 0..10000")
        self.compression = compression
        self.storage = storage
        self.statistics = statistics

    def column_definition_class(self):
        """Build a ``PostgresColumnDefinition`` for these options."""
        return PostgresColumnDefinition

    def apply_to(self, column) -> None:
        """Transfer the PostgreSQL-only fields onto the column definition."""
        if not isinstance(column, PostgresColumnDefinition):
            raise TypeError(
                "PostgresColumnOptions.apply_to requires a PostgresColumnDefinition, "
                f"got {type(column).__name__}"
            )
        column.compression = self.compression
        column.storage = self.storage
        column.statistics = self.statistics
