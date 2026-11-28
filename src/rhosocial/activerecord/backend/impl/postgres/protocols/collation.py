# src/rhosocial/activerecord/backend/impl/postgres/protocols/collation.py
"""PostgreSQL collation feature support protocol."""

from typing import Protocol, runtime_checkable, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.functions import CollateFunction


@runtime_checkable
class PostgresCollationSupport(Protocol):
    """PostgreSQL collation feature support protocol."""

    def supports_collate_expression(self)-> bool: ...

    def format_collation_name(self, expr: "CollateFunction") -> str: ...
