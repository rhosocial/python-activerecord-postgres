# src/rhosocial/activerecord/backend/impl/postgres/introspection/types.py
"""PostgreSQL-specific introspection result types.

Core introspection covers the objects every backend shares: tables, views,
columns, indexes, foreign keys and triggers. A sequence is not one of those --
``information_schema`` has no sequence entry point that core models -- so its
row shape lives here, next to the code that reads it.

The type is frozen: a row read from the catalog is a snapshot, and a caller that
edited it would be lying about what the server reported.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

__all__ = ["SequenceInfo"]


@dataclass(frozen=True)
class SequenceInfo:
    """One sequence, as ``information_schema.sequences`` describes it.

    ``last_value`` comes from ``pg_sequences`` (PostgreSQL 10+) rather than
    from ``information_schema``, whose sequence view has no such column. It is
    ``None`` in three situations, none of which is an error:

    * the sequence has never handed out or written a value in this database;
    * the connected role has neither ``SELECT`` nor ``USAGE`` on it, so the
      catalog view withholds the value;
    * the server predates ``pg_sequences`` (PostgreSQL 9.x), where the join
      has no source and the query reports the column as absent.

    ``cache_size`` is withheld under the same conditions, for the same reasons.
    """

    name: str
    schema: str
    data_type: str = ""
    start_value: Optional[int] = None
    minimum_value: Optional[int] = None
    maximum_value: Optional[int] = None
    increment: Optional[int] = None
    cycle: bool = False
    cache_size: Optional[int] = None
    last_value: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)
