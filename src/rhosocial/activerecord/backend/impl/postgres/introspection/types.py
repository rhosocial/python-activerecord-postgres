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
from typing import Any, Dict, List, Optional

from rhosocial.activerecord.backend.introspection.types import ColumnDataProfile

__all__ = ["SequenceInfo", "PostgresColumnDataProfile"]


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


@dataclass
class PostgresColumnDataProfile(ColumnDataProfile):
    """A column profile carrying the extra facts ``pg_stats`` supplies.

    Named fields rather than ``extra`` entries: these are PostgreSQL's own
    statistics, and a consumer that knows it asked PostgreSQL should be able to
    read them off the object instead of guessing at dictionary keys.

    ``correlation`` is the physical/logical ordering agreement the planner
    uses -- 1.0 means a scan reads the table in index order, near 0 means it
    does not. It is not a quality measure and not a hint. ``most_common_vals``
    and ``most_common_freqs`` are the kept value list and its shares, in
    lockstep; both are also what :meth:`sample_column_values` returns, and they
    are kept here because the profile is where a caller looks to decide whether
    asking for them is worth it.

    ``min_value``/``max_value`` are the true extremes: the parser merges the
    histogram's endpoints with the most-common values, since ANALYZE's
    histogram excludes the values it kept as MCVs.
    """

    #: Agreement between physical row order and the column's ordering.
    correlation: Optional[float] = None
    #: The most common values ANALYZE kept, in the column's own types.
    most_common_vals: List[Any] = field(default_factory=list)
    #: Their shares of the rows, in lockstep with ``most_common_vals``.
    most_common_freqs: List[float] = field(default_factory=list)
