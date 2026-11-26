# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_hint_plan.py
"""
pg_hint_plan hint builders.

Builders return typed :class:`PostgresHintExpression` nodes.  No SQL or hint
text is assembled here -- rendering belongs to the dialect formatters.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.impl.postgres.expression.hint import (
    CorrectionKind,
    HintKind,
    PostgresHintedQueryExpression,
    PostgresHintExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.load_library import (
    PostgresLoadLibraryExpression,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def hint_seq_scan(dialect: "SQLDialectBase", table: str) -> PostgresHintExpression:
    """``SeqScan(table)``."""
    return PostgresHintExpression(dialect, HintKind.SEQ_SCAN, tables=(table,))


def hint_tid_scan(dialect: "SQLDialectBase", table: str) -> PostgresHintExpression:
    """``TidScan(table)``."""
    return PostgresHintExpression(dialect, HintKind.TID_SCAN, tables=(table,))


def hint_index_scan(
    dialect: "SQLDialectBase", table: str, *indexes: str
) -> PostgresHintExpression:
    """``IndexScan(table [index ...])``."""
    return PostgresHintExpression(dialect, HintKind.INDEX_SCAN, tables=(table,), indexes=indexes)


def hint_index_only_scan(
    dialect: "SQLDialectBase", table: str, *indexes: str
) -> PostgresHintExpression:
    """``IndexOnlyScan(table [index ...])``."""
    return PostgresHintExpression(dialect, HintKind.INDEX_ONLY_SCAN, tables=(table,), indexes=indexes)


def hint_bitmap_scan(
    dialect: "SQLDialectBase", table: str, *indexes: str
) -> PostgresHintExpression:
    """``BitmapScan(table [index ...])``."""
    return PostgresHintExpression(dialect, HintKind.BITMAP_SCAN, tables=(table,), indexes=indexes)


def hint_no_seq_scan(dialect: "SQLDialectBase", table: str) -> PostgresHintExpression:
    """``NoSeqScan(table)``."""
    return PostgresHintExpression(dialect, HintKind.NO_SEQ_SCAN, tables=(table,))


def hint_no_index_scan(dialect: "SQLDialectBase", table: str) -> PostgresHintExpression:
    """``NoIndexScan(table)``."""
    return PostgresHintExpression(dialect, HintKind.NO_INDEX_SCAN, tables=(table,))


def hint_hash_join(dialect: "SQLDialectBase", *tables: str) -> PostgresHintExpression:
    """``HashJoin(table table ...)``."""
    return PostgresHintExpression(dialect, HintKind.HASH_JOIN, tables=tables)


def hint_merge_join(dialect: "SQLDialectBase", *tables: str) -> PostgresHintExpression:
    """``MergeJoin(table table ...)``."""
    return PostgresHintExpression(dialect, HintKind.MERGE_JOIN, tables=tables)


def hint_nest_loop(dialect: "SQLDialectBase", *tables: str) -> PostgresHintExpression:
    """``NestLoop(table table ...)``."""
    return PostgresHintExpression(dialect, HintKind.NEST_LOOP, tables=tables)


def hint_leading(
    dialect: "SQLDialectBase", *targets: object
) -> PostgresHintExpression:
    """``Leading(...)``; pass :class:`HintGroup` for nested join pairs."""
    return PostgresHintExpression(dialect, HintKind.LEADING, sequence=targets)


def hint_rows(
    dialect: "SQLDialectBase",
    kind: CorrectionKind,
    amount: float,
    *tables: str,
) -> PostgresHintExpression:
    """``Rows(table table ... correction)`` with a typed correction operator."""
    return PostgresHintExpression(
        dialect,
        HintKind.ROWS,
        tables=tables,
        correction_kind=kind,
        correction_amount=amount,
    )


def hint_parallel(
    dialect: "SQLDialectBase", table: str, workers: int, soft: bool = True
) -> PostgresHintExpression:
    """``Parallel(table workers [soft|hard])``."""
    return PostgresHintExpression(
        dialect, HintKind.PARALLEL, tables=(table,), workers=workers, soft=soft
    )


def hint_set(
    dialect: "SQLDialectBase", guc: str, value: object
) -> PostgresHintExpression:
    """``Set(GUC-param value)`` for planner-time GUC overrides."""
    return PostgresHintExpression(dialect, HintKind.SET, guc=guc, guc_value=value)


def pg_hint_plan_hinted_query(
    dialect: "SQLDialectBase",
    query,
    hints,
) -> PostgresHintedQueryExpression:
    """Wrap ``query`` in a validated pg_hint_plan hint comment."""
    return PostgresHintedQueryExpression(dialect, query, tuple(hints))


def load_pg_hint_plan(dialect: "SQLDialectBase") -> PostgresLoadLibraryExpression:
    """Build ``LOAD 'pg_hint_plan'`` for the current session.

    ``CREATE EXTENSION pg_hint_plan`` alone does not install the planner hook;
    the library must be loaded per session (or listed in
    ``shared_preload_libraries`` on the server).
    """
    return PostgresLoadLibraryExpression(dialect, "pg_hint_plan")


__all__ = [
    "CorrectionKind",
    "hint_seq_scan",
    "hint_tid_scan",
    "hint_index_scan",
    "hint_index_only_scan",
    "hint_bitmap_scan",
    "hint_no_seq_scan",
    "hint_no_index_scan",
    "hint_hash_join",
    "hint_merge_join",
    "hint_nest_loop",
    "hint_leading",
    "hint_rows",
    "hint_parallel",
    "hint_set",
    "pg_hint_plan_hinted_query",
    "load_pg_hint_plan",
]
