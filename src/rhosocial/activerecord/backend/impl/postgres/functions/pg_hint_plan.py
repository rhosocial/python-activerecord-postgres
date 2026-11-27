# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_hint_plan.py
"""
pg_hint_plan hint builders.

Builders return :class:`PostgresHint` payloads, not expression nodes: a hint
is a comment carried by a statement, so the phrase is rendered by the dialect
formatter and never assembled here.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.impl.postgres.expression.hint import (
    CorrectionKind,
    HintKind,
    PostgresHint,
)
from rhosocial.activerecord.backend.impl.postgres.expression.load_library import (
    PostgresLoadLibraryExpression,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def hint_seq_scan(dialect: "SQLDialectBase", table: str) -> PostgresHint:
    """``SeqScan(table)``."""
    return PostgresHint(HintKind.SEQ_SCAN, tables=(table,))


def hint_tid_scan(dialect: "SQLDialectBase", table: str) -> PostgresHint:
    """``TidScan(table)``."""
    return PostgresHint(HintKind.TID_SCAN, tables=(table,))


def hint_index_scan(
    dialect: "SQLDialectBase", table: str, *indexes: str
) -> PostgresHint:
    """``IndexScan(table [index ...])``."""
    return PostgresHint(HintKind.INDEX_SCAN, tables=(table,), indexes=indexes)


def hint_index_only_scan(
    dialect: "SQLDialectBase", table: str, *indexes: str
) -> PostgresHint:
    """``IndexOnlyScan(table [index ...])``."""
    return PostgresHint(HintKind.INDEX_ONLY_SCAN, tables=(table,), indexes=indexes)


def hint_bitmap_scan(
    dialect: "SQLDialectBase", table: str, *indexes: str
) -> PostgresHint:
    """``BitmapScan(table [index ...])``."""
    return PostgresHint(HintKind.BITMAP_SCAN, tables=(table,), indexes=indexes)


def hint_no_seq_scan(dialect: "SQLDialectBase", table: str) -> PostgresHint:
    """``NoSeqScan(table)``."""
    return PostgresHint(HintKind.NO_SEQ_SCAN, tables=(table,))


def hint_no_index_scan(dialect: "SQLDialectBase", table: str) -> PostgresHint:
    """``NoIndexScan(table)``."""
    return PostgresHint(HintKind.NO_INDEX_SCAN, tables=(table,))


def hint_hash_join(dialect: "SQLDialectBase", *tables: str) -> PostgresHint:
    """``HashJoin(table table ...)``."""
    return PostgresHint(HintKind.HASH_JOIN, tables=tables)


def hint_merge_join(dialect: "SQLDialectBase", *tables: str) -> PostgresHint:
    """``MergeJoin(table table ...)``."""
    return PostgresHint(HintKind.MERGE_JOIN, tables=tables)


def hint_nest_loop(dialect: "SQLDialectBase", *tables: str) -> PostgresHint:
    """``NestLoop(table table ...)``."""
    return PostgresHint(HintKind.NEST_LOOP, tables=tables)


def hint_leading(
    dialect: "SQLDialectBase", *targets: object
) -> PostgresHint:
    """``Leading(...)``; pass :class:`HintGroup` for nested join pairs."""
    return PostgresHint(HintKind.LEADING, sequence=targets)


def hint_rows(
    dialect: "SQLDialectBase",
    kind: CorrectionKind,
    amount: float,
    *tables: str,
) -> PostgresHint:
    """``Rows(table table ... correction)`` with a typed correction operator."""
    return PostgresHint(
        HintKind.ROWS,
        tables=tables,
        correction_kind=kind,
        correction_amount=amount,
    )


def hint_parallel(
    dialect: "SQLDialectBase", table: str, workers: int, soft: bool = True
) -> PostgresHint:
    """``Parallel(table workers [soft|hard])``."""
    return PostgresHint(
        HintKind.PARALLEL, tables=(table,), workers=workers, soft=soft
    )


def hint_set(
    dialect: "SQLDialectBase", guc: str, value: object
) -> PostgresHint:
    """``Set(GUC-param value)`` for planner-time GUC overrides."""
    return PostgresHint(HintKind.SET, guc=guc, guc_value=value)


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
    "load_pg_hint_plan",
]
