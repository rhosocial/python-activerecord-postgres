# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_hint_plan.py
"""
pg_hint_plan hint builders.

Builders return plain dictionary payloads (``HintPayload``), not expression
nodes: a hint is a comment carried by a statement, so the phrase is rendered by
the dialect formatter and never assembled here. Every payload carries every key
(``None``/``()`` where a field does not apply), which is what makes it
self-describing on the artifact wire.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.impl.postgres.expression.hint import (
    CorrectionKind,
    HintPayload,
    HintKind,
    make_hint_payload,
)
from rhosocial.activerecord.backend.impl.postgres.expression.load_library import (
    PostgresLoadLibraryExpression,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def hint_seq_scan(dialect: "SQLDialectBase", table: str) -> "HintPayload":
    """``SeqScan(table)``."""
    return make_hint_payload(HintKind.SEQ_SCAN, tables=(table,))


def hint_tid_scan(dialect: "SQLDialectBase", table: str) -> "HintPayload":
    """``TidScan(table)``."""
    return make_hint_payload(HintKind.TID_SCAN, tables=(table,))


def hint_index_scan(
    dialect: "SQLDialectBase", table: str, *indexes: str
) -> "HintPayload":
    """``IndexScan(table [index ...])``."""
    return make_hint_payload(HintKind.INDEX_SCAN, tables=(table,), indexes=indexes)


def hint_index_only_scan(
    dialect: "SQLDialectBase", table: str, *indexes: str
) -> "HintPayload":
    """``IndexOnlyScan(table [index ...])``."""
    return make_hint_payload(HintKind.INDEX_ONLY_SCAN, tables=(table,), indexes=indexes)


def hint_bitmap_scan(
    dialect: "SQLDialectBase", table: str, *indexes: str
) -> "HintPayload":
    """``BitmapScan(table [index ...])``."""
    return make_hint_payload(HintKind.BITMAP_SCAN, tables=(table,), indexes=indexes)


def hint_no_seq_scan(dialect: "SQLDialectBase", table: str) -> "HintPayload":
    """``NoSeqScan(table)``."""
    return make_hint_payload(HintKind.NO_SEQ_SCAN, tables=(table,))


def hint_no_index_scan(dialect: "SQLDialectBase", table: str) -> "HintPayload":
    """``NoIndexScan(table)``."""
    return make_hint_payload(HintKind.NO_INDEX_SCAN, tables=(table,))


def hint_hash_join(dialect: "SQLDialectBase", *tables: str) -> "HintPayload":
    """``HashJoin(table table ...)``."""
    return make_hint_payload(HintKind.HASH_JOIN, tables=tables)


def hint_merge_join(dialect: "SQLDialectBase", *tables: str) -> "HintPayload":
    """``MergeJoin(table table ...)``."""
    return make_hint_payload(HintKind.MERGE_JOIN, tables=tables)


def hint_nest_loop(dialect: "SQLDialectBase", *tables: str) -> "HintPayload":
    """``NestLoop(table table ...)``."""
    return make_hint_payload(HintKind.NEST_LOOP, tables=tables)


def hint_leading(
    dialect: "SQLDialectBase", *targets: object
) -> "HintPayload":
    """``Leading(...)``; pass :class:`HintGroup` for nested join pairs."""
    return make_hint_payload(HintKind.LEADING, sequence=targets)


def hint_rows(
    dialect: "SQLDialectBase",
    kind: CorrectionKind,
    amount: float,
    *tables: str,
) -> "HintPayload":
    """``Rows(table table ... correction)`` with a typed correction operator."""
    return make_hint_payload(
        HintKind.ROWS,
        tables=tables,
        correction_kind=kind,
        correction_amount=amount,
    )


def hint_parallel(
    dialect: "SQLDialectBase", table: str, workers: int, soft: bool = True
) -> "HintPayload":
    """``Parallel(table workers [soft|hard])``."""
    return make_hint_payload(
        HintKind.PARALLEL, tables=(table,), workers=workers, soft=soft
    )


def hint_set(
    dialect: "SQLDialectBase", guc: str, value: object
) -> "HintPayload":
    """``Set(GUC-param value)`` for planner-time GUC overrides."""
    return make_hint_payload(HintKind.SET, guc=guc, guc_value=value)


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
