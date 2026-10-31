# tests/providers/fixtures/_common.py
"""Shared helpers for the PostgreSQL DDL expression fixtures.

Provides the ``drop_table`` helper used by all feature providers.
"""

from __future__ import annotations

from typing import Tuple

from rhosocial.activerecord.backend.expression import (
    CreateTableStatement,
    DropTableStatement,
)
from rhosocial.activerecord.backend.expression.objects import Table


def drop_table(dialect, table_name: str) -> DropTableStatement:
    """Build a ``DROP TABLE IF EXISTS table_name CASCADE`` expression."""
    return DropTableStatement(
        dialect=dialect,
        table=Table(dialect, table_name),
        if_exists=True,
    )


def create_table_sql(expr: CreateTableStatement) -> Tuple[str, tuple]:
    """Generate canonical PostgreSQL DDL for a :class:`CreateTableStatement`.

    For PostgreSQL no post-processing is required (unlike MySQL where storage
    option quotes must be stripped).  This pass-through wrapper exists so the
    call site is the same as the MySQL counterpart.
    """
    return expr.to_sql()
