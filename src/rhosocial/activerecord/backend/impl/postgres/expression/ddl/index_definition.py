# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/index_definition.py
"""PostgreSQL-specific index expressions.

PostgreSQL adds index options with no generic equivalent:

* ``opclasses`` — operator classes per column (e.g. ``{"a": "text_pattern_ops"}``).
  A key may also be an expression node, so an operator class can be attached to
  a functional index column (e.g. ``{data_expr: "jsonb_path_ops"}``).
* ``nulls_not_distinct`` — ``NULLS NOT DISTINCT`` on a UNIQUE index (PG 15+).
* ``with_options`` — index storage parameters, e.g. ``{"fillfactor": 70}``.

They live on ``PostgresCreateIndexExpression`` (standalone ``CREATE INDEX``)
and ``PostgresIndexDefinition`` (inline table index definition), both deriving
their generic core counterparts, and are rendered by the PostgreSQL
``PostgresIndexMixin`` overrides.
"""

from typing import Any, Dict, List, Optional

from rhosocial.activerecord.backend.expression.bases import SQLPredicate
from rhosocial.activerecord.backend.expression.objects import Index, Table
from rhosocial.activerecord.backend.expression.statements.ddl_index import (
    CreateIndexStatement,
    DropIndexStatement,
)
from rhosocial.activerecord.backend.expression.statements.ddl_table import IndexDefinition

from .index_type import PostgresIndexType


class PostgresCreateIndexExpression(CreateIndexStatement):
    """A PostgreSQL ``CREATE INDEX`` statement extending the generic one.

    Adds the PostgreSQL-only typed options consumed by
    ``PostgresIndexMixin.format_create_index_statement``:

    * ``opclasses`` — operator classes per column (e.g. ``{"a": "text_pattern_ops"}``).
  A key may also be an expression node, so an operator class can be attached to
  a functional index column (e.g. ``{data_expr: "jsonb_path_ops"}``).
    * ``nulls_not_distinct`` — ``NULLS NOT DISTINCT`` on a UNIQUE index (PG 15+).
    * ``with_options`` — index storage parameters, e.g. ``{"fillfactor": 70}``.
    """

    def __init__(
        self,
        dialect: Any,
        index: Index,
        table: Table,
        columns: List[Any],
        unique: bool = False,
        if_not_exists: bool = False,
        index_type: Optional[str] = None,
        where: Optional[SQLPredicate] = None,
        include: Optional[List[str]] = None,
        tablespace: Optional[str] = None,
        concurrent: bool = False,
        *,
        opclasses: Optional[Dict[Any, str]] = None,
        nulls_not_distinct: bool = False,
        with_options: Optional[Dict[str, Any]] = None,
        expression_opclasses: Optional[List[tuple]] = None,
    ):
        super().__init__(
            dialect,
            index=index,
            table=table,
            columns=columns,
            unique=unique,
            if_not_exists=if_not_exists,
            index_type=index_type,
            where=where,
            include=include,
            tablespace=tablespace,
            concurrent=concurrent,
        )
        # Keep the caller's string spelling (the artifact and the SQL are
        # byte-identical with it) but refuse a word that is not an access
        # method this backend can render.
        self.index_type = (
            PostgresIndexType.coerce(index_type) if index_type else None
        )
        self.opclasses = opclasses or {}
        self.nulls_not_distinct = nulls_not_distinct
        self.with_options = with_options or {}
        # Expression nodes are unhashable, so a dict cannot key them. Operator
        # classes for functional columns are bound by node identity in a list
        # of ``(node, opclass)`` pairs instead.
        self.expression_opclasses = expression_opclasses or []


class PostgresIndexDefinition(IndexDefinition):
    """A PostgreSQL index definition extending the generic one.

    Adds the PostgreSQL-only ``opclasses`` / ``nulls_not_distinct`` / ``with``
    options.
    """

    def __init__(
        self,
        dialect: Any,
        name: str,
        columns: List[Any],
        *,
        unique: bool = False,
        type: Optional[str] = None,
        partial_condition: Optional[SQLPredicate] = None,
        include_columns: Optional[List[str]] = None,
        opclasses: Optional[Dict[Any, str]] = None,
        nulls_not_distinct: bool = False,
        with_options: Optional[Dict[str, Any]] = None,
        expression_opclasses: Optional[List[tuple]] = None,
    ):
        super().__init__(
            dialect,
            name=name,
            columns=columns,
            unique=unique,
            type=type,
            partial_condition=partial_condition,
            include_columns=include_columns,
        )
        self.type = PostgresIndexType.coerce(type) if type else None
        self.opclasses = opclasses or {}
        self.nulls_not_distinct = nulls_not_distinct
        self.with_options = with_options or {}
        self.expression_opclasses = expression_opclasses or []


class PostgresDropIndexExpression(DropIndexStatement):
    """A PostgreSQL DROP INDEX statement extending the generic one.

    Adds the PostgreSQL-only ``concurrent`` flag (``DROP INDEX CONCURRENTLY``,
    PG 18+); the generic ``DropIndexStatement`` already carries ``concurrent``,
    so this subclass mainly marks PostgreSQL ownership for the DDL gates.
    """

