# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/index_definition.py
"""PostgreSQL-specific index expressions.

PostgreSQL adds index options with no generic equivalent:

* ``opclasses`` — operator classes per column (e.g. ``{"a": "text_pattern_ops"}``).
* ``nulls_not_distinct`` — ``NULLS NOT DISTINCT`` on a UNIQUE index (PG 15+).
* ``with_options`` — index storage parameters, e.g. ``{"fillfactor": 70}``.

They live on ``PostgresCreateIndexExpression`` (standalone ``CREATE INDEX``)
and ``PostgresIndexDefinition`` (inline table index definition), both deriving
their generic core counterparts, and are rendered by the PostgreSQL
``PostgresIndexMixin`` overrides.
"""

from typing import Any, Dict, List, Optional

from rhosocial.activerecord.backend.expression.bases import SQLPredicate
from rhosocial.activerecord.backend.expression.statements.ddl_index import (
    CreateIndexExpression,
    DropIndexExpression,
)
from rhosocial.activerecord.backend.expression.statements.ddl_table import IndexDefinition


class PostgresCreateIndexExpression(CreateIndexExpression):
    """A PostgreSQL ``CREATE INDEX`` statement extending the generic one.

    Adds the PostgreSQL-only typed options consumed by
    ``PostgresIndexMixin.format_create_index_statement``:

    * ``opclasses`` — operator classes per column (e.g. ``{"a": "text_pattern_ops"}``).
    * ``nulls_not_distinct`` — ``NULLS NOT DISTINCT`` on a UNIQUE index (PG 15+).
    * ``with_options`` — index storage parameters, e.g. ``{"fillfactor": 70}``.
    """

    def __init__(
        self,
        dialect: Any,
        index_name: str,
        table_name: str,
        columns: List[Any],
        unique: bool = False,
        if_not_exists: bool = False,
        index_type: Optional[str] = None,
        where: Optional[SQLPredicate] = None,
        include: Optional[List[str]] = None,
        tablespace: Optional[str] = None,
        concurrent: bool = False,
        *,
        opclasses: Optional[Dict[str, str]] = None,
        nulls_not_distinct: bool = False,
        with_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            dialect,
            index_name=index_name,
            table_name=table_name,
            columns=columns,
            unique=unique,
            if_not_exists=if_not_exists,
            index_type=index_type,
            where=where,
            include=include,
            tablespace=tablespace,
            concurrent=concurrent,
        )
        self.opclasses = opclasses or {}
        self.nulls_not_distinct = nulls_not_distinct
        self.with_options = with_options or {}


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
        opclasses: Optional[Dict[str, str]] = None,
        nulls_not_distinct: bool = False,
        with_options: Optional[Dict[str, Any]] = None,
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
        self.opclasses = opclasses or {}
        self.nulls_not_distinct = nulls_not_distinct
        self.with_options = with_options or {}


class PostgresDropIndexExpression(DropIndexExpression):
    """A PostgreSQL DROP INDEX statement extending the generic one.

    Adds the PostgreSQL-only ``concurrent`` flag (``DROP INDEX CONCURRENTLY``,
    PG 18+); the generic ``DropIndexExpression`` already carries ``concurrent``,
    so this subclass mainly marks PostgreSQL ownership for the DDL gates.
    """

