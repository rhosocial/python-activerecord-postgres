# src/rhosocial/activerecord/backend/impl/postgres/expression/statements/hinted_statements.py
"""PostgreSQL's own statements for core classes that need an optimizer-hint list.

Optimizer hints are PostgreSQL-specific syntax (pg_hint_plan), so the
``hints`` parameter has no business on a core statement: core expressions
carry the parameters their own semantics needs, and a dialect may add one only
by deriving its own class (backend-expression-dialect-capability §3.1, case B
of backend-expression-families §5.5).

These are the backend's statements, not hint-specific ones: each is core's
statement with one PostgreSQL-only parameter added, so a second PostgreSQL-only
parameter tomorrow joins the same class rather than spawning a parallel one.
The base parameters are untouched and forwarded verbatim, because
``get_params()`` reads this class's own ``__init__`` signature: an artifact
must name every constructor parameter of the class it rebuilt.  ``format_method``
is inherited from the base, so the derivation adds no expression/formatter pair
and no kind logic; the PostgreSQL formatter overrides in
:class:`~...mixins.extensions.pg_hint_plan.PostgresHintedStatementsMixin`
handle both the generic expression and the derived one.

Two of PostgreSQL's eight hinted statements were already derived before this
change and are extended in place, not duplicated here:
``PostgresCreateMaterializedViewStatement`` (``CREATE MATERIALIZED VIEW ... AS``)
and ``PostgresRefreshMaterializedViewStatement`` (``REFRESH MATERIALIZED VIEW``),
both in :mod:`...expression.statements.ddl.mv`.

A payload is a plain dictionary built by
:mod:`~...functions.pg_hint_plan`.  It is normalised here through
:func:`~...expression.query_parts.hint.normalise_hints`, so a payload decoded from an
artifact (nested arrays arrive as lists) compares equal to the one that was
encoded.
"""

from typing import Any, Dict, List, Mapping, Optional, Tuple, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.query_parts import (
    ForUpdateClause,
    GroupByHavingClause,
    LimitOffsetClause,
    OrderByClause,
    QualifyClause,
    WhereClause,
)
from rhosocial.activerecord.backend.expression.statements._types import FromSourceType
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    ColumnDefinition,
    CreateTableAsStatement,
)
from rhosocial.activerecord.backend.expression.statements.dml import (
    DeleteStatement,
    InsertDataSource,
    InsertStatement,
    OnConflictClause,
    ReturningClause,
    UpdateStatement,
)
from rhosocial.activerecord.backend.expression.statements.dql import (
    QueryStatement,
    SelectModifier,
)
from rhosocial.activerecord.backend.expression.statements.explain import (
    ExplainOptions,
    ExplainStatement,
)

from ..query_parts.hint import normalise_hints

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression.bases.bases import BaseExpression, SQLPredicate
    from rhosocial.activerecord.backend.expression.sources.subquery import Subquery
    from rhosocial.activerecord.backend.expression.objects import Table
    from rhosocial.activerecord.backend.expression.query_parts import JoinClause
    from rhosocial.activerecord.backend.expression.sources.query_sources import (
        LateralExpression,
        SetOperation,
        TableFunctionExpression,
        ValuesExpression,
    )
    from rhosocial.activerecord.backend.expression.sources import NamedRelationRef
    from rhosocial.activerecord.backend.expression.statements.ddl_table import (
        StorageOptionsExpression,
    )

#: The hint payload list every derived statement stores. ``Mapping`` rather
#: than ``dict`` because a caller may pass any mapping the renderer can read.
HintList = Tuple[Mapping[str, Any], ...]


class PostgresQueryStatement(QueryStatement):
    """A ``SELECT`` that may carry pg_hint_plan hints."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        select: List["BaseExpression"],
        from_: Optional[
            Union[
                FromSourceType,
                List[FromSourceType],
            ]
        ] = None,
        where: Optional[Union["SQLPredicate", "WhereClause"]] = None,
        group_by_having: Optional["GroupByHavingClause"] = None,
        order_by: Optional["OrderByClause"] = None,
        qualify: Optional["QualifyClause"] = None,
        limit_offset: Optional["LimitOffsetClause"] = None,
        for_update: Optional["ForUpdateClause"] = None,
        select_modifier: Optional[SelectModifier] = None,
        *,
        hints: HintList = (),
    ):
        super().__init__(
            dialect,
            select,
            from_=from_,
            where=where,
            group_by_having=group_by_having,
            order_by=order_by,
            qualify=qualify,
            limit_offset=limit_offset,
            for_update=for_update,
            select_modifier=select_modifier,
        )
        self.hints = normalise_hints(hints)


class PostgresInsertStatement(InsertStatement):
    """An ``INSERT`` that may carry pg_hint_plan hints."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        into: "Table",
        source: InsertDataSource,
        columns: Optional[List[str]] = None,
        *,
        on_conflict: Optional[Union[OnConflictClause, List[OnConflictClause]]] = None,
        returning: Optional["ReturningClause"] = None,
        or_replace: bool = False,
        or_ignore: bool = False,
        hints: HintList = (),
    ):
        super().__init__(
            dialect,
            into=into,
            source=source,
            columns=columns,
            on_conflict=on_conflict,
            returning=returning,
            or_replace=or_replace,
            or_ignore=or_ignore,
        )
        self.hints = normalise_hints(hints)


class PostgresUpdateStatement(UpdateStatement):
    """An ``UPDATE`` that may carry pg_hint_plan hints."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table: "Table",
        assignments: Dict[str, "BaseExpression"],
        *,
        from_: Optional[
            Union[
                "NamedRelationRef",
                "Subquery",
                "SetOperation",
                "JoinClause",
                List[
                    Union[
                        "NamedRelationRef",
                        "Subquery",
                        "SetOperation",
                        "JoinClause",
                        "ValuesExpression",
                        "TableFunctionExpression",
                        "LateralExpression",
                    ]
                ],
            ]
        ] = None,
        where: Optional[Union["SQLPredicate", "WhereClause"]] = None,
        returning: Optional["ReturningClause"] = None,
        hints: HintList = (),
    ):
        super().__init__(
            dialect,
            table,
            assignments,
            from_=from_,
            where=where,
            returning=returning,
        )
        self.hints = normalise_hints(hints)


class PostgresDeleteStatement(DeleteStatement):
    """A ``DELETE`` that may carry pg_hint_plan hints."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        tables: Union["Table", List["Table"]],
        *,
        using: Optional[
            Union[
                "NamedRelationRef",
                "Subquery",
                "SetOperation",
                "JoinClause",
                List[
                    Union[
                        "NamedRelationRef",
                        "Subquery",
                        "SetOperation",
                        "JoinClause",
                        "ValuesExpression",
                        "TableFunctionExpression",
                        "LateralExpression",
                    ]
                ],
            ]
        ] = None,
        where: Optional[Union["SQLPredicate", "WhereClause"]] = None,
        returning: Optional["ReturningClause"] = None,
        hints: HintList = (),
    ):
        super().__init__(
            dialect,
            tables,
            using=using,
            where=where,
            returning=returning,
        )
        self.hints = normalise_hints(hints)


class PostgresExplainStatement(ExplainStatement):
    """An ``EXPLAIN`` that may carry pg_hint_plan hints."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        statement: "BaseExpression",
        options: Optional[ExplainOptions] = None,
        *,
        hints: HintList = (),
    ):
        super().__init__(dialect, statement, options=options)
        self.hints = normalise_hints(hints)


class PostgresCreateTableAsStatement(CreateTableAsStatement):
    """``CREATE TABLE ... AS`` with a head hint pg_hint_plan honours."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table: "Table",
        as_query: "QueryStatement",
        *,
        columns: Optional[List[ColumnDefinition]] = None,
        temporary: bool = False,
        if_not_exists: bool = False,
        storage_options: Optional["StorageOptionsExpression"] = None,
        with_data: bool = False,
        no_data: bool = False,
        hints: HintList = (),
    ):
        super().__init__(
            dialect,
            table,
            as_query,
            columns=columns,
            temporary=temporary,
            if_not_exists=if_not_exists,
            storage_options=storage_options,
            with_data=with_data,
            no_data=no_data,
        )
        self.hints = normalise_hints(hints)


__all__ = [
    "HintList",
    "PostgresCreateTableAsStatement",
    "PostgresDeleteStatement",
    "PostgresExplainStatement",
    "PostgresInsertStatement",
    "PostgresQueryStatement",
    "PostgresUpdateStatement",
]
