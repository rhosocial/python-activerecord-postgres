# src/rhosocial/activerecord/backend/impl/postgres/expression/hinted_statements.py
"""Statements that carry pg_hint_plan hints.

A pg_hint_plan hint is a comment attached to *one statement*, not a statement
of its own.  So rather than wrapping a query in a second node, each of the
four DML/DQL statements gets a derived class that carries the hint payloads
beside its own fields, and ``EXPLAIN`` gets one too (the comment belongs
before the ``EXPLAIN`` keyword, which is the position pg_hint_plan documents).

The derived classes declare the base class's full ``__init__`` signature --
:meth:`~...bases.BaseExpression.get_params` reads the constructor parameters
off the class's own ``__init__``, so a ``*args, **kwargs`` shortcut would
report nothing -- and add one keyword-only ``hints`` parameter.
``format_method`` is inherited unchanged: the derived class dispatches to the
same formatter the base does, and that formatter is where the prefix happens.
"""

from typing import Dict, List, Optional, Tuple, TYPE_CHECKING, Union

from rhosocial.activerecord.backend.expression.bases import (
    BaseExpression,
    SQLPredicate,
)
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.query_parts import (
    ForUpdateClause,
    GroupByHavingClause,
    LimitOffsetClause,
    OrderByClause,
    QualifyClause,
    WhereClause,
)
from rhosocial.activerecord.backend.expression.statements._types import (
    FromSourceType,
)
from rhosocial.activerecord.backend.expression.statements.dml import (
    DeleteStatement,
    InsertDataSource,
    InsertStatement,
    OnConflictClause,
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

from .hint import PostgresHint, assert_hint_payload, hint_payload

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression.core import Subquery
    from rhosocial.activerecord.backend.expression.query_parts import (
        JoinClause,
        ReturningClause,
    )
    from rhosocial.activerecord.backend.expression.query_sources import (
        LateralExpression,
        SetOperation,
        TableFunctionExpression,
        ValuesExpression,
    )
    from rhosocial.activerecord.backend.expression.sources import NamedRelationRef

#: What a hinted statement accepts: the builder's dataclass or the dictionary
#: an artifact decodes back into that dataclass's :meth:`as_dict` form.
HintPayload = Union[PostgresHint, dict]


def _normalise_hints(hints: Tuple[HintPayload, ...]) -> tuple:
    """The hint payloads a statement carries, each in its canonical dict form.

    An artifact decodes the payload's arrays as lists, so :func:`hint_payload`
    normalises nested sequences to tuples.  A value that is neither a
    :class:`PostgresHint` nor a dict is kept as given; ``check_renderable``
    refuses it at render time rather than the constructor failing, because a
    node must always build.
    """
    return tuple(
        hint_payload(hint) if isinstance(hint, (PostgresHint, dict)) else hint
        for hint in hints
    )


class _HintedStatement:
    """The checks every hinted statement shares.

    Listed after the core statement in each derived class's bases so the core
    node stays the first base: that is what makes the class a *derived* node
    (inheriting ``format_method`` and ``statement_type``) rather than a
    declaration of its own kind.  Its method is called explicitly.
    """

    hints: tuple

    def _check_hints(self) -> None:
        """A hinted statement needs at least one well-formed hint phrase."""
        if not self.hints:
            raise TypeError("At least one hint is required")
        for hint in self.hints:
            assert_hint_payload(hint)


class PostgresHintedQueryStatement(QueryStatement, _HintedStatement):
    """A ``SELECT`` carrying pg_hint_plan hints."""

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
        hints: Tuple[HintPayload, ...] = (),
    ):
        super().__init__(
            dialect,
            select=select,
            from_=from_,
            where=where,
            group_by_having=group_by_having,
            order_by=order_by,
            qualify=qualify,
            limit_offset=limit_offset,
            for_update=for_update,
            select_modifier=select_modifier,
        )
        self.hints = _normalise_hints(hints)

    def check_renderable(self) -> None:
        self._check_hints()


class PostgresHintedInsertStatement(InsertStatement, _HintedStatement):
    """An ``INSERT`` carrying pg_hint_plan hints."""

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
        hints: Tuple[HintPayload, ...] = (),
    ):
        super().__init__(
            dialect,
            into,
            source,
            columns,
            on_conflict=on_conflict,
            returning=returning,
            or_replace=or_replace,
            or_ignore=or_ignore,
        )
        self.hints = _normalise_hints(hints)

    def check_renderable(self) -> None:
        self._check_hints()


class PostgresHintedUpdateStatement(UpdateStatement, _HintedStatement):
    """An ``UPDATE`` carrying pg_hint_plan hints."""

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
        hints: Tuple[HintPayload, ...] = (),
    ):
        super().__init__(
            dialect,
            table,
            assignments,
            from_=from_,
            where=where,
            returning=returning,
        )
        self.hints = _normalise_hints(hints)

    def check_renderable(self) -> None:
        self._check_hints()


class PostgresHintedDeleteStatement(DeleteStatement, _HintedStatement):
    """A ``DELETE`` carrying pg_hint_plan hints."""

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
        hints: Tuple[HintPayload, ...] = (),
    ):
        super().__init__(dialect, tables, using=using, where=where, returning=returning)
        self.hints = _normalise_hints(hints)

    def check_renderable(self) -> None:
        self._check_hints()


class PostgresHintedExplainStatement(ExplainStatement, _HintedStatement):
    """An ``EXPLAIN`` carrying pg_hint_plan hints before its keyword."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        statement: "BaseExpression",
        options: Optional[ExplainOptions] = None,
        *,
        hints: Tuple[HintPayload, ...] = (),
    ):
        super().__init__(dialect, statement, options)
        self.hints = _normalise_hints(hints)

    def check_renderable(self) -> None:
        self._check_hints()
        if getattr(self.statement, "hints", ()):
            raise TypeError(
                "EXPLAIN cannot carry hints for a statement that already "
                "carries its own: the comment position would be ambiguous"
            )


__all__ = [
    "HintPayload",
    "PostgresHintedDeleteStatement",
    "PostgresHintedExplainStatement",
    "PostgresHintedInsertStatement",
    "PostgresHintedQueryStatement",
    "PostgresHintedUpdateStatement",
]
