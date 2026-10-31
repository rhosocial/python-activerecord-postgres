# src/rhosocial/activerecord/backend/impl/postgres/expression/wildcard.py
"""PostgreSQL's own row-count wildcard: ``COUNT(t.*)``.

Core splits the one asterisk into two by grammar position (spec 4.10): the
select-list ``*`` expands columns and belongs to ``AllColumnsWildcard``, while
``COUNT(*)``'s asterisk counts rows and belongs to ``RowCountWildcard``.  Core
draws the line at the bare asterisk -- ``RowCountWildcard`` accepts no relation,
because standard ``COUNT(*)`` never qualifies it.

PostgreSQL has a second spelling that core deliberately leaves to the backends
that have it: ``COUNT(t.*)``, which counts non-NULL rows of ``t`` rather than
rows of the join.  It is not the same question as ``COUNT(*)`` -- a LEFT JOIN
with a matching-but-NULL outer row answers differently -- so it is a different
node rather than a flag, and it belongs here rather than in core.

The class name says ``Postgres``, not ``PostgreSQL``: a run of capitals is one
word when the kind is derived, so ``PostgreSQLRowCountWildcard`` would produce
``postgre_sql_row_count_wildcard`` -- a kind that disagrees with the backend
identifier every other class here uses (rules §3).
"""

from typing import Optional, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.core import RowCountWildcard

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression.sources import NamedRelationRef


class PostgresRowCountWildcard(RowCountWildcard):
    """``COUNT(t.*)`` -- rows of *relation* that are not NULL, not rows of the join.

    Args:
        dialect: The dialect that renders the node.
        relation: The relation whose columns the argument expands. The
            qualifier rendered is the one the query itself uses: the alias when
            the reference carries one, the relation's own qualified name
            otherwise -- the same rule ``AllColumnsWildcard`` follows, because it
            is the same "which name does this query call it" question.

    With no relation this renders ``*`` and is the plain ``COUNT(*)``; the
    relation is what makes the qualified form possible at all.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        relation: Optional["NamedRelationRef"] = None,
    ):
        super().__init__(dialect)
        self.relation = relation


__all__ = ["PostgresRowCountWildcard"]
