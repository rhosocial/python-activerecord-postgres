# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/exclude_constraint.py
"""PostgreSQL EXCLUDE constraint definition.

PostgreSQL's ``EXCLUDE`` table constraint has no generic equivalent, and its
grammar is richer than the core branch's three fields: the excluded elements
are ``(expression, operator)`` pairs rather than one shared operator over a
column list, and the whole thing may carry a partial-exclusion predicate.

So the branch is :class:`PostgresExcludeBranch` -- core's
:class:`TableExcludeConstraint` with those two PostgreSQL additions -- and
:class:`PostgresExcludeConstraint` is the production-level sugar that puts
``CONSTRAINT <name>`` and the traits where the grammar puts them. It mirrors
core's own ``ForeignKeyConstraint``, which is sugar over
``TableForeignKeyConstraint`` in exactly the same way, so callers keep writing
``PostgresExcludeConstraint(dialect, name=..., elements=[...])`` instead of
assembling the production themselves.
"""

from typing import Any, List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import SQLPredicate
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    TableConstraint,
    TableExcludeConstraint,
)

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


class PostgresExcludeBranch(TableExcludeConstraint):
    """The EXCLUDE branch as PostgreSQL spells it.

    ``elements`` is a list of ``(expression, operator)`` pairs where the
    expression is either a column name (``str``) or a ``BaseExpression``;
    ``using`` is the index access method and ``where`` the optional
    partial-exclusion predicate. ``columns`` and ``operator`` are still filled
    in from the first element so the inherited accessors keep answering.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        elements: Optional[List[Tuple[Any, str]]] = None,
        *,
        using: str = "gist",
        where: Optional[SQLPredicate] = None,
    ):
        elements = list(elements or [])
        columns = [element[0] for element in elements if isinstance(element[0], str)]
        operator = elements[0][1] if elements else "="
        super().__init__(dialect, columns, operator, using or None)
        self.elements = elements
        self.using = using
        self.where = where


class PostgresExcludeConstraint(TableConstraint):
    """A PostgreSQL EXCLUDE constraint: the production plus PostgreSQL's branch.

    ``name`` and the two-spelling traits are the production's, as the grammar
    has it; ``elements`` / ``using`` / ``where`` are the branch's, and are
    exposed here as views rather than stored twice.

    The deferrability and enforcement options follow the round's two-parameter
    shape (``deferrable`` / ``not_deferrable``, ``initially_deferred`` /
    ``initially_immediate``, ``enforced`` / ``not_enforced``); setting both of a
    pair raises ``ValueError`` at construction through the generic base. What
    the EXCLUDE branch cannot carry is refused there too: it is neither an
    ENFORCABLE nor a DEFERRABLE branch in SQL:2016's list, so
    ``PostgresExcludeConstraint(dialect, elements=[...], enforced=True)`` is a
    ``ValueError`` at construction rather than a clause the server would answer
    with ``0A000`` at execution time.
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: Optional[str] = None,
        elements: Optional[List[Tuple[Any, str]]] = None,
        *,
        using: str = "gist",
        where: Optional[SQLPredicate] = None,
        deferrable: bool = False,
        not_deferrable: bool = False,
        initially_deferred: bool = False,
        initially_immediate: bool = False,
        validation: Any = None,
    ):
        super().__init__(
            dialect,
            branch=PostgresExcludeBranch(dialect, elements, using=using, where=where),
            name=name,
            deferrable=deferrable,
            not_deferrable=not_deferrable,
            initially_deferred=initially_deferred,
            initially_immediate=initially_immediate,
            validation=validation,
        )

    @property
    def elements(self) -> List[Tuple[Any, str]]:
        """The ``(expression, operator)`` pairs, as the branch records them."""
        return self.branch.elements

    @property
    def using(self) -> str:
        """The index access method, as the branch records it."""
        return self.branch.using

    @property
    def where(self) -> Optional[SQLPredicate]:
        """The partial-exclusion predicate, as the branch records it."""
        return self.branch.where