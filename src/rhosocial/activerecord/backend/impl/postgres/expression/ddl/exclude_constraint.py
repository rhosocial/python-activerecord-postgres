# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/exclude_constraint.py
"""PostgreSQL EXCLUDE constraint definition.

PostgreSQL adds the ``EXCLUDE`` table constraint with no generic equivalent.
The excluded elements (expression/operator pairs), the index access method and
the optional partial predicate live on ``PostgresExcludeConstraint`` (deriving
the generic ``TableConstraint``) and are rendered by the PostgreSQL
``_format_exclude_constraint``.
"""

from typing import Any, List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import SQLPredicate
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    TableConstraint,
    TableConstraintType,
)

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


class PostgresExcludeConstraint(TableConstraint):
    """A PostgreSQL EXCLUDE constraint extending the generic one.

    ``elements`` is a list of ``(expression, operator)`` pairs where the
    expression is either a column name (``str``) or a ``BaseExpression``;
    ``using`` selects the index access method (default ``gist``) and ``where``
    is an optional partial-exclusion predicate.

    The deferrability and enforcement options follow the round's two-parameter
    shape (``deferrable`` / ``not_deferrable``, ``initially_deferred`` /
    ``initially_immediate``, ``enforced`` / ``not_enforced``); setting both of a
    pair raises ``ValueError`` at construction through the generic base.
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
        enforced: bool = False,
        not_enforced: bool = False,
    ):
        super().__init__(
            dialect,
            TableConstraintType.EXCLUDE,
            name=name,
            deferrable=deferrable,
            not_deferrable=not_deferrable,
            initially_deferred=initially_deferred,
            initially_immediate=initially_immediate,
            validation=validation,
            enforced=enforced,
            not_enforced=not_enforced,
        )
        self.elements = elements or []
        self.using = using
        self.where = where
