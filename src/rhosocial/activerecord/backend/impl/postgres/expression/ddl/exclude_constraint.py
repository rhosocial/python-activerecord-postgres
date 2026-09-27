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
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        name: Optional[str] = None,
        elements: Optional[List[Tuple[Any, str]]] = None,
        *,
        using: str = "gist",
        where: Optional[SQLPredicate] = None,
        deferrable: Optional[bool] = None,
        initially_deferred: Optional[bool] = None,
        validation: Any = None,
        enforced: Optional[bool] = None,
    ):
        super().__init__(
            dialect,
            TableConstraintType.EXCLUDE,
            name=name,
            deferrable=deferrable,
            initially_deferred=initially_deferred,
            validation=validation,
            enforced=enforced,
        )
        self.elements = elements or []
        self.using = using
        self.where = where
