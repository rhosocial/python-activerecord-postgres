# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/constraint.py
"""PostgreSQL-specific constraint capability defaults.

Provides default implementations for PostgreSQL-proprietary constraint
features that are not part of the SQL standard.

SQL standard constraint capability defaults are defined in the core
ConstraintMixin (rhosocial.activerecord.backend.dialect.mixins).
"""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.statements import (
        AddTableConstraint,
        TableConstraint,
    )


class PostgresConstraintMixin:
    """PostgreSQL-proprietary constraint capability defaults.

    These features are PostgreSQL-specific and not part of the SQL standard.
    Both NOT VALID and EXCLUDE constraints are supported by all PostgreSQL
    versions, so they default to True.

    SQL standard constraint capability defaults are in ConstraintMixin.
    """

    def supports_constraint_novalidate(self) -> bool:
        """NOT VALID constraint option (PostgreSQL-proprietary).

        Allows adding a constraint without validating existing data.
        Supported by all PostgreSQL versions.
        """
        return True

    def supports_exclude_constraint(self) -> bool:
        """EXCLUDE constraints (PostgreSQL-proprietary).

        Exclusion constraints prevent overlapping rows on specified
        columns using specified operators.
        Supported by all PostgreSQL versions.
        """
        return True

    def supports_drop_constraint_if_exists(self) -> bool:
        """``DROP CONSTRAINT IF EXISTS`` (vendor extension).

        PostgreSQL has supported the ``IF EXISTS`` qualifier on
        ``DROP CONSTRAINT`` since 9.6.
        """
        return True

    def format_add_table_constraint_action(
        self, action: "AddTableConstraint",
    ) -> Tuple[str, tuple]:
        """Format ADD CONSTRAINT action with PostgreSQL-specific extensions.

        Extends the base class implementation with:
        - EXCLUDE constraint support (PG-specific constraint type)
        - NOT VALID suffix (PG-specific: skip validation of existing rows)
        """
        from rhosocial.activerecord.backend.expression.statements import (
            TableConstraintType, ConstraintValidation,
        )

        # Handle EXCLUDE constraint (PG-specific, not in base class)
        if action.constraint.constraint_type == TableConstraintType.EXCLUDE:
            parts = []
            exclude_sql, params = self._format_exclude_constraint(action.constraint)
            parts.append(exclude_sql)

            # NOT VALID suffix
            if getattr(action.constraint, 'validation', None) == ConstraintValidation.NOVALIDATE:
                parts.append("NOT VALID")

            return f"ADD {' '.join(parts)}", tuple(params)

        # Use base class for standard formatting (includes DEFERRABLE)
        sql, params = super().format_add_table_constraint_action(action)

        # PostgreSQL NOT VALID suffix
        if getattr(action.constraint, 'validation', None) == ConstraintValidation.NOVALIDATE:
            sql += " NOT VALID"

        return sql, params

    def _format_exclude_constraint(
        self, constraint: "TableConstraint",
    ) -> Tuple[str, tuple]:
        """Format EXCLUDE constraint (PostgreSQL-specific).

        EXCLUDE constraints are built as ``PostgresExcludeConstraint``:
        - ``elements``: List of (expression, operator) tuples
          e.g., [('range', '&&')] for EXCLUDE USING gist (range WITH &&)
        - ``using``: The index access method (default 'gist')
          e.g., 'gist', 'btree', 'spgist'
        - ``where``: Optional predicate for partial exclusion constraints

        Example:
            PostgresExcludeConstraint(
                dialect,
                name='exclude_range_overlap',
                elements=[('range', '&&')],
                using='gist',
            )
            # Generates: EXCLUDE USING gist (range WITH &&)

        """
        params: list = []

        if constraint.name:
            parts = ["CONSTRAINT", self.format_identifier(constraint.name)]
        else:
            parts = []

        # USING clause - validate index access method.
        valid_using = frozenset({"gist", "btree", "spgist", "hash", "gin", "brin"})
        using = getattr(constraint, "using", "gist") or "gist"
        if using not in valid_using:
            raise ValueError(
                f"Invalid index access method '{using}': must be one of {valid_using}"
            )
        parts.append(f"EXCLUDE USING {using}")

        # Elements: (expression, operator) pairs - validate operators.
        valid_ops = frozenset({
            "=", "<", "<=", ">", ">=", "<>",
            "&&", "@>", "<@", "<<", ">>", "&<", "&>",
            "~=", "@@", "?|", "?&", "is", "is not",
        })
        exclude_elements = []
        for expr, op in getattr(constraint, "elements", None) or []:
            if op not in valid_ops:
                raise ValueError(
                    f"Invalid exclude operator '{op}': must be one of {valid_ops}"
                )
            if isinstance(expr, str):
                exclude_elements.append(f"{self.format_identifier(expr)} WITH {op}")
            else:
                expr_sql, expr_params = expr.to_sql()
                params.extend(expr_params)
                exclude_elements.append(f"{expr_sql} WITH {op}")

        if exclude_elements:
            parts.append(f"({', '.join(exclude_elements)})")

        # WHERE clause for partial exclusion constraint
        where_expr = getattr(constraint, "where", None)
        if where_expr is not None:
            where_sql, where_params = where_expr.to_sql()
            params.extend(where_params)
            parts.append(f"WHERE ({where_sql})")

        # DEFERRABLE / NOT DEFERRABLE
        if constraint.deferrable is True:
            if constraint.initially_deferred is True:
                parts.append("DEFERRABLE INITIALLY DEFERRED")
            elif constraint.initially_deferred is False:
                parts.append("DEFERRABLE INITIALLY IMMEDIATE")
            else:
                parts.append("DEFERRABLE")
        elif constraint.deferrable is False:
            parts.append("NOT DEFERRABLE")

        return ' '.join(parts), tuple(params)
