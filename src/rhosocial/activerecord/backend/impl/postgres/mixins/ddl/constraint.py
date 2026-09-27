# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/constraint.py
"""PostgreSQL-specific constraint capability defaults.

Provides default implementations for PostgreSQL-proprietary constraint
features that are not part of the SQL standard.

SQL standard constraint capability defaults are defined in the core
ConstraintMixin (rhosocial.activerecord.backend.dialect.mixins).
"""

import re
from typing import Any, List, Tuple, TYPE_CHECKING, cast

from rhosocial.activerecord.backend.dialect.mixins.ddl_table import (
    normalize_table_constraint_type,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect.mixins.ddl_table import ConstraintMixin
    from rhosocial.activerecord.backend.expression.statements import (
        AddTableConstraint,
        AlterConstraint,
        TableConstraint,
        ValidateConstraint,
    )

    class _ConstraintMixinBase(ConstraintMixin):
        version: Tuple[int, int, int]
        name: str

        def format_identifier(self, identifier: str, need_quote: bool = True) -> str:
            return identifier

else:

    class _ConstraintMixinBase:
        pass


class PostgresConstraintMixin(_ConstraintMixinBase):
    """PostgreSQL-proprietary constraint capability defaults.

    These features are PostgreSQL-specific and not part of the SQL standard.
    Both NOT VALID and EXCLUDE constraints are supported by all PostgreSQL
    versions, so they default to True.

    SQL standard constraint capability defaults are in ConstraintMixin.
    """

    def supports_constraint_enforced(self, constraint_type: Any = None) -> bool:
        if constraint_type is None:
            return self.version >= (18, 0, 0)
        from rhosocial.activerecord.backend.expression.statements import TableConstraintType

        normalized = normalize_table_constraint_type(constraint_type)
        if normalized not in {TableConstraintType.CHECK, TableConstraintType.FOREIGN_KEY}:
            return False
        return self.version >= (18, 0, 0)

    def supports_alter_constraint_enforced(self, constraint_type: Any = None) -> bool:
        if constraint_type is None:
            return False
        from rhosocial.activerecord.backend.expression.statements import TableConstraintType

        normalized = normalize_table_constraint_type(constraint_type)
        if normalized == TableConstraintType.FOREIGN_KEY:
            return self.version >= (18, 0, 0)
        if normalized == TableConstraintType.CHECK:
            return self.version >= (19, 0, 0)
        return False

    def supports_validate_constraint(self) -> bool:
        return True

    def _require_constraint_enforced(self, constraint_type: Any) -> None:
        if self.supports_constraint_enforced(constraint_type):
            return
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

        raise UnsupportedFeatureError(
            self.name,
            "ENFORCED/NOT ENFORCED constraint",
        )

    def _require_enforceable_constraint(self, constraint_type: Any) -> None:
        if self.supports_alter_constraint_enforced(constraint_type):
            return
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

        raise UnsupportedFeatureError(
            self.name,
            "ALTER CONSTRAINT ENFORCED/NOT ENFORCED",
        )

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

    def format_table_constraint(self, expr: "TableConstraint") -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.expression.statements import TableConstraintType

        constraint_type = normalize_table_constraint_type(expr.constraint_type)
        if getattr(expr, "enforced", None) is not None:
            self._require_constraint_enforced(constraint_type)
        if constraint_type == TableConstraintType.EXCLUDE:
            return self._format_exclude_constraint(expr)
        return cast(Tuple[str, tuple], super().format_table_constraint(expr))

    def format_add_table_constraint_action(
        self, action: "AddTableConstraint",
    ) -> Tuple[str, tuple]:
        from rhosocial.activerecord.backend.expression.statements import TableConstraintType

        constraint_type = normalize_table_constraint_type(action.constraint.constraint_type)
        if constraint_type == TableConstraintType.EXCLUDE:
            if getattr(action.constraint, "enforced", None) is not None:
                self._require_constraint_enforced(constraint_type)
            validation_sql = self._format_constraint_validation(action.constraint)
            exclude_sql, params = self._format_exclude_constraint(action.constraint)
            return f"ADD {exclude_sql}{validation_sql}", tuple(params)
        return cast(Tuple[str, tuple], super().format_add_table_constraint_action(action))

    def format_alter_constraint_action(self, action: "AlterConstraint") -> Tuple[str, tuple]:
        constraint_type = normalize_table_constraint_type(action.constraint_type)
        self._require_enforceable_constraint(constraint_type)
        return cast(Tuple[str, tuple], super().format_alter_constraint_action(action))

    def format_validate_constraint_action(self, action: "ValidateConstraint") -> Tuple[str, tuple]:
        return cast(Tuple[str, tuple], super().format_validate_constraint_action(action))

    @staticmethod
    def _validate_exclude_expression_sql(expression_sql: str, context: str) -> None:
        if not isinstance(expression_sql, str) or not expression_sql.strip():
            raise ValueError(f"{context} must render a non-empty SQL expression")
        depth = 0
        quote: Any = None
        index = 0
        while index < len(expression_sql):
            character = expression_sql[index]
            if quote is not None:
                if character == quote:
                    if index + 1 < len(expression_sql) and expression_sql[index + 1] == quote:
                        index += 2
                        continue
                    quote = None
                elif character == "\\" and quote == "'":
                    raise ValueError(f"{context} contains an unsupported string escape")
                index += 1
                continue
            if character in {"'", '"'}:
                quote = character
            elif character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if depth < 0:
                    raise ValueError(f"{context} has unbalanced parentheses")
            elif character == ";":
                raise ValueError(f"{context} must not contain statement separators")
            elif character == "-" and expression_sql[index : index + 2] == "--":
                raise ValueError(f"{context} must not contain SQL comments")
            elif character == "/" and expression_sql[index : index + 2] == "/*":
                raise ValueError(f"{context} must not contain SQL comments")
            index += 1
        if quote is not None or depth != 0:
            raise ValueError(f"{context} has unbalanced quotes or parentheses")

    def _format_exclude_constraint(
        self, constraint: "TableConstraint",
    ) -> Tuple[str, tuple]:
        valid_using = frozenset({"gist", "btree", "spgist", "hash", "brin"})
        using = getattr(constraint, "using", "gist")
        if using is None:
            using = "gist"
        if not isinstance(using, str):
            raise ValueError("EXCLUDE index access method must be a string")
        using = using.strip().lower()
        if using not in valid_using:
            raise ValueError(
                f"Invalid index access method '{using}': must be one of {sorted(valid_using)}"
            )

        elements = list(getattr(constraint, "elements", None) or [])
        if not elements:
            raise ValueError("EXCLUDE constraint requires at least one element")

        params: List[Any] = []
        rendered_elements: List[str] = []
        for element in elements:
            if not isinstance(element, (tuple, list)) or len(element) != 2:
                raise ValueError("each EXCLUDE element must be an expression/operator pair")
            expression, operator = element
            if not isinstance(operator, str):
                raise ValueError("EXCLUDE operator must be a string")
            operator = "".join(operator.strip().lower().split())
            if operator in {"is", "isnot"} or re.fullmatch(
                r"[a-z0-9_+\-*/<>=~!@#%^&|?$]+", operator
            ) is None:
                raise ValueError(f"Invalid exclude operator '{operator}'")
            if isinstance(expression, str):
                if not expression.strip():
                    raise ValueError("EXCLUDE element expression must not be empty")
                if "(" in expression or ")" in expression:
                    raise ValueError(
                        "parenthesized EXCLUDE elements must be supplied as SQL expressions"
                    )
                expression_sql = self.format_identifier(expression)
            else:
                to_sql = getattr(expression, "to_sql", None)
                if not callable(to_sql):
                    raise TypeError("EXCLUDE element expression must be an identifier or SQL expression")
                if getattr(expression, "dialect", None) is not self:
                    raise ValueError("EXCLUDE element expression must use the PostgreSQL dialect")
                expression_sql, expression_params = to_sql()
                if expression_params:
                    raise ValueError("EXCLUDE expression must not contain bind parameters")
                self._validate_exclude_expression_sql(expression_sql, "EXCLUDE element expression")
                expression_sql = f"({expression_sql})"
            rendered_elements.append(f"{expression_sql} WITH {operator}")

        parts: List[str] = []
        if constraint.name:
            parts.extend(("CONSTRAINT", self.format_identifier(constraint.name)))
        parts.extend(("EXCLUDE", "USING", using, f"({', '.join(rendered_elements)})"))

        where_expr = getattr(constraint, "where", None)
        if where_expr is not None:
            where_to_sql = getattr(where_expr, "to_sql", None)
            if not callable(where_to_sql):
                raise TypeError("EXCLUDE where must be a SQL expression")
            if getattr(where_expr, "dialect", None) is not self:
                raise ValueError("EXCLUDE where expression must use the PostgreSQL dialect")
            where_sql, where_params = where_to_sql()
            if where_params:
                raise ValueError("EXCLUDE where expression must not contain bind parameters")
            self._validate_exclude_expression_sql(where_sql, "EXCLUDE where expression")
            params.extend(where_params)
            parts.append(f"WHERE ({where_sql})")

        if getattr(constraint, "deferrable", None) is True:
            if getattr(constraint, "initially_deferred", None) is True:
                parts.append("DEFERRABLE INITIALLY DEFERRED")
            elif getattr(constraint, "initially_deferred", None) is False:
                parts.append("DEFERRABLE INITIALLY IMMEDIATE")
            else:
                parts.append("DEFERRABLE")
        elif getattr(constraint, "deferrable", None) is False:
            parts.append("NOT DEFERRABLE")

        return " ".join(parts), tuple(params)
