# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/domain.py
"""PostgreSQL DOMAIN DDL capabilities and formatting."""

from __future__ import annotations

from typing import Any, List, Optional, Tuple, Type, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.mixins.ddl_domain import DomainMixin
from rhosocial.activerecord.backend.expression.statements.ddl_domain import (
    AddDomainCheckAction,
    AlterDomainExpression,
    CreateDomainExpression,
    DomainAlterAction,
    DomainCheckConstraint,
    DomainNullability,
    DomainValueExpression,
    DropDomainCheckAction,
    DropDomainDefaultAction,
    DropDomainExpression,
    DropDomainNotNullAction,
    RenameDomainAction,
    SetDomainDefaultAction,
    SetDomainNotNullAction,
)
from ...expression.ddl.domain import (
    _UnsupportedDomainAction,
    PostgresAddDomainCheckAction,
    PostgresChangeDomainOwnerAction,
    PostgresCreateDomainExpression,
    PostgresDropDomainCheckAction,
    PostgresRenameDomainConstraintAction,
    PostgresSetDomainSchemaAction,
    PostgresValidateDomainConstraintAction,
)

if TYPE_CHECKING:
    from ...expression.ddl.domain import _LegacySqlExpression


__all__ = ["PostgresDomainMixin"]


class PostgresDomainMixin(DomainMixin):
    """PostgreSQL DOMAIN DDL support."""

    if TYPE_CHECKING:
        name: str
        version: Tuple[int, int, int]

        def format_identifier(self, identifier: str, need_quote: bool = True) -> str: ...

    _POSTGRES_DOMAIN_ACTIONS = (
        SetDomainDefaultAction,
        DropDomainDefaultAction,
        SetDomainNotNullAction,
        DropDomainNotNullAction,
        AddDomainCheckAction,
        DropDomainCheckAction,
        RenameDomainAction,
        PostgresAddDomainCheckAction,
        PostgresDropDomainCheckAction,
        PostgresRenameDomainConstraintAction,
        PostgresValidateDomainConstraintAction,
        PostgresChangeDomainOwnerAction,
        PostgresSetDomainSchemaAction,
    )

    def _format_domain_identifier(self, name: str, schema: Optional[str] = None) -> str:
        value = f"{schema}.{name}" if schema is not None else name
        parts = value.split(".")
        if not parts or any(not part.strip() for part in parts):
            raise ValueError("PostgreSQL identifiers must contain non-empty segments")
        return ".".join(self.format_identifier(part) for part in parts)

    def _format_domain_behavior(self, cascade: bool, restrict: bool) -> str:
        if cascade and restrict:
            raise ValueError("CASCADE and RESTRICT are mutually exclusive")
        if cascade:
            return "CASCADE"
        if restrict:
            return "RESTRICT"
        return ""

    def supports_domains(self) -> bool:
        return self.version >= (9, 6, 0)

    def supports_create_domain(self) -> bool:
        return self.supports_domains()

    def supports_alter_domain(self) -> bool:
        return self.supports_domains()

    def supports_drop_domain(self) -> bool:
        return self.supports_domains()

    def supports_domain_default(self) -> bool:
        return self.supports_domains()

    def supports_domain_nullability(self, nullability: DomainNullability) -> bool:
        return self.supports_domains() and nullability in set(DomainNullability)

    def supports_domain_checks(self) -> bool:
        return self.supports_domains()

    def supports_named_domain_checks(self) -> bool:
        return self.supports_domains()

    def supports_multiple_domain_checks(self) -> bool:
        return self.supports_domains()

    def supports_domain_collation(self) -> bool:
        return self.supports_domains()

    def supports_alter_domain_action(
        self,
        action_type: Type[DomainAlterAction],
    ) -> bool:
        if not self.supports_domains():
            return False
        try:
            return issubclass(action_type, self._POSTGRES_DOMAIN_ACTIONS)
        except TypeError:
            return False

    def supports_multiple_domain_alter_actions(self) -> bool:
        return False

    def supports_drop_domain_if_exists(self) -> bool:
        return self.supports_domains()

    def supports_drop_domain_cascade(self) -> bool:
        return self.supports_domains()

    def supports_drop_domain_restrict(self) -> bool:
        return self.supports_domains()

    def supports_unnamed_domain_check_drop(self) -> bool:
        return False

    def format_create_domain_statement(
        self,
        expr: CreateDomainExpression,
    ) -> Tuple[str, tuple]:
        if not self.supports_domains() or not self.supports_create_domain():
            raise UnsupportedFeatureError(
                self.name,
                "CREATE DOMAIN",
                suggestion="requires PostgreSQL 9.6+",
            )
        type_sql, type_params = expr.data_type.to_sql()
        if type_params:
            raise ValueError("DOMAIN data types must render without bind parameters")
        parts = [
            "CREATE DOMAIN",
            self._format_domain_identifier(
                expr.domain_name,
                getattr(expr, "schema_name", None),
            ),
            "AS",
            type_sql,
        ]
        params = list(type_params)
        if expr.collation is not None:
            if not self.supports_domain_collation():
                raise UnsupportedFeatureError(self.name, "DOMAIN COLLATE")
            parts.append(f"COLLATE {self._format_domain_identifier(expr.collation)}")
        if expr.default is not None:
            if not self.supports_domain_default():
                raise UnsupportedFeatureError(self.name, "DOMAIN DEFAULT")
            default_sql, default_params = expr.default.to_sql()
            if default_params:
                raise ValueError("DOMAIN DEFAULT must render without bind parameters")
            parts.append(f"DEFAULT {default_sql}")
        if isinstance(expr, PostgresCreateDomainExpression):
            clauses: List[Any] = list(expr._constraint_clauses)
        else:
            clauses = list(expr.checks)
        if clauses and not self.supports_domain_checks():
            raise UnsupportedFeatureError(self.name, "DOMAIN CHECK")
        if any(
            isinstance(clause, DomainCheckConstraint) and clause.name is not None
            for clause in clauses
        ) and not self.supports_named_domain_checks():
            raise UnsupportedFeatureError(self.name, "named DOMAIN CHECK")
        if len(clauses) > 1 and not self.supports_multiple_domain_checks():
            raise UnsupportedFeatureError(self.name, "multiple domain CHECK constraints")
        for clause in clauses:
            clause_sql, clause_params = clause.to_sql()
            parts.append(clause_sql)
            params.extend(clause_params)
        if expr.nullability is not DomainNullability.UNSPECIFIED:
            if not self.supports_domain_nullability(expr.nullability):
                raise UnsupportedFeatureError(
                    self.name,
                    f"DOMAIN {expr.nullability.value}",
                )
            parts.append(expr.nullability.value)
        return " ".join(parts), tuple(params)

    def format_alter_domain_statement(
        self,
        expr: AlterDomainExpression,
    ) -> Tuple[str, tuple]:
        if not self.supports_domains() or not self.supports_alter_domain():
            raise UnsupportedFeatureError(
                self.name,
                "ALTER DOMAIN",
                suggestion="requires PostgreSQL 9.6+",
            )
        if len(expr.actions) > 1 and not self.supports_multiple_domain_alter_actions():
            raise UnsupportedFeatureError(self.name, "multiple ALTER DOMAIN actions")
        action_parts: List[str] = []
        action_params: List[Any] = []
        for action in expr.actions:
            if not isinstance(action, _UnsupportedDomainAction) and not self.supports_alter_domain_action(
                type(action)
            ):
                raise UnsupportedFeatureError(
                    self.name,
                    f"ALTER DOMAIN action {action.action_kind}",
                )
            action_sql, params = action.to_sql()
            action_parts.append(action_sql)
            action_params.extend(params)
        domain_ref = self._format_domain_identifier(
            expr.domain_name,
            getattr(expr, "schema_name", None),
        )
        return (
            f"ALTER DOMAIN {domain_ref} {', '.join(action_parts)}",
            tuple(action_params),
        )

    def format_postgres_alter_domain_statement(
        self,
        expr: AlterDomainExpression,
    ) -> Tuple[str, tuple]:
        return self.format_alter_domain_statement(expr)

    def format_drop_domain_statement(
        self,
        expr: DropDomainExpression,
    ) -> Tuple[str, tuple]:
        if not self.supports_domains() or not self.supports_drop_domain():
            raise UnsupportedFeatureError(
                self.name,
                "DROP DOMAIN",
                suggestion="requires PostgreSQL 9.6+",
            )
        if_exists = getattr(expr, "if_exists", False)
        cascade = getattr(expr, "cascade", False)
        restrict = getattr(expr, "restrict", False)
        if if_exists and not self.supports_drop_domain_if_exists():
            raise UnsupportedFeatureError(self.name, "DROP DOMAIN IF EXISTS")
        if cascade and not self.supports_drop_domain_cascade():
            raise UnsupportedFeatureError(self.name, "DROP DOMAIN CASCADE")
        if restrict and not self.supports_drop_domain_restrict():
            raise UnsupportedFeatureError(self.name, "DROP DOMAIN RESTRICT")
        behavior = self._format_domain_behavior(cascade, restrict)
        parts = ["DROP DOMAIN"]
        if if_exists:
            parts.append("IF EXISTS")
        parts.append(
            self._format_domain_identifier(
                expr.domain_name,
                getattr(expr, "schema_name", None),
            )
        )
        if behavior:
            parts.append(behavior)
        return " ".join(parts), ()

    def format_domain_value_expression(
        self,
        expr: DomainValueExpression,
    ) -> Tuple[str, tuple]:
        if not self.supports_domains():
            raise UnsupportedFeatureError(self.name, "DOMAIN VALUE")
        return "VALUE", ()

    def format_domain_check_constraint(
        self,
        expr: DomainCheckConstraint,
    ) -> Tuple[str, tuple]:
        return DomainMixin.format_domain_check_constraint(self, expr)

    def format_domain_alter_action(
        self,
        expr: DomainAlterAction,
    ) -> Tuple[str, tuple]:
        if not self.supports_domains() or not self.supports_alter_domain():
            raise UnsupportedFeatureError(self.name, "ALTER DOMAIN action")
        if not self.supports_alter_domain_action(type(expr)):
            raise UnsupportedFeatureError(
                self.name,
                f"ALTER DOMAIN action {expr.action_kind}",
            )
        if isinstance(expr, SetDomainDefaultAction):
            if not self.supports_domain_default():
                raise UnsupportedFeatureError(self.name, "ALTER DOMAIN SET DEFAULT")
            value_sql, params = expr.default.to_sql()
            if params:
                raise ValueError("ALTER DOMAIN DEFAULT must render without bind parameters")
            return f"SET DEFAULT {value_sql}", ()
        if isinstance(expr, DropDomainDefaultAction):
            if not self.supports_domain_default():
                raise UnsupportedFeatureError(self.name, "ALTER DOMAIN DROP DEFAULT")
            return "DROP DEFAULT", ()
        if isinstance(expr, SetDomainNotNullAction):
            if not self.supports_domain_nullability(expr.nullability):
                raise UnsupportedFeatureError(self.name, "ALTER DOMAIN SET NOT NULL")
            return "SET NOT NULL", ()
        if isinstance(expr, DropDomainNotNullAction):
            if not self.supports_domain_nullability(expr.nullability):
                raise UnsupportedFeatureError(self.name, "ALTER DOMAIN DROP NOT NULL")
            return "DROP NOT NULL", ()
        if isinstance(expr, PostgresAddDomainCheckAction):
            check_sql, params = expr.check.to_sql()
            suffix = " NOT VALID" if expr.not_valid else ""
            return f"ADD {check_sql}{suffix}", tuple(params)
        if isinstance(expr, AddDomainCheckAction):
            check_sql, params = expr.check.to_sql()
            return f"ADD {check_sql}", tuple(params)
        if isinstance(expr, PostgresDropDomainCheckAction):
            if expr.if_exists and not self.supports_drop_domain_if_exists():
                raise UnsupportedFeatureError(self.name, "ALTER DOMAIN DROP CONSTRAINT IF EXISTS")
            if expr.cascade and not self.supports_drop_domain_cascade():
                raise UnsupportedFeatureError(self.name, "ALTER DOMAIN DROP CONSTRAINT CASCADE")
            if expr.restrict and not self.supports_drop_domain_restrict():
                raise UnsupportedFeatureError(self.name, "ALTER DOMAIN DROP CONSTRAINT RESTRICT")
            parts = ["DROP CONSTRAINT"]
            if expr.if_exists:
                parts.append("IF EXISTS")
            parts.append(self.format_identifier(expr.name or ""))
            behavior = self._format_domain_behavior(expr.cascade, expr.restrict)
            if behavior:
                parts.append(behavior)
            return " ".join(parts), ()
        if isinstance(expr, DropDomainCheckAction):
            if expr.name is None:
                if not self.supports_unnamed_domain_check_drop():
                    raise UnsupportedFeatureError(self.name, "unnamed DOMAIN CHECK drop")
                return "DROP CONSTRAINT", ()
            if not self.supports_named_domain_checks():
                raise UnsupportedFeatureError(self.name, "named DOMAIN CHECK drop")
            return f"DROP CONSTRAINT {self.format_identifier(expr.name)}", ()
        if isinstance(expr, PostgresRenameDomainConstraintAction):
            return (
                f"RENAME CONSTRAINT {self.format_identifier(expr.name)} "
                f"TO {self.format_identifier(expr.new_name)}"
            ), ()
        if isinstance(expr, PostgresValidateDomainConstraintAction):
            return f"VALIDATE CONSTRAINT {self.format_identifier(expr.name)}", ()
        if isinstance(expr, PostgresChangeDomainOwnerAction):
            return f"OWNER TO {self.format_identifier(expr.new_owner)}", ()
        if isinstance(expr, PostgresSetDomainSchemaAction):
            return f"SET SCHEMA {self.format_identifier(expr.new_schema)}", ()
        if isinstance(expr, RenameDomainAction):
            return f"RENAME TO {self.format_identifier(expr.new_name)}", ()
        raise UnsupportedFeatureError(
            self.name,
            f"ALTER DOMAIN action {getattr(expr, 'action_kind', type(expr).__name__)}",
        )

    def _format_postgres_legacy_sql_expression(
        self,
        expr: "_LegacySqlExpression",
    ) -> Tuple[str, tuple]:
        return expr.sql, ()

    def _format_postgres_unsupported_domain_action(
        self,
        expr: "_UnsupportedDomainAction",
    ) -> Tuple[str, tuple]:
        raise ValueError(f"Unsupported ALTER DOMAIN action: {expr.action}")
