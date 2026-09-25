# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/constraint.py
"""PostgreSQL typed ALTER TABLE constraint actions."""

from rhosocial.activerecord.backend.expression.statements import (
    AlterConstraint,
    ValidateConstraint,
)


class PostgresAlterConstraint(AlterConstraint):
    """PostgreSQL ``ALTER CONSTRAINT`` enforcement action."""


class PostgresValidateConstraint(ValidateConstraint):
    """PostgreSQL ``VALIDATE CONSTRAINT`` action."""


PostgresAlterConstraintAction = PostgresAlterConstraint
PostgresValidateConstraintAction = PostgresValidateConstraint
PostgresAlterConstraintExpression = PostgresAlterConstraint
PostgresValidateConstraintExpression = PostgresValidateConstraint
PostgresAlterTableConstraint = PostgresAlterConstraint
PostgresValidateTableConstraint = PostgresValidateConstraint


__all__ = [
    "PostgresAlterConstraint",
    "PostgresValidateConstraint",
    "PostgresAlterConstraintAction",
    "PostgresValidateConstraintAction",
    "PostgresAlterConstraintExpression",
    "PostgresValidateConstraintExpression",
    "PostgresAlterTableConstraint",
    "PostgresValidateTableConstraint",
]
