# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/constraint.py
"""PostgreSQL typed ALTER TABLE constraint actions."""

from rhosocial.activerecord.backend.expression.statements import AlterConstraintAction


class PostgresAlterConstraintAction(AlterConstraintAction):
    """PostgreSQL ``ALTER CONSTRAINT`` enforcement action.

    This is core's action under the backend's own name. It adds nothing: the
    ``constraint_kind`` view that used to live here existed only because core
    declared the parameter under one name and stored it under another, so
    ``get_params`` dropped it and no artifact could be rebuilt. Core now keeps
    both reachable -- ``constraint_branch`` for the formatter, ``constraint_kind``
    as the wire spelling -- so the view belongs to the class that declares the
    parameter, and nothing is left to override.

    ``VALIDATE CONSTRAINT`` needs no PostgreSQL node either: its grammar and
    parameters are core's, and the backend's differences are in the formatter
    (``format_validate_constraint_action``).
    """


__all__ = ["PostgresAlterConstraintAction"]
