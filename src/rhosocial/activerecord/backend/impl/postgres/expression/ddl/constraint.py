# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/constraint.py
"""PostgreSQL typed ALTER TABLE constraint actions."""

from rhosocial.activerecord.backend.expression.statements import AlterConstraintAction


class PostgresAlterConstraintAction(AlterConstraintAction):
    """PostgreSQL ``ALTER CONSTRAINT`` enforcement action.

    The class adds one thing to core's action: ``constraint_kind`` as a
    **readable** property.

    ``AlterConstraintAction.__init__`` takes ``constraint_kind`` and stores it
    as ``constraint_branch`` -- a different name. ``BaseExpression.get_params()``
    resolves each declared parameter by name and is the single serialization
    path, so without this property the kind is dropped on the way out and every
    round-trip of this action comes back unable to rebuild itself.

    The value is the **spelling**, not the class: a class object is not
    something any of the three channels can encode, so it would come back as
    null. The spelling is derived by the same rule the constructor matches with
    -- the branch name minus its ``Table`` prefix -- so the two cannot drift
    apart silently. (Core's own ``AlterConstraintAction`` stores the parameter
    under a different name from the one it declares and has no such view, so it
    does not round-trip at all; this class is the backend's, and this property
    is what makes it work.)

    ``VALIDATE CONSTRAINT`` needs no PostgreSQL node: its grammar and its
    parameters are core's, and the backend's differences are in the formatter
    (``format_validate_constraint_action``), so core's
    :class:`~...statements.ddl_alter.ValidateConstraintAction` is what callers
    construct.
    """

    @property
    def constraint_kind(self):
        """The kind's spelling, which is what the constructor accepts back."""
        return self.constraint_branch.__name__.replace("Table", "")


__all__ = ["PostgresAlterConstraintAction"]
