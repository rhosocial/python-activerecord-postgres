# src/rhosocial/activerecord/backend/impl/postgres/suggestion/table_constraint_change_suggestion.py
"""PostgreSQL's table-constraint change answer.

``ALTER TABLE ... ADD CONSTRAINT`` and ``DROP CONSTRAINT [IF EXISTS]`` are real
PostgreSQL statements, and the core expression system already has the matching
``AddTableConstraintAction`` / ``DropTableConstraintAction`` nodes plus a
PostgreSQL formatter for the ``IF EXISTS`` qualifier (measured on PG 19beta4;
``IF EXISTS`` since 9.6). Both verbs are therefore answerable when the point
states enough to build those nodes.

A frozen projection states a constraint's *name* but not its branch class:
``PRIMARY KEY (a)`` and ``UNIQUE (a)`` freeze to the same ``("params", ...)``
value. The enriched change point also carries the live ``TableConstraint``
declarations, whose ``branch`` names the kind; an addition reuses that node
instead of guessing. Kinds this dialect cannot spell stay unsupported. A drop
only needs the name and is always expressible.
"""
from typing import Any, Dict, Optional, Tuple

from rhosocial.activerecord.backend.suggestion import TableConstraintChangeSuggestion

from ._change_points import declaration_params, projection

__all__ = ["PostgresTableConstraintChangeSuggestionMixin"]


def _named(values: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    """Index declared constraints by name, or None if a name is missing.

    PostgreSQL's ``DROP CONSTRAINT`` names a constraint; it has no "drop the
    constraint on these columns" form, so an unnamed declaration cannot be
    expressed without inventing a server-generated name.
    """
    named: Dict[str, Any] = {}
    for value in values or ():
        params = declaration_params(value)
        name = params.get("name") if params else None
        if not isinstance(name, str) or not name:
            return None
        named[name] = value
    return named


def _live_by_name(values: Tuple[Any, ...], cls: Any) -> Dict[str, Any]:
    """Index live declaration nodes by name, skipping unusable entries."""
    named: Dict[str, Any] = {}
    for value in values or ():
        if not isinstance(value, cls):
            continue
        name = getattr(value, "name", None)
        if isinstance(name, str) and name:
            named[name] = value
    return named


def _spellable_constraint(dialect: Any, constraint: Any) -> bool:
    """Whether ``ADD CONSTRAINT`` can actually spell this live node.

    The accept surface is measured on PostgreSQL 19beta4: PRIMARY KEY, UNIQUE,
    CHECK, FOREIGN KEY and EXCLUDE can all be added. EXCLUDE is gated by its own
    probe; an unknown branch, or a trait the grammar puts on a narrower set of
    kinds, is not spelled by inventing a constraint the declaration did not
    state.
    """
    from rhosocial.activerecord.backend.expression.statements import (
        ConstraintValidation,
        TableCheckConstraint,
        TableExcludeConstraint,
        TableForeignKeyConstraint,
        TablePrimaryKeyConstraint,
        TableUniqueConstraint,
    )

    branch = getattr(constraint, "branch", None)
    if isinstance(branch, TableExcludeConstraint):
        return bool(dialect.supports_exclude_constraint()) and hasattr(
            dialect, branch.format_method
        )
    if not isinstance(
        branch,
        (
            TablePrimaryKeyConstraint,
            TableUniqueConstraint,
            TableCheckConstraint,
            TableForeignKeyConstraint,
        ),
    ):
        return False
    if not hasattr(dialect, branch.format_method):
        return False
    if getattr(constraint, "validation", None) is ConstraintValidation.NOVALIDATE:
        if not isinstance(branch, (TableCheckConstraint, TableForeignKeyConstraint)):
            return False
        if not dialect.supports_constraint_novalidate():
            return False
    return True


class PostgresTableConstraintChangeSuggestionMixin:
    """Answer a constraint change with real ALTER TABLE constraint actions."""

    def suggest_table_constraint_change(
        self, point: TableConstraintChangeSuggestion
    ) -> TableConstraintChangeSuggestion:
        """Drop removed constraints, then add live replacement constraint nodes.

        Each action gets its own ``AlterTableStatement``: one action per
        statement, as the core planner does. Drops use ``IF EXISTS`` on every
        version this backend supports. Additions take the live ``TableConstraint``
        from ``new_constraint_declarations`` (keyed by name), falling back to a
        live node passed directly in ``new_constraints``. A frozen projection
        cannot say whether its branch was PRIMARY KEY or UNIQUE, and a branch
        this dialect cannot spell is not invented: both are ``False``.
        """
        from rhosocial.activerecord.backend.expression.objects import Table
        from rhosocial.activerecord.backend.expression.statements import (
            AddTableConstraintAction,
            AlterTableStatement,
            DropTableConstraintAction,
            TableConstraint,
        )

        if not (self.supports_add_constraint() and self.supports_drop_constraint()):
            return TableConstraintChangeSuggestion(operations=False)

        old = _named(point.old_constraints)
        new = _named(point.new_constraints)
        if old is None or new is None:
            return TableConstraintChangeSuggestion(operations=False)

        removed = [
            name for name, value in old.items()
            if name not in new or projection(value) != projection(new[name])
        ]
        added = [
            name for name, value in new.items()
            if name not in old or projection(value) != projection(old[name])
        ]
        if not removed and not added:
            return TableConstraintChangeSuggestion(operations=None)

        new_live = _live_by_name(point.new_constraint_declarations, TableConstraint)
        replacements = {}
        for name in added:
            constraint = new_live.get(name)
            if constraint is None and isinstance(new[name], TableConstraint):
                constraint = new[name]
            if constraint is None or not _spellable_constraint(self, constraint):
                return TableConstraintChangeSuggestion(operations=False)
            replacements[name] = constraint

        table = Table(self, point.table_name)
        chain = []
        if_exists = self.supports_drop_constraint_if_exists()
        for name in removed:
            action = DropTableConstraintAction(self, name, if_exists=if_exists)
            chain.append(AlterTableStatement(self, table, action))
        for name in added:
            action = AddTableConstraintAction(self, replacements[name])
            chain.append(AlterTableStatement(self, table, action))
        return TableConstraintChangeSuggestion(operations=tuple(chain))
