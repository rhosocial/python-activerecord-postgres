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
value. Guessing which one to re-add would invent a constraint, so an addition
whose projection lost the live node is explicitly unsupported. A drop only
needs the name and is always expressible.
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


class PostgresTableConstraintChangeSuggestionMixin:
    """Answer a constraint change with real ALTER TABLE constraint actions."""

    def suggest_table_constraint_change(
        self, point: TableConstraintChangeSuggestion
    ) -> TableConstraintChangeSuggestion:
        """Drop removed constraints, then add live replacement constraint nodes.

        Each action gets its own ``AlterTableStatement``: one action per
        statement, as the core planner does. Drops use ``IF EXISTS`` on every
        version this backend supports. Additions reuse the live
        ``TableConstraint`` node in the point when one is present; a frozen
        projection cannot say whether its branch was PRIMARY KEY or UNIQUE, so
        that case is ``False`` rather than a guessed constraint.
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
        if any(not isinstance(new[name], TableConstraint) for name in added):
            # Frozen projections do not carry the constraint branch class.
            return TableConstraintChangeSuggestion(operations=False)

        table = Table(self, point.table_name)
        chain = []
        if_exists = self.supports_drop_constraint_if_exists()
        for name in removed:
            action = DropTableConstraintAction(self, name, if_exists=if_exists)
            chain.append(AlterTableStatement(self, table, [action]))
        for name in added:
            action = AddTableConstraintAction(self, new[name])
            chain.append(AlterTableStatement(self, table, [action]))
        return TableConstraintChangeSuggestion(operations=tuple(chain))
