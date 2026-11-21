# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_drop_suggestion.py
"""PostgreSQL's partition drop suggestion.

A leaf goes through an ordered detach-then-drop chain; a whole branch goes
through a single cascading ``DROP TABLE``. A plan that yields no concrete spec
has no PostgreSQL mechanism, so the answer is the explicit "unsupported" state.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionDropSuggestion

from ._common import PostgresPartitionSuggestionBase

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionByClause

__all__ = ["PostgresPartitionDropSuggestionMixin"]


class PostgresPartitionDropSuggestionMixin(PostgresPartitionSuggestionBase):
    """Drop one partition; a whole branch goes through a cascading DROP."""

    def suggest_partition_drop(
        self, strategy: "PartitionByClause", plan: object
    ) -> PartitionDropSuggestion:
        """Drop one partition; a whole branch goes through a cascading DROP."""
        from ....expression.objects import Table
        from ....expression.statements import DropTableStatement

        spec = self._suggestion_spec(plan)
        if spec is None:
            return PartitionDropSuggestion(False)
        name = getattr(spec, "name", None)
        parent = getattr(spec, "parent", None) or self._suggestion_parent(plan)
        if not name or not parent:
            return PartitionDropSuggestion(False)
        if not getattr(spec, "is_leaf", True):
            # A branch carries subpartitions: drop the subtree in one statement
            # (the maintenance executor's drop_branch), not a leaf detach.
            cascade = DropTableStatement(
                self,
                Table(self, name, schema_name=self._suggestion_schema(plan)),
                if_exists=True,
                cascade=True,
            )
            return PartitionDropSuggestion((cascade,))
        return PartitionDropSuggestion(
            self._suggestion_drop_chain(name, parent, self._suggestion_schema(plan))
        )
