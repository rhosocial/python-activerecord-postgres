# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_add_suggestion.py
"""PostgreSQL's partition add suggestion.

A partition is added with ``CREATE TABLE ... PARTITION OF ... FOR VALUES``. A
concrete spec (name, parent, bounds) and a supported strategy are required;
without them the answer is the explicit "unsupported" state.
"""

from rhosocial.activerecord.backend.suggestion import PartitionAddSuggestion

from ....expression.statements import PartitionStrategy
from ._common import PostgresPartitionSuggestionBase

__all__ = ["PostgresPartitionAddSuggestionMixin"]


class PostgresPartitionAddSuggestionMixin(PostgresPartitionSuggestionBase):
    """Add a partition with ``CREATE TABLE ... PARTITION OF ... FOR VALUES``."""

    def suggest_partition_add(
        self, strategy: PartitionStrategy, plan: object
    ) -> PartitionAddSuggestion:
        """Add a partition with ``CREATE TABLE ... PARTITION OF ... FOR VALUES``."""
        from ..expression.ddl.partition import PostgresCreatePartitionStatement

        word = self._suggestion_strategy(strategy, plan)
        spec = self._suggestion_spec(plan)
        if spec is None or word is None:
            return PartitionAddSuggestion(False)
        name = getattr(spec, "name", None)
        parent = getattr(spec, "parent", None) or self._suggestion_parent(plan)
        bounds = dict(getattr(spec, "bounds", None) or {})
        if not name or not parent or not bounds:
            return PartitionAddSuggestion(False)
        statement = PostgresCreatePartitionStatement(
            self,
            partition_name=name,
            parent_table=parent,
            partition_type=word,
            partition_values=bounds,
            schema=self._suggestion_schema(plan),
            parent_schema=self._suggestion_schema(plan),
            partition_clause=self._suggestion_child_clause(spec),
        )
        return PartitionAddSuggestion((statement,))
