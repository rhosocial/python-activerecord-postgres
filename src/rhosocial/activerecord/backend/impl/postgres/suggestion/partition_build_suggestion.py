# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_build_suggestion.py
"""PostgreSQL's partition build suggestion.

The parent's contribution at CREATE TABLE time is the ``PARTITION BY <method>
(<keys>)`` clause. The clause is the real expression node a caller attaches to
its CREATE TABLE, so it is the single operation returned here. Strategies outside
RANGE/LIST/HASH and plans with no key columns have no PostgreSQL mechanism.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionBuildSuggestion

from ._common import PostgresPartitionSuggestionBase

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionByClause

__all__ = ["PostgresPartitionBuildSuggestionMixin"]


class PostgresPartitionBuildSuggestionMixin(PostgresPartitionSuggestionBase):
    """Declare the parent as ``PARTITION BY <method> (<keys>)``."""

    def suggest_partition_build(
        self, strategy: "PartitionByClause", plan: object
    ) -> PartitionBuildSuggestion:
        """Declare the parent as ``PARTITION BY <method> (<keys>)``.

        PostgreSQL has no inline named partitions: the parent's contribution at
        CREATE TABLE time is the PARTITION BY clause. The clause is the real
        expression node a caller attaches to its CREATE TABLE, so it is the
        single operation returned here. Strategies outside RANGE/LIST/HASH and
        plans with no key columns have no PostgreSQL mechanism.
        """
        from ....expression.core import Column
        from ....expression.statements import PartitionClause

        word = self._suggestion_strategy(strategy, plan)
        columns = self._suggestion_columns(plan)
        if word is None or not columns:
            return PartitionBuildSuggestion(False)
        clause = PartitionClause(self, word, [Column(self, name) for name in columns])
        return PartitionBuildSuggestion((clause,))
