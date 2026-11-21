# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_split_suggestion.py
"""PostgreSQL's partition split suggestion.

PostgreSQL has no split operation (``supports_reorganize_partition`` is False),
so the answer is the explicit "unsupported" state for every strategy.
"""

from rhosocial.activerecord.backend.suggestion import PartitionSplitSuggestion

from ....expression.statements import PartitionStrategy
from ._common import PostgresPartitionSuggestionBase

__all__ = ["PostgresPartitionSplitSuggestionMixin"]


class PostgresPartitionSplitSuggestionMixin(PostgresPartitionSuggestionBase):
    """Answer PostgreSQL's lack of partition split explicitly."""

    def suggest_partition_split(
        self, strategy: PartitionStrategy, plan: object
    ) -> PartitionSplitSuggestion:
        """PostgreSQL has no split operation (supports_reorganize_partition is False)."""
        return PartitionSplitSuggestion(False)
