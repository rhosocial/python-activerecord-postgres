# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_split_suggestion.py
"""PostgreSQL's partition split suggestion.

PostgreSQL has no split operation (``supports_reorganize_partition`` is False),
so the answer is the explicit "unsupported" state for every strategy.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionSplitSuggestion

from ._common import PostgresPartitionSuggestionBase

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionByClause

__all__ = ["PostgresPartitionSplitSuggestionMixin"]


class PostgresPartitionSplitSuggestionMixin(PostgresPartitionSuggestionBase):
    """Answer PostgreSQL's lack of partition split explicitly."""

    def suggest_partition_split(
        self, strategy: "PartitionByClause", plan: object
    ) -> PartitionSplitSuggestion:
        """PostgreSQL has no split operation (supports_reorganize_partition is False)."""
        return PartitionSplitSuggestion(False)
