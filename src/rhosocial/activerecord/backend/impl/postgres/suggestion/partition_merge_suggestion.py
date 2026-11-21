# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_merge_suggestion.py
"""PostgreSQL's partition merge suggestion.

PostgreSQL has no merge-partition operation, so the answer is the explicit
"unsupported" state for every strategy.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionMergeSuggestion

from ._common import PostgresPartitionSuggestionBase

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionByClause

__all__ = ["PostgresPartitionMergeSuggestionMixin"]


class PostgresPartitionMergeSuggestionMixin(PostgresPartitionSuggestionBase):
    """Answer PostgreSQL's lack of partition merge explicitly."""

    def suggest_partition_merge(
        self, strategy: "PartitionByClause", plan: object
    ) -> PartitionMergeSuggestion:
        """PostgreSQL has no merge-partition operation."""
        return PartitionMergeSuggestion(False)
