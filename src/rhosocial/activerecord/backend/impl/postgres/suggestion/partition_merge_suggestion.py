# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_merge_suggestion.py
"""PostgreSQL's partition merge suggestion.

PostgreSQL gained ``ALTER TABLE ... MERGE PARTITIONS`` in 14, so claiming the
engine has no merge mechanism at all is wrong on every modern server. The
conservative ``unsupported`` answer is only correct on PostgreSQL 13 and older
(or when the server version is unknown).

This backend has no expression node that renders ``MERGE PARTITIONS``, and the
expression and dialect layers forbid inventing one or embedding a raw SQL
string here. On servers that do support the statement the answer is therefore
``None`` (no operations to apply), not ``False`` (no mechanism): the true
capability is reported by ``supports_merge_partition()``.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionMergeSuggestion

from ._common import PostgresPartitionSuggestionBase

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionByClause

__all__ = ["PostgresPartitionMergeSuggestionMixin"]


class PostgresPartitionMergeSuggestionMixin(PostgresPartitionSuggestionBase):
    """Report PostgreSQL's partition merge capability honestly by version."""

    def suggest_partition_merge(
        self, strategy: "PartitionByClause", plan: object
    ) -> PartitionMergeSuggestion:
        """Return ``False`` only where the server truly has no merge statement.

        PostgreSQL 14+ supports ``MERGE PARTITIONS`` but this backend has no
        expression node to render it, so those servers yield no operations
        rather than a false "no mechanism" answer.
        """
        if not self.supports_merge_partition():
            return PartitionMergeSuggestion(False)
        return PartitionMergeSuggestion(None)
