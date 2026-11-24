# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_split_suggestion.py
"""PostgreSQL's partition split suggestion.

PostgreSQL gained ``ALTER TABLE ... SPLIT PARTITION`` in 17, so claiming the
engine has no split mechanism at all is wrong on modern servers. The
conservative ``unsupported`` answer is only correct on PostgreSQL 16 and older
(or when the server version is unknown). This is unrelated to the MySQL-style
``REORGANIZE PARTITION`` operation exposed by ``supports_reorganize_partition``.

This backend has no expression node that renders ``SPLIT PARTITION``, and the
expression and dialect layers forbid inventing one or embedding a raw SQL
string here. On servers that do support the statement the answer is therefore
``None`` (no operations to apply), not ``False`` (no mechanism): the true
capability is reported by ``supports_split_partition()``.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionSplitSuggestion

from ._common import PostgresPartitionSuggestionBase

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionByClause

__all__ = ["PostgresPartitionSplitSuggestionMixin"]


class PostgresPartitionSplitSuggestionMixin(PostgresPartitionSuggestionBase):
    """Report PostgreSQL's partition split capability honestly by version."""

    def suggest_partition_split(
        self, strategy: "PartitionByClause", plan: object
    ) -> PartitionSplitSuggestion:
        """Return ``False`` only where the server truly has no split statement.

        PostgreSQL 17+ supports ``SPLIT PARTITION`` but this backend has no
        expression node to render it, so those servers yield no operations
        rather than a false "no mechanism" answer.
        """
        if not self.supports_split_partition():
            return PartitionSplitSuggestion(False)
        return PartitionSplitSuggestion(None)
