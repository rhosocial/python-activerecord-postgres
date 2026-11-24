# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_split_suggestion.py
"""PostgreSQL's partition split suggestion.

Community PostgreSQL has no ``SPLIT PARTITION`` statement on any version: the
``ALTER TABLE`` reference lists only ATTACH PARTITION and DETACH PARTITION for
partition maintenance, and a split is spelled by detaching the source and
attaching the replacement partitions. ``SPLIT PARTITION`` is Oracle Database
syntax; Oracle-compatible forks such as EDB Postgres Advanced Server carry it,
which is where the earlier claim that PostgreSQL gained it in 17 came from.
Do not restore that claim for this backend. This is also unrelated to the
MySQL-style ``REORGANIZE PARTITION`` operation reported by
``supports_reorganize_partition``.

The engine-neutral answer is therefore the explicit "no such mechanism"
(``False``), not "no action needed" (``None``).
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionSplitSuggestion

from ._common import PostgresPartitionSuggestionBase

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionByClause

__all__ = ["PostgresPartitionSplitSuggestionMixin"]


class PostgresPartitionSplitSuggestionMixin(PostgresPartitionSuggestionBase):
    """Report community PostgreSQL's lack of a partition split statement."""

    def suggest_partition_split(
        self, strategy: "PartitionByClause", plan: object
    ) -> PartitionSplitSuggestion:
        """Report that community PostgreSQL has no ``SPLIT PARTITION`` DDL.

        The ``ALTER TABLE`` reference exposes only ATTACH PARTITION and DETACH
        PARTITION for partition maintenance, so a split has to be performed as
        a detached source plus attached replacement partitions. The answer is
        the explicit "no such mechanism" on every server version.
        """
        return PartitionSplitSuggestion(False)
