# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_merge_suggestion.py
"""PostgreSQL's partition merge suggestion.

Community PostgreSQL has no ``MERGE PARTITIONS`` statement on any version: the
``ALTER TABLE`` reference lists only ATTACH PARTITION and DETACH PARTITION for
partition maintenance, and MERGE is spelled by detaching the sources and
attaching the replacement partition. ``MERGE PARTITIONS`` is Oracle Database
syntax; Oracle-compatible forks such as EDB Postgres Advanced Server carry it,
which is where the earlier claim that PostgreSQL gained it in 14 came from.
Do not restore that claim for this backend.

The engine-neutral answer is therefore the explicit "no such mechanism"
(``False``), not "no action needed" (``None``).
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import PartitionMergeSuggestion

from ._common import PostgresPartitionSuggestionBase

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.statements import PartitionByClause

__all__ = ["PostgresPartitionMergeSuggestionMixin"]


class PostgresPartitionMergeSuggestionMixin(PostgresPartitionSuggestionBase):
    """Report community PostgreSQL's lack of a partition merge statement."""

    def suggest_partition_merge(
        self, strategy: "PartitionByClause", plan: object
    ) -> PartitionMergeSuggestion:
        """Report that community PostgreSQL has no ``MERGE PARTITIONS`` DDL.

        The ``ALTER TABLE`` reference exposes only ATTACH PARTITION and DETACH
        PARTITION for partition maintenance, so a merge has to be performed as
        detached sources plus an attached replacement partition. The answer is
        the explicit "no such mechanism" on every server version.
        """
        return PartitionMergeSuggestion(False)
