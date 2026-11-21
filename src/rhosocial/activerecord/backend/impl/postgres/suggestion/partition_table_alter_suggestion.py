# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_table_alter_suggestion.py
"""PostgreSQL's partition table alter suggestion.

PostgreSQL cannot ALTER a table's partitioning scheme in place: changing a
parent's strategy is a rebuild (new table + copy + swap), not one of the modelled
DDL expression statements. The honest answer is the explicit "no such mechanism"
state.
"""

from rhosocial.activerecord.backend.suggestion import PartitionTableAlterSuggestion

from ....expression.statements import PartitionStrategy
from ._common import PostgresPartitionSuggestionBase

__all__ = ["PostgresPartitionTableAlterSuggestionMixin"]


class PostgresPartitionTableAlterSuggestionMixin(PostgresPartitionSuggestionBase):
    """Answer PostgreSQL's lack of in-place partition scheme alter explicitly."""

    def suggest_partition_table_alter(
        self, strategy: PartitionStrategy, plan: object
    ) -> PartitionTableAlterSuggestion:
        """PostgreSQL cannot ALTER a table's partitioning scheme in place.

        Changing a parent's strategy is a rebuild (new table + copy + swap),
        not one of the modelled DDL expression statements, so the honest answer
        is the explicit "no such mechanism" state.
        """
        return PartitionTableAlterSuggestion(False)
