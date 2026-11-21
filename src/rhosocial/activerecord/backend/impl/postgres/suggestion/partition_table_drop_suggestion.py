# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_table_drop_suggestion.py
"""PostgreSQL's partition table drop suggestion.

The whole partitioned parent goes with a single ``DROP TABLE``. A plan with no
parent name has no PostgreSQL mechanism, so the answer is the explicit
"unsupported" state.
"""

from rhosocial.activerecord.backend.suggestion import PartitionTableDropSuggestion

from ....expression.statements import PartitionStrategy
from ._common import PostgresPartitionSuggestionBase

__all__ = ["PostgresPartitionTableDropSuggestionMixin"]


class PostgresPartitionTableDropSuggestionMixin(PostgresPartitionSuggestionBase):
    """Drop the whole partitioned parent with a single DROP TABLE."""

    def suggest_partition_table_drop(
        self, strategy: PartitionStrategy, plan: object
    ) -> PartitionTableDropSuggestion:
        """Drop the whole partitioned parent with a single DROP TABLE."""
        from ....expression.objects import Table
        from ....expression.statements import DropTableStatement

        parent = self._suggestion_parent(plan)
        if not parent:
            return PartitionTableDropSuggestion(False)
        statement = DropTableStatement(
            self,
            Table(self, parent, schema_name=self._suggestion_schema(plan)),
            if_exists=True,
        )
        return PartitionTableDropSuggestion((statement,))
