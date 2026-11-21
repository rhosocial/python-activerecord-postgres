# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_retention_suggestion.py
"""PostgreSQL's partition-retention suggestion.

Retire a dated RANGE partition by detaching then dropping it. Retention is a
date-bound notion, so only RANGE partitions with a concrete date/datetime bound
have a mechanism here; LIST and HASH partitions are explicitly unsupported.
"""

from datetime import date, datetime

from rhosocial.activerecord.backend.suggestion import PartitionRetentionSuggestion

from ....expression.statements import PartitionStrategy
from ._common import PostgresPartitionSuggestionBase

__all__ = ["PostgresPartitionRetentionSuggestionMixin"]


class PostgresPartitionRetentionSuggestionMixin(PostgresPartitionSuggestionBase):
    """Retire a dated RANGE partition by detaching then dropping it."""

    def suggest_partition_retention(
        self, strategy: PartitionStrategy, plan: object
    ) -> PartitionRetentionSuggestion:
        """Retire a dated RANGE partition by detaching then dropping it.

        Retention is a date-bound notion, so only RANGE partitions with a
        concrete date/datetime bound have a mechanism here; LIST and HASH
        partitions are explicitly unsupported.
        """
        word = self._suggestion_strategy(strategy, plan)
        if word != "RANGE":
            return PartitionRetentionSuggestion(False)
        spec = self._suggestion_spec(plan)
        if spec is None:
            return PartitionRetentionSuggestion(False)
        bound = getattr(spec, "range_end", None)
        if bound is None:
            bound = (getattr(spec, "bounds", None) or {}).get("to")
        if not isinstance(bound, (date, datetime)):
            return PartitionRetentionSuggestion(False)
        name = getattr(spec, "name", None)
        parent = getattr(spec, "parent", None) or self._suggestion_parent(plan)
        if not name or not parent:
            return PartitionRetentionSuggestion(False)
        return PartitionRetentionSuggestion(
            self._suggestion_drop_chain(name, parent, self._suggestion_schema(plan))
        )
