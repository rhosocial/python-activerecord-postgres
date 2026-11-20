# src/rhosocial/activerecord/backend/impl/postgres/mixins/partition_suggestion.py
"""PostgreSQL's partition lifecycle suggestions.

This mixin is the single "how" source for the eight partition backend-suggestion
protocols. Every answer is assembled from the backend's own DDL expression
classes, so the PostgreSQL spelling of each operation lives in one place.
No SQL text is composed here: the returned nodes' formatters own the spelling.

It is installed first in :class:`PostgresDialect` bases so no core mixin can
shadow an answer. The ``plan`` argument stays opaque to the protocol; this mixin
duck-types the backend's ``PostgresPartitionPlan`` / ``PartitionSpec`` to read
the parent, keys, schema and bounds it needs, and answers ``False`` when the
strategy or plan cannot yield a concrete mechanism.
"""

from datetime import date, datetime
from typing import Any, Optional, Tuple

from ....expression.statements import (
    PartitionAddSuggestion,
    PartitionBuildSuggestion,
    PartitionDropSuggestion,
    PartitionMergeSuggestion,
    PartitionRetentionSuggestion,
    PartitionSplitSuggestion,
    PartitionStrategy,
    PartitionTableAlterSuggestion,
    PartitionTableDropSuggestion,
)

__all__ = ["PostgresPartitionSuggestionMixin"]

_RANGE_LIST_HASH = ("RANGE", "LIST", "HASH")


class PostgresPartitionSuggestionMixin:
    """Answer PostgreSQL's partition DDL for all eight suggestion protocols."""

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _suggestion_strategy(strategy: Any, plan: Any) -> Optional[str]:
        """The normalized RANGE/LIST/HASH word, or None when unsupported."""
        for candidate in (strategy, getattr(plan, "method", None)):
            if candidate is None:
                continue
            try:
                word = PartitionStrategy.parse(candidate)
            except TypeError:
                # An explicitly supplied strategy that PostgreSQL cannot spell
                # (KEY, INTERVAL, ...) is a definite "no mechanism", not a cue
                # to fall back to the plan's own method.
                if candidate is strategy:
                    return None
                continue
            return word if word in _RANGE_LIST_HASH else None
        return None

    @staticmethod
    def _suggestion_parent(plan: Any) -> Optional[str]:
        return getattr(plan, "parent", None) or None

    @staticmethod
    def _suggestion_schema(plan: Any) -> Optional[str]:
        return getattr(plan, "schema", None) or None

    @staticmethod
    def _suggestion_columns(plan: Any) -> Tuple[str, ...]:
        columns = getattr(plan, "columns", ())
        if isinstance(columns, str):
            return (columns,)
        try:
            return tuple(columns)
        except TypeError:
            return ()

    @staticmethod
    def _suggestion_spec(plan: Any) -> Any:
        """Return a concrete partition spec from a spec or a plan, else None.

        A ``PartitionSpec`` already carries ``name`` / ``parent`` / ``bounds``.
        A ``PostgresPartitionPlan`` yields its first spec through ``iter_specs``;
        a generated RANGE policy with no horizon raises there, which is the
        honest "nothing concrete to suggest" answer.
        """
        if all(hasattr(plan, attr) for attr in ("name", "parent", "bounds")):
            return plan
        iterator = getattr(plan, "iter_specs", None)
        if iterator is None:
            return None
        try:
            for spec in iterator():
                return spec
        except (TypeError, ValueError):
            return None
        return None

    def _suggestion_child_clause(self, spec: Any) -> Any:
        """Build the child-level PARTITION BY clause of a branch, when present."""
        child_method = getattr(spec, "child_method", None)
        if not child_method:
            return None
        from ....expression.core import Column
        from ....expression.statements import PartitionClause

        columns = getattr(spec, "child_columns", ()) or ()
        return PartitionClause(self, child_method, [Column(self, name) for name in columns])

    def _suggestion_drop_chain(
        self, name: str, parent: str, schema: Optional[str]
    ) -> Tuple[Any, ...]:
        """The ordered detach-then-drop statements for one leaf partition.

        ``DETACH PARTITION CONCURRENTLY`` is only part of PostgreSQL's answer on
        a server that supports it (14+); below that the single transactional
        ``DROP TABLE`` is the whole mechanism.
        """
        from ....expression.objects import Table
        from ....expression.statements import DropTableStatement
        from ..expression.ddl.partition import PostgresDetachPartitionStatement

        table = Table(self, name, schema_name=schema)
        drop = DropTableStatement(self, table, if_exists=True)
        if not self.supports_concurrent_detach():
            return (drop,)
        detach = PostgresDetachPartitionStatement(
            self,
            partition_name=name,
            parent_table=parent,
            schema=schema,
            parent_schema=schema,
            concurrently=True,
        )
        return (detach, drop)

    # -------------------------------------------------------------- protocols
    def suggest_partition_build(
        self, strategy: PartitionStrategy, plan: object
    ) -> PartitionBuildSuggestion:
        """Declare the parent as ``PARTITION BY <method> (<keys>)``.

        PostgreSQL has no inline named partitions: the parent's contribution at
        CREATE TABLE time is the PARTITION BY clause. The clause is the real
        expression node a caller attaches to its CREATE TABLE, so it is the
        single operation returned here. Strategies outside RANGE/LIST/HASH and
        plans with no key columns have no PostgreSQL mechanism.
        """
        from ....expression.core import Column
        from ....expression.statements import PartitionClause

        word = self._suggestion_strategy(strategy, plan)
        columns = self._suggestion_columns(plan)
        if word is None or not columns:
            return PartitionBuildSuggestion(False)
        clause = PartitionClause(self, word, [Column(self, name) for name in columns])
        return PartitionBuildSuggestion((clause,))

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

    def suggest_partition_table_alter(
        self, strategy: PartitionStrategy, plan: object
    ) -> PartitionTableAlterSuggestion:
        """PostgreSQL cannot ALTER a table's partitioning scheme in place.

        Changing a parent's strategy is a rebuild (new table + copy + swap),
        not one of the modelled DDL expression statements, so the honest answer
        is the explicit "no such mechanism" state.
        """
        return PartitionTableAlterSuggestion(False)

    def suggest_partition_add(
        self, strategy: PartitionStrategy, plan: object
    ) -> PartitionAddSuggestion:
        """Add a partition with ``CREATE TABLE ... PARTITION OF ... FOR VALUES``."""
        from ..expression.ddl.partition import PostgresCreatePartitionStatement

        word = self._suggestion_strategy(strategy, plan)
        spec = self._suggestion_spec(plan)
        if spec is None or word is None:
            return PartitionAddSuggestion(False)
        name = getattr(spec, "name", None)
        parent = getattr(spec, "parent", None) or self._suggestion_parent(plan)
        bounds = dict(getattr(spec, "bounds", None) or {})
        if not name or not parent or not bounds:
            return PartitionAddSuggestion(False)
        statement = PostgresCreatePartitionStatement(
            self,
            partition_name=name,
            parent_table=parent,
            partition_type=word,
            partition_values=bounds,
            schema=self._suggestion_schema(plan),
            parent_schema=self._suggestion_schema(plan),
            partition_clause=self._suggestion_child_clause(spec),
        )
        return PartitionAddSuggestion((statement,))

    def suggest_partition_drop(
        self, strategy: PartitionStrategy, plan: object
    ) -> PartitionDropSuggestion:
        """Drop one partition; a whole branch goes through a cascading DROP."""
        from ....expression.objects import Table
        from ....expression.statements import DropTableStatement

        spec = self._suggestion_spec(plan)
        if spec is None:
            return PartitionDropSuggestion(False)
        name = getattr(spec, "name", None)
        parent = getattr(spec, "parent", None) or self._suggestion_parent(plan)
        if not name or not parent:
            return PartitionDropSuggestion(False)
        if not getattr(spec, "is_leaf", True):
            # A branch carries subpartitions: drop the subtree in one statement
            # (the maintenance executor's drop_branch), not a leaf detach.
            cascade = DropTableStatement(
                self,
                Table(self, name, schema_name=self._suggestion_schema(plan)),
                if_exists=True,
                cascade=True,
            )
            return PartitionDropSuggestion((cascade,))
        return PartitionDropSuggestion(
            self._suggestion_drop_chain(name, parent, self._suggestion_schema(plan))
        )

    def suggest_partition_split(
        self, strategy: PartitionStrategy, plan: object
    ) -> PartitionSplitSuggestion:
        """PostgreSQL has no split operation (supports_reorganize_partition is False)."""
        return PartitionSplitSuggestion(False)

    def suggest_partition_merge(
        self, strategy: PartitionStrategy, plan: object
    ) -> PartitionMergeSuggestion:
        """PostgreSQL has no merge-partition operation."""
        return PartitionMergeSuggestion(False)

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
