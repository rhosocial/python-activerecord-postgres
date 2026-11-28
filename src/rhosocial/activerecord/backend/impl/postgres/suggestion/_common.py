# src/rhosocial/activerecord/backend/impl/postgres/suggestion/_common.py
"""Shared helpers for PostgreSQL's eight partition suggestion mixins.

The eight answer mixins in this package each implement one suggestion protocol.
They all read the same opaque ``plan`` through the same duck-typed accessors and
assemble their answers from the same DDL expression nodes. Those helpers live
here, on :class:`PostgresPartitionSuggestionBase`, so each per-protocol module
holds only its own ``suggest_partition_<x>()`` method and the imports that method
needs. Behaviour is unchanged from the former single ``partition.py``: the
helpers still read the same strategy word, now carried by a strategy clause
expression rather than a strategy enum.
"""

from typing import Any, Optional, Tuple

__all__ = ["PostgresPartitionSuggestionBase"]

_RANGE_LIST_HASH = ("RANGE", "LIST", "HASH")


class PostgresPartitionSuggestionBase:
    """Shared plan/strategy readers and statement builders for partition answers."""

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _suggestion_strategy(strategy: Any, plan: Any) -> Optional[str]:
        """The normalized RANGE/LIST/HASH word, or None when unsupported."""
        for candidate in (strategy, getattr(plan, "method", None)):
            if candidate is None:
                continue
            # A strategy clause carries the word in ``word``/``method``. A
            # legacy str Enum carries it in ``value``; a plain string is itself.
            word = candidate
            for attr in ("word", "method", "value"):
                candidate_word = getattr(candidate, attr, None)
                if isinstance(candidate_word, str):
                    word = candidate_word
                    break
            # An unsupported word (KEY, INTERVAL, ...) is a definite "no
            # mechanism", not a cue to fall back to the plan's own method.
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
        from rhosocial.activerecord.backend.expression.column_types import Column
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
        from ..expression.statements.ddl.partition import PostgresDetachPartitionStatement

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
