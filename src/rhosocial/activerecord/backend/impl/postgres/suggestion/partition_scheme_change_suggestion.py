# src/rhosocial/activerecord/backend/impl/postgres/suggestion/partition_scheme_change_suggestion.py
"""PostgreSQL's partition-scheme change answer.

This protocol answers *scheme* changes from the old/new declarations in the
point. A frozen ``PartitionClause`` projection carries the method and the key
columns, nothing about a concrete partition: so ``add`` / ``drop`` /
``retention`` cannot name a partition and ``split`` / ``merge`` have no community
PostgreSQL statement at all (those two are always ``False``; see their own
modules for the measured evidence).

None of the three scheme transitions yields a safe, standalone executable
statement here. Adopting a scheme would only produce the ``PARTITION BY`` clause
from ``build`` -- a CREATE TABLE fragment, not a statement that
``resolve_suggestions`` could append to the executable ``plan.statements`` list.
Abandoning a scheme would produce the ``DROP TABLE`` from ``table_drop``, which
destroys the table's data. Switching between two schemes is a rebuild and
PostgreSQL cannot ALTER a parent's scheme in place (the ``table_alter`` answer
is ``False``). All three therefore answer ``False`` (explicitly unsupported);
``None`` is reserved for no change.
"""
from types import SimpleNamespace
from typing import Any, Dict, Optional, Tuple

from rhosocial.activerecord.backend.suggestion import PartitionSchemeChangeSuggestion

from ._change_points import declaration_params, frozen_params, projection

__all__ = ["PostgresPartitionSchemeChangeSuggestionMixin"]

# A partition was declared but its projection lost a fact a real clause needs.
# That is an explicit "unsupported", not the "no partitioning declared" None.
_UNUSABLE = object()


def _partition_projection(value: Any) -> Optional[Tuple[Any, ...]]:
    """The single frozen partition clause in a point field, if one is present."""
    if value is None:
        return None
    if isinstance(value, tuple):
        if value and value[0] in ("params", "repr"):
            return value
        for item in value:
            if isinstance(item, tuple) and item and item[0] == "params":
                return item
        return None
    params = declaration_params(value)
    if params is None:
        return None
    return (
        "params",
        tuple(sorted((str(key), projection(item)) for key, item in params.items())),
    )


class PostgresPartitionSchemeChangeSuggestionMixin:
    """Answer a partition-scheme change without offering destructive or fragmentary DDL."""

    def _scheme_context(self, table_name: str, value: Any) -> Any:
        """Build ``(strategy, plan)`` for the existing partition suggest methods.

        The plan is deliberately minimal: the point states the parent table and
        the partition keys, but no partition name, bounds or schema. Its empty
        ``iter_specs`` is what makes the spec-based answers honestly decline.
        ``None`` means no partition was declared; ``_UNUSABLE`` means one was
        declared but its projection lost a fact the clause would need.
        """
        if value is None:
            return None
        frozen = _partition_projection(value)
        params = frozen_params(frozen) if frozen is not None else None
        if params is None:
            return _UNUSABLE
        # Keep the declared word, even when it is outside RANGE/LIST/HASH: the
        # partition answers own that decision. Only a live strategy clause needs
        # its word unwrapped first.
        word = params.get("method")
        if not isinstance(word, str):
            word = self._suggestion_strategy(word, None)
        if not isinstance(word, str) or not word.strip():
            return _UNUSABLE
        keys = params.get("keys") or ()
        columns = []
        for key in keys:
            key_params = declaration_params(key)
            name = key_params.get("name") if key_params else None
            if not isinstance(name, str) or not name:
                return _UNUSABLE
            columns.append(name)
        if not columns:
            return _UNUSABLE

        from ....expression.core import Column
        from ....expression.statements import PartitionClause

        strategy = PartitionClause(self, word, [Column(self, name) for name in columns])
        plan = SimpleNamespace(
            parent=table_name,
            schema=None,
            columns=tuple(columns),
            method=word,
            iter_specs=lambda: iter(()),
        )
        return strategy, plan

    def _scheme_answers(self, context: Any) -> Dict[str, Any]:
        """Ask the maintenance and table-alter protocols; never restate their DDL.

        ``build`` and ``table_drop`` are deliberately absent: the former returns
        a CREATE TABLE clause fragment, the latter a destructive DROP TABLE.
        Neither belongs in an executable suggestion chain.
        """
        strategy, plan = context
        return {
            "table_alter": self.suggest_partition_table_alter(strategy, plan),
            "add": self.suggest_partition_add(strategy, plan),
            "drop": self.suggest_partition_drop(strategy, plan),
            "split": self.suggest_partition_split(strategy, plan),
            "merge": self.suggest_partition_merge(strategy, plan),
            "retention": self.suggest_partition_retention(strategy, plan),
        }

    def suggest_partition_scheme_change(
        self, point: PartitionSchemeChangeSuggestion
    ) -> PartitionSchemeChangeSuggestion:
        """Answer only with safe, standalone executable statements.

        Adopting a partition scheme would yield the parent's ``PARTITION BY``
        clause from the build answer -- a fragment that belongs in ``CREATE
        TABLE``, not a standalone statement ``MigrationContext.apply`` could
        execute -- so the answer is ``False`` here.

        Removing partitioning requires a data-preserving rebuild
        (``CREATE TABLE new ...`` -> copy the rows -> swap the table -> drop the
        old table). That rebuild is outside a suggestion chain and must be an
        explicitly authorized migration plan, so the answer is ``False`` rather
        than a destructive ``DROP TABLE``. Switching between two schemes is a
        rebuild too: PostgreSQL cannot ALTER a parent's scheme in place.
        """
        old = self._scheme_context(point.table_name, point.old_partition)
        new = self._scheme_context(point.table_name, point.new_partition)
        if old is _UNUSABLE or new is _UNUSABLE:
            return PartitionSchemeChangeSuggestion(operations=False)
        if old is None and new is None:
            return PartitionSchemeChangeSuggestion(operations=None)

        answers = self._scheme_answers(new or old)
        maintenance = (
            answers["add"],
            answers["drop"],
            answers["split"],
            answers["merge"],
            answers["retention"],
        )
        if any(answer.operations for answer in maintenance):
            # A scheme point carries no partition spec; if a maintenance answer
            # ever found one, that is a different decision and must not be
            # reported as the scheme change's action chain.
            return PartitionSchemeChangeSuggestion(operations=False)

        if old is None or new is None:
            # Adopt: only a CREATE TABLE clause fragment. Abandon: only a data
            # destroying DROP TABLE. Neither is a safe executable statement.
            return PartitionSchemeChangeSuggestion(operations=False)
        if projection(point.old_partition) == projection(point.new_partition):
            return PartitionSchemeChangeSuggestion(operations=None)
        # Both sides declare partitioning but differ: PostgreSQL cannot ALTER a
        # parent's scheme in place. The table-alter answer is the honest False.
        return PartitionSchemeChangeSuggestion(operations=answers["table_alter"].operations)
