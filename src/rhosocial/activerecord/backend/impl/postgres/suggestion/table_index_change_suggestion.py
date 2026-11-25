# src/rhosocial/activerecord/backend/impl/postgres/suggestion/table_index_change_suggestion.py
"""PostgreSQL's table-index change answer.

PostgreSQL spells index creation and removal as standalone ``CREATE INDEX`` and
``DROP INDEX`` statements (``DROP INDEX`` names the index alone; no ``ON``
clause). It has **no** ``ALTER TABLE ... ADD INDEX`` / ``DROP INDEX`` action --
those exist on MySQL/MariaDB, and are not PostgreSQL grammar. The answer here
is built only from the real ``CreateIndexStatement`` / ``DropIndexStatement``
nodes; the core ``AddIndexAction`` / ``DropIndexAction`` alter-table nodes are
never used, so a point that can only be stated as an ALTER TABLE index action
is explicitly unsupported.
"""
from typing import Any, Dict, Optional, Tuple

from rhosocial.activerecord.backend.suggestion import TableIndexChangeSuggestion

from ._change_points import declaration_params, projection, reusable_expression

__all__ = ["PostgresTableIndexChangeSuggestionMixin"]


def _named(values: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    """Index declared indexes by name, or None if a name is missing."""
    named: Dict[str, Any] = {}
    for value in values or ():
        params = declaration_params(value)
        name = params.get("name") if params else None
        if not isinstance(name, str) or not name:
            return None
        named[name] = value
    return named


def _live_by_name(values: Tuple[Any, ...], cls: Any) -> Dict[str, Any]:
    """Index live declaration nodes by name, skipping unusable entries."""
    named: Dict[str, Any] = {}
    for value in values or ():
        if not isinstance(value, cls):
            continue
        name = getattr(value, "name", None)
        if isinstance(name, str) and name:
            named[name] = value
    return named


class PostgresTableIndexChangeSuggestionMixin:
    """Answer an index change with standalone CREATE/DROP INDEX statements."""

    def suggest_table_index_change(
        self, point: TableIndexChangeSuggestion
    ) -> TableIndexChangeSuggestion:
        """Return real PostgreSQL index statements, never ALTER TABLE index actions.

        Drops come first, then creates, one statement per operation. A frozen
        declaration projection keeps plain column names, flags and the index
        method; it loses expression columns and a partial-index predicate. The
        enriched point also carries live ``IndexDefinition`` declarations, so a
        change that needs either of those reuses the live node directly instead
        of rebuilding it from a projection that cannot state it. Options this
        dialect cannot spell still make the answer ``False``.
        """
        from rhosocial.activerecord.backend.expression.objects import Index, Table
        from rhosocial.activerecord.backend.expression.statements import (
            CreateIndexStatement,
            DropIndexStatement,
            IndexDefinition,
        )

        if not (self.supports_create_index() and self.supports_drop_index()):
            return TableIndexChangeSuggestion(operations=False)

        old = _named(point.old_indexes)
        new = _named(point.new_indexes)
        if old is None or new is None:
            return TableIndexChangeSuggestion(operations=False)

        removed = [
            name for name, value in old.items()
            if name not in new or projection(value) != projection(new[name])
        ]
        added = [
            name for name, value in new.items()
            if name not in old or projection(value) != projection(old[name])
        ]
        if not removed and not added:
            return TableIndexChangeSuggestion(operations=None)

        old_live = _live_by_name(point.old_index_declarations, IndexDefinition)
        new_live = _live_by_name(point.new_index_declarations, IndexDefinition)
        table = Table(self, point.table_name)
        chain = []
        for name in removed:
            params = declaration_params(old_live.get(name, old[name])) or {}
            if_exists = bool(params.get("if_exists", False))
            concurrent = bool(params.get("concurrent", False))
            if if_exists and not self.supports_index_if_exists():
                return TableIndexChangeSuggestion(operations=False)
            if concurrent and not self.supports_streaming_btree_index_build():
                # DROP INDEX CONCURRENTLY is a PostgreSQL 18 spelling.
                return TableIndexChangeSuggestion(operations=False)
            chain.append(
                DropIndexStatement(
                    self, Index(self, name), if_exists=if_exists, concurrent=concurrent
                )
            )

        for name in added:
            params = declaration_params(new_live.get(name, new[name]))
            if params is None:
                return TableIndexChangeSuggestion(operations=False)
            columns = params.get("columns") or ()
            if not columns or not all(
                isinstance(column, str) or reusable_expression(column) for column in columns
            ):
                return TableIndexChangeSuggestion(operations=False)
            where = params.get("partial_condition")
            if where is not None and not reusable_expression(where):
                # A frozen predicate cannot be rebuilt as a real expression.
                return TableIndexChangeSuggestion(operations=False)
            include = params.get("include_columns") or None
            tablespace = params.get("tablespace")
            if_not_exists = bool(params.get("if_not_exists", False))
            concurrent = bool(params.get("concurrent", False))
            if include is not None:
                if not self.supports_index_include():
                    return TableIndexChangeSuggestion(operations=False)
                include = list(include)
            if tablespace and not self.supports_index_tablespace():
                return TableIndexChangeSuggestion(operations=False)
            if if_not_exists and not self.supports_index_if_not_exists():
                return TableIndexChangeSuggestion(operations=False)
            if concurrent and not self.supports_parallel_create_index():
                return TableIndexChangeSuggestion(operations=False)
            chain.append(
                CreateIndexStatement(
                    self,
                    Index(self, name),
                    table,
                    list(columns),
                    unique=bool(params.get("unique", False)),
                    if_not_exists=if_not_exists,
                    index_type=params.get("type"),
                    where=where,
                    include=include,
                    tablespace=tablespace,
                    concurrent=concurrent,
                )
            )
        return TableIndexChangeSuggestion(operations=tuple(chain))
