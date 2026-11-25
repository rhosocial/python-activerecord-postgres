# src/rhosocial/activerecord/backend/impl/postgres/suggestion/column_reorder_suggestion.py
"""PostgreSQL's column-reorder answer.

The ``ALTER TABLE`` reference has no column-position clause: there is no
``AFTER`` / ``BEFORE`` / ``FIRST`` form to move an existing column in place. The
in-place primitives are ``RENAME COLUMN`` and ``ALTER COLUMN ... TYPE``; changing
a column's *position* is a table rebuild (new table, copy, swap). The honest
answer is therefore the explicit "unsupported" state for every point.
"""
from rhosocial.activerecord.backend.suggestion import ColumnReorderSuggestion

__all__ = ["PostgresColumnReorderSuggestionMixin"]


class PostgresColumnReorderSuggestionMixin:
    """Report PostgreSQL's lack of an in-place column-reorder statement."""

    def suggest_column_reorder(
        self, point: ColumnReorderSuggestion
    ) -> ColumnReorderSuggestion:
        """There is no column-position clause; reordering needs a table rebuild.

        ``ALTER TABLE ... RENAME COLUMN`` and ``ALTER COLUMN ... SET DATA TYPE``
        are the only in-place column primitives PostgreSQL offers; neither
        changes a column's ordinal position. Moving a column requires rebuilding
        the table, which is not one of the modelled single-action statements, so
        the answer is ``False`` (explicitly unsupported) rather than ``None``.
        """
        return ColumnReorderSuggestion(operations=False)
