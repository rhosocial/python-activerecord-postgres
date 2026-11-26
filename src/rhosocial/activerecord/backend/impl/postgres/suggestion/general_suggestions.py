# src/rhosocial/activerecord/backend/impl/postgres/suggestion/general_suggestions.py
"""PostgreSQL's non-type production suggestions."""
from typing import Optional

from rhosocial.activerecord.backend.suggestion.definitions import (
    ConcatenationFunctionSuggestion,
    CreateTableLayoutSuggestion,
)
class PostgresCreateTableLayoutSuggestionMixin:
    """PostgreSQL renders indexes and comments as standalone statements."""

    def suggest_create_table_layout(self) -> CreateTableLayoutSuggestion:
        inline_index = self.supports_inline_index()
        inline_comment = self.supports_table_comment()
        return CreateTableLayoutSuggestion(
            strategy="single" if inline_index and inline_comment else "split",
            inline_index=inline_index,
            inline_comment=inline_comment,
            state="actions",
        )


class PostgresStringConcatenationSuggestionMixin:
    """PostgreSQL spells string concatenation with the ``||`` operator."""

    def suggest_concatenation_function(self) -> Optional[str]:
        return ConcatenationFunctionSuggestion(function=None)


__all__ = [
    "PostgresCreateTableLayoutSuggestionMixin",
    "PostgresStringConcatenationSuggestionMixin",
]
