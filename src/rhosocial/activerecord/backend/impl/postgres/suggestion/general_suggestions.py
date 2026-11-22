# src/rhosocial/activerecord/backend/impl/postgres/suggestion/general_suggestions.py
"""PostgreSQL's non-type production suggestions."""
from typing import Optional

from rhosocial.activerecord.backend.suggestion.definitions import (
    ConcatenationFunctionSuggestion,
)


class PostgresStringConcatenationSuggestionMixin:
    """PostgreSQL spells string concatenation with the ``||`` operator."""

    def suggest_concatenation_function(self) -> Optional[str]:
        return ConcatenationFunctionSuggestion(function=None)


__all__ = ["PostgresStringConcatenationSuggestionMixin"]
