# src/rhosocial/activerecord/backend/impl/postgres/suggestion/__init__.py
"""PostgreSQL backend-suggestion implementations.

Suggestions are not expressions: the immutable suggestion values live in
``rhosocial.activerecord.backend.suggestion``. This package holds PostgreSQL's
concrete answer mixins -- which the dialect installs ahead of the core mixins
so no portable default can shadow them.
"""

from .data_types import (
    POSTGRES_DATA_TYPE_SUGGESTIONS,
    POSTGRES_EXTRA_DATA_TYPE_SUGGESTIONS,
    PostgresDataTypeSuggestionsMixin,
)
from .integer_auto_increment import PostgresIntegerAutoIncrementColumnSuggestionMixin
from .partition import PostgresPartitionSuggestionMixin

__all__ = [
    "POSTGRES_DATA_TYPE_SUGGESTIONS",
    "POSTGRES_EXTRA_DATA_TYPE_SUGGESTIONS",
    "PostgresDataTypeSuggestionsMixin",
    "PostgresIntegerAutoIncrementColumnSuggestionMixin",
    "PostgresPartitionSuggestionMixin",
]
