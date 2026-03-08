# src/rhosocial/activerecord/backend/impl/postgres/adapters/text_search.py
"""
PostgreSQL Text Search Types Adapters.

This module provides type adapters for PostgreSQL full-text search types.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-textsearch.html

Text Search Types:
- tsvector: Text search vector (sorted list of distinct lexemes)
- tsquery: Text search query (query for text search)

PostgreSQL full-text search allows searching through documents using
lexemes (normalized words) with optional weights and positions.
"""
from typing import Any, Dict, List, Optional, Set, Type, Union

from ..types.text_search import PostgresTsVector, PostgresTsQuery


class PostgresTsVectorAdapter:
    """PostgreSQL tsvector type adapter.

    This adapter converts between Python PostgresTsVector objects and
    PostgreSQL tsvector type values.

    The tsvector type represents a document in a form optimized for
    full-text search. It contains a sorted list of distinct lexemes
    (normalized words) with their positions and optional weights.
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            PostgresTsVector: {str},
        }

    def to_database(
        self,
        value: Union[PostgresTsVector, str, Dict[str, Any]],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Convert Python value to PostgreSQL tsvector string.

        Args:
            value: PostgresTsVector object, string, or dictionary
            target_type: Target type (not used, kept for interface)
            options: Optional conversion options

        Returns:
            PostgreSQL tsvector literal string, or None if value is None

        Raises:
            TypeError: If value type is not supported
        """
        if value is None:
            return None

        if isinstance(value, PostgresTsVector):
            return value.to_postgres_string()

        if isinstance(value, str):
            # Validate and return as-is or parse and re-format
            return value

        if isinstance(value, dict):
            # Convert dictionary to PostgresTsVector
            tsvector = PostgresTsVector()
            for lexeme, positions in value.items():
                if isinstance(positions, list):
                    tsvector.add_lexeme(lexeme, positions)
                else:
                    tsvector.add_lexeme(lexeme)
            return tsvector.to_postgres_string()

        if isinstance(value, list):
            # Convert list of lexemes to PostgresTsVector
            tsvector = PostgresTsVector()
            for item in value:
                if isinstance(item, str):
                    tsvector.add_lexeme(item)
                elif isinstance(item, tuple) and len(item) == 2:
                    tsvector.add_lexeme(item[0], item[1])
            return tsvector.to_postgres_string()

        raise TypeError(f"Cannot convert {type(value).__name__} to tsvector")

    def from_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[PostgresTsVector]:
        """Convert PostgreSQL tsvector value to PostgresTsVector object.

        Args:
            value: Tsvector value from database
            target_type: Target Python type (PostgresTsVector)
            options: Optional conversion options

        Returns:
            PostgresTsVector object, or None if value is None
        """
        if value is None:
            return None

        if isinstance(value, PostgresTsVector):
            return value

        if isinstance(value, str):
            return PostgresTsVector.from_postgres_string(value)

        raise TypeError(f"Cannot convert {type(value).__name__} to PostgresTsVector")

    def to_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert values to database format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Optional[PostgresTsVector]]:
        """Batch convert values from database format."""
        return [self.from_database(v, target_type, options) for v in values]


class PostgresTsQueryAdapter:
    """PostgreSQL tsquery type adapter.

    This adapter converts between Python PostgresTsQuery objects and
    PostgreSQL tsquery type values.

    The tsquery type represents a text search query. It contains
    lexemes and operators that can be used to search tsvector values.
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            PostgresTsQuery: {str},
        }

    def to_database(
        self,
        value: Union[PostgresTsQuery, str],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Convert Python value to PostgreSQL tsquery string.

        Args:
            value: PostgresTsQuery object or string
            target_type: Target type (not used, kept for interface)
            options: Optional conversion options

        Returns:
            PostgreSQL tsquery literal string, or None if value is None

        Raises:
            TypeError: If value type is not supported
        """
        if value is None:
            return None

        if isinstance(value, PostgresTsQuery):
            return value.to_postgres_string()

        if isinstance(value, str):
            return value

        raise TypeError(f"Cannot convert {type(value).__name__} to tsquery")

    def from_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[PostgresTsQuery]:
        """Convert PostgreSQL tsquery value to PostgresTsQuery object.

        Args:
            value: Tsquery value from database
            target_type: Target Python type (PostgresTsQuery)
            options: Optional conversion options

        Returns:
            PostgresTsQuery object, or None if value is None
        """
        if value is None:
            return None

        if isinstance(value, PostgresTsQuery):
            return value

        if isinstance(value, str):
            return PostgresTsQuery.parse(value)

        raise TypeError(f"Cannot convert {type(value).__name__} to PostgresTsQuery")

    def to_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert values to database format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Optional[PostgresTsQuery]]:
        """Batch convert values from database format."""
        return [self.from_database(v, target_type, options) for v in values]


__all__ = ['PostgresTsVectorAdapter', 'PostgresTsQueryAdapter']
