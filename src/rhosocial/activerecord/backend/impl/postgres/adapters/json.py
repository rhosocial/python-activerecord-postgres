# src/rhosocial/activerecord/backend/impl/postgres/adapters/json.py
"""
PostgreSQL JSON Types Adapters.

This module provides type adapters for PostgreSQL JSON types.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-json.html

JSON Types:
- json: Binary JSON data, stored as text
- jsonb: Binary JSON data, stored in a decomposed binary format
- jsonpath: Path language for SQL/JSON (PostgreSQL 12+)

Both json and jsonb accept identical input values; the key difference is in
storage format. jsonb is generally preferred for query purposes.
"""

import json
from typing import Any, Dict, List, Optional, Set, Type, Union

from psycopg.types.json import Jsonb

from rhosocial.activerecord.backend.type_adapter import SQLTypeAdapter

from ..types.json import PostgresJsonPath


class PostgresJSONBAdapter(SQLTypeAdapter):
    """
    Adapts Python dict to PostgreSQL JSONB and vice-versa.

    Note: psycopg automatically serializes/deserializes JSON, so dict <-> JSON.
    When reading from DB, psycopg returns dict. When target type is str,
    we need to serialize back to JSON string.
    """

    @property
    def supported_types(self) -> Dict[Type, List[Any]]:
        return {dict: [Jsonb]}

    def to_database(self, value: Union[dict, list], target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        if value is None:
            return None
        return Jsonb(value)

    def from_database(
        self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> Union[dict, list]:
        if value is None:
            return None
        # For string target type, serialize dict/list back to JSON string
        # This is needed because psycopg auto-deserializes JSON to dict,
        # but model fields may be defined as str type
        if target_type is str:
            if isinstance(value, (dict, list)):
                return json.dumps(value)
            return value
        # For dict/list target types, return as-is
        if isinstance(value, dict):
            return value
        if isinstance(value, list):
            return value
        # In case it's a string representation
        return json.loads(value)


class PostgresJsonPathAdapter:
    """PostgreSQL jsonpath type adapter.

    This adapter converts between Python strings/PostgresJsonPath and
    PostgreSQL jsonpath values.

    Note: PostgreSQL validates jsonpath on input and will raise an error
    for invalid paths.

    **IMPORTANT: This adapter is NOT registered by default in the backend's
    type registry.**

    Reason for not registering by default:
    This adapter registers str -> str type mapping, which would conflict
    with the default string handling. Use this adapter explicitly when
    working with jsonpath values.

    To use this adapter:

    1. Register it explicitly on your backend instance:
    ```python
    from rhosocial.activerecord.backend.impl.postgres import PostgresJsonPathAdapter
    adapter = PostgresJsonPathAdapter()
    backend.adapter_registry.register(adapter, PostgresJsonPath, str)
    ```


    2. Specify it directly when executing queries:
    ```python
    from rhosocial.activerecord.backend.impl.postgres import PostgresJsonPath, PostgresJsonPathAdapter
    path_adapter = PostgresJsonPathAdapter()
    result = backend.execute(
        "SELECT * FROM data WHERE content @? %s",
        [PostgresJsonPath("$.items[*]")],
        type_adapters={PostgresJsonPath: path_adapter}
    )
    ```

    The adapter supports:
    - PostgresJsonPath -> str: Converting PostgresJsonPath objects to database format
    - str -> PostgresJsonPath: Converting database values to PostgresJsonPath objects
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            PostgresJsonPath: {str},
        }

    def to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Convert Python value to PostgreSQL jsonpath.

        Args:
            value: PostgresJsonPath, str, or None
            target_type: Target type
            options: Optional conversion options

        Returns:
            jsonpath string, or None

        Raises:
            TypeError: If value type is not supported
            ValueError: If string doesn't start with '$'
        """
        if value is None:
            return None

        if isinstance(value, PostgresJsonPath):
            return value.path

        if isinstance(value, str):
            if not value.startswith("$"):
                raise ValueError(f"jsonpath must start with '$': {value}")
            return value

        raise TypeError(f"Cannot convert {type(value).__name__} to jsonpath")

    def from_database(
        self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> Optional[PostgresJsonPath]:
        """Convert PostgreSQL jsonpath value to Python.

        Args:
            value: jsonpath value from database
            target_type: Target Python type
            options: Optional conversion options

        Returns:
            PostgresJsonPath object, or None

        Raises:
            TypeError: If value type is not supported
        """
        if value is None:
            return None

        if isinstance(value, PostgresJsonPath):
            return value

        if isinstance(value, str):
            return PostgresJsonPath(value)

        raise TypeError(f"Cannot convert {type(value).__name__} from jsonpath")

    def to_database_batch(
        self, values: List[Any], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert values to database format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(
        self, values: List[Any], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert values from database format."""
        return [self.from_database(v, target_type, options) for v in values]


__all__ = ["PostgresJSONBAdapter", "PostgresJsonPathAdapter"]
