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
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union, get_args, get_origin  # noqa: F401

from psycopg.types.json import Jsonb

from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter

from ..type_values.json import PostgresJsonPath


class PostgresJSONBAdapter(BaseSQLTypeAdapter):
    """
    Adapts Python dict to PostgreSQL JSONB and vice-versa.

    Note: psycopg automatically serializes/deserializes JSON, so dict <-> JSON.
    When reading from DB, psycopg returns dict. When target type is str,
    we need to serialize back to JSON string.

    The optional ``default`` and ``object_hook`` mirror their ``json.dumps`` /
    ``json.loads`` namesakes:

    - ``default`` is forwarded to psycopg's dumper through the per-value
      ``Jsonb(value, dumps)`` hook, so it is applied when psycopg serialises
      the value (psycopg's global ``dumps`` is replaced, not bypassed).
    - ``object_hook`` is applied with ``json.loads``. psycopg has already
      decoded a JSONB column to ``dict``/``list`` by the time the adapter sees
      it, so the already-decoded value is re-encoded and decoded again to run
      the hook over every object, exactly as ``json.loads`` would.
    """

    def __init__(
        self,
        default: Optional[Callable[[Any], Any]] = None,
        object_hook: Optional[Callable[[dict], Any]] = None,
    ):
        super().__init__()
        self.default = default
        self.object_hook = object_hook
        self.register_type(dict, Jsonb)

    def _dumps(self, value: Any) -> str:
        """Serialise for psycopg, applying the configured ``default``."""
        return json.dumps(value, default=self.default)

    def _wrap(self, value: Any) -> Jsonb:
        """Build the psycopg wrapper, wiring ``default`` into the dumper."""
        if self.default is None:
            # Keep psycopg's default dumps function untouched.
            return Jsonb(value)
        return Jsonb(value, self._dumps)

    def _apply_object_hook(self, value: Any) -> Any:
        """Run ``object_hook`` over an already-decoded JSON value."""
        if self.object_hook is None:
            return value
        return json.loads(json.dumps(value, default=self.default), object_hook=self.object_hook)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        return self._wrap(value)

    def _do_from_database(
        self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> Union[dict, list, str]:
        # For string target type, serialize dict/list back to JSON string
        # This is needed because psycopg auto-deserializes JSON to dict,
        # but model fields may be defined as Optional[str] type
        if target_type is str:
            if isinstance(value, (dict, list)):
                return json.dumps(value, ensure_ascii=False, default=self.default)
            return value
        # For dict/list target types, return the decoded value (running the
        # hook if one was configured against the already-decoded document)
        if isinstance(value, dict):
            return self._apply_object_hook(value)
        if isinstance(value, list):
            return self._apply_object_hook(value)
        # In case it's a string representation
        return json.loads(value, object_hook=self.object_hook)


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
    from rhosocial.activerecord.backend.impl.postgres.adapters import PostgresJsonPathAdapter
    adapter = PostgresJsonPathAdapter()
    backend.adapter_registry.register(adapter, PostgresJsonPath, str)
    ```


    2. Specify it directly when executing queries:
    ```python
    from rhosocial.activerecord.backend.impl.postgres.type_values import PostgresJsonPath
    from rhosocial.activerecord.backend.impl.postgres.adapters import PostgresJsonPathAdapter
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
