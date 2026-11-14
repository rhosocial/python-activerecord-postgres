# src/rhosocial/activerecord/backend/impl/postgres/adapters/base.py
"""
Base PostgreSQL type adapters.

This module contains basic type adapters for PostgreSQL.
"""

import json
from enum import Enum as PythonEnum
from typing import Any, Callable, Dict, List, Type, Union, Optional, Set, get_args, get_origin  # noqa: F401

from psycopg.types.json import Jsonb

from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter


class PostgresListAdapter(BaseSQLTypeAdapter):
    """
    Adapts Python list to PostgreSQL array types.

    This adapter passes Python lists through to psycopg for native array
    column support. On read, if the database returns a string (e.g. when
    a list was stored in a TEXT column via psycopg array serialization),
    it parses the value back using psycopg's own array loader — the exact
    mirror of the ``ListDumper`` used on write — and falls back to JSON
    for the portable ``["a","b"]`` representation.
    """

    def __init__(self):
        super().__init__()
        self.register_type(list, list)

    def _do_to_database(self, value: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        return value

    @staticmethod
    def _parse_array_literal(value: str) -> list:
        """Parse a PostgreSQL array literal using psycopg's array loader.

        ``psycopg.types.array._load_text`` is the exact reverse of the
        ``ListDumper`` that psycopg uses to serialize a Python list on
        write, so the round-trip is symmetric and handles quoting,
        backslash escaping, ``NULL`` elements and nested arrays correctly.
        """
        from psycopg.types.array import _load_text

        class _TextElementLoader:
            def load(self, data):
                return data.decode("utf-8")

        try:
            return _load_text(value.encode("utf-8"), _TextElementLoader())
        except Exception:
            raise ValueError(f"Not a PostgreSQL array literal: {value!r}") from None

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            stripped = value.strip()
            # Try JSON first (more portable format)
            if stripped.startswith("["):
                try:
                    return json.loads(stripped)
                except (json.JSONDecodeError, ValueError):
                    pass
            # Try PostgreSQL array literal
            if stripped.startswith("{"):
                try:
                    return self._parse_array_literal(stripped)
                except (ValueError, IndexError):
                    pass
            raise TypeError(f"Cannot convert str to list: {value!r}")
        raise TypeError(f"Cannot convert {type(value).__name__} to list")

    def to_database_batch(
        self, values: List[Any], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Optimized batch conversion - psycopg handles lists natively.

        No per-element processing needed, just pass through.
        This avoids the overhead of calling to_database() for each element.

        Args:
            values: List of values to convert
            target_type: Target type (not used for lists)
            options: Optional conversion options

        Returns:
            The same list, psycopg handles conversion internally
        """
        return values

    def from_database_batch(
        self, values: List[Any], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Optimized batch conversion from database.

        Args:
            values: List of values from database
            target_type: Target Python type
            options: Optional conversion options

        Returns:
            The same list, psycopg already returns Python lists
        """
        return values


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

    def to_database_batch(
        self, values: List[Any], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Optimized batch JSON conversion to database format."""
        result = []
        for value in values:
            if value is None:
                result.append(None)
            else:
                result.append(self._wrap(value))
        return result

    def from_database_batch(
        self, values: List[Any], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Optimized batch JSON deserialization from database."""
        result = []
        for value in values:
            if value is None:
                result.append(None)
            elif target_type is str:
                if isinstance(value, (dict, list)):
                    result.append(json.dumps(value, ensure_ascii=False, default=self.default))
                else:
                    result.append(value)
            else:
                result.append(self._apply_object_hook(value))
        return result


class PostgresNetworkAddressAdapter(BaseSQLTypeAdapter):
    """
    Adapts Python ipaddress objects to PostgreSQL network types.
    """

    def __init__(self):
        super().__init__()
        try:
            import ipaddress
            self.register_type(ipaddress.IPv4Address, str)
            self.register_type(ipaddress.IPv6Address, str)
            self.register_type(ipaddress.IPv4Network, str)
            self.register_type(ipaddress.IPv6Network, str)
        except ImportError:
            pass

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        return str(value)

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        try:
            import ipaddress
            return ipaddress.ip_address(value)
        except (ImportError, ValueError):
            try:
                import ipaddress
                return ipaddress.ip_network(value)
            except (ImportError, ValueError):
                return value

    def to_database_batch(
        self, values: List[Any], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Optimized batch network address conversion.

        Converts all values to strings in a single pass.

        Args:
            values: List of values to convert
            target_type: Target type
            options: Optional conversion options

        Returns:
            List of string representations
        """
        result = []
        for value in values:
            if value is None:
                result.append(None)
            else:
                result.append(str(value))
        return result

    def from_database_batch(
        self, values: List[Any], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Optimized batch network address parsing from database.

        Args:
            values: List of string values from database
            target_type: Target Python type
            options: Optional conversion options

        Returns:
            List of ipaddress objects or strings
        """
        import ipaddress

        result = []
        for value in values:
            if value is None:
                result.append(None)
            else:
                try:
                    result.append(ipaddress.ip_address(value))
                except (ImportError, ValueError):
                    try:
                        result.append(ipaddress.ip_network(value))
                    except (ImportError, ValueError):
                        result.append(value)
        return result


class PostgresEnumAdapter(BaseSQLTypeAdapter):
    """PostgreSQL ENUM type adapter.

    This adapter handles conversion between Python values and PostgreSQL enum values.

    The adapter can work with:
    - String values (validated against enum type)
    - Python Enum instances
    - None (NULL)
    """

    def __init__(self):
        super().__init__()
        self.register_type(str, str)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
        # Handle Python Enum
        if isinstance(value, PythonEnum):
            result = value.name
        elif isinstance(value, str):
            result = value
        else:
            raise TypeError(f"Cannot convert {type(value).__name__} to enum value")

        # Validate if enum_type provided
        if options and "enum_type" in options:
            from ..expression.enum_ import PostgresEnumType

            enum_type = options["enum_type"]
            if isinstance(enum_type, PostgresEnumType):
                if not enum_type.validate_value(result):
                    raise ValueError(f"Invalid enum value: '{result}'")

        return result

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:  # noqa: E501
        # If already a string, check if we should convert to Python Enum
        if isinstance(value, str):
            if options and "enum_class" in options:
                enum_class = options["enum_class"]
                if issubclass(enum_class, PythonEnum):
                    return enum_class[value]
            return value

        raise TypeError(f"Cannot convert {type(value).__name__} from enum")

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
