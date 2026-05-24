# src/rhosocial/activerecord/backend/impl/postgres/adapters/base.py
"""
Base PostgreSQL type adapters.

This module contains basic type adapters for PostgreSQL.
"""

import json
from enum import Enum as PythonEnum
from typing import Any, Dict, List, Type, Union, Optional, Set, get_args, get_origin

from psycopg.types.json import Jsonb

from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter


class PostgresListAdapter(BaseSQLTypeAdapter):
    """
    Adapts Python list to PostgreSQL array types.

    This adapter does not perform any conversion - psycopg handles
    Python lists natively for PostgreSQL array types.
    """

    def __init__(self):
        super().__init__()
        self._register_type(list, list)

    def _do_to_database(self, value: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        # psycopg handles list natively for PostgreSQL arrays
        return value

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        if isinstance(value, list):
            return value
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
    """

    def __init__(self):
        super().__init__()
        self._register_type(dict, Jsonb)

    def _do_to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        return Jsonb(value)

    def _do_from_database(
        self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> Union[dict, list, str]:
        # For string target type, serialize dict/list back to JSON string
        # This is needed because psycopg auto-deserializes JSON to dict,
        # but model fields may be defined as Optional[str] type
        if target_type is str:
            if isinstance(value, (dict, list)):
                return json.dumps(value, ensure_ascii=False)
            return value
        # For dict/list target types, return as-is
        if isinstance(value, dict):
            return value
        if isinstance(value, list):
            return value
        # In case it's a string representation
        return json.loads(value)

    def to_database_batch(
        self, values: List[Any], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Optimized batch JSON conversion to database format."""
        result = []
        for value in values:
            if value is None:
                result.append(None)
            else:
                result.append(Jsonb(value))
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
                    result.append(json.dumps(value, ensure_ascii=False))
                else:
                    result.append(value)
            else:
                result.append(value)
        return result


class PostgresNetworkAddressAdapter(BaseSQLTypeAdapter):
    """
    Adapts Python ipaddress objects to PostgreSQL network types.
    """

    def __init__(self):
        super().__init__()
        try:
            import ipaddress
            self._register_type(ipaddress.IPv4Address, str)
            self._register_type(ipaddress.IPv6Address, str)
            self._register_type(ipaddress.IPv4Network, str)
            self._register_type(ipaddress.IPv6Network, str)
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
        self._register_type(str, str)

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
            from ..types import PostgresEnumType

            enum_type = options["enum_type"]
            if isinstance(enum_type, PostgresEnumType):
                if not enum_type.validate_value(result):
                    raise ValueError(f"Invalid enum value: '{result}'")

        return result

    def _do_from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
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
