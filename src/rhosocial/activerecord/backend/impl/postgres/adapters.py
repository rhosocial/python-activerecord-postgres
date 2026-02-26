# src/rhosocial/activerecord/backend/impl/postgres/adapters.py
import datetime
import json
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Type, Union, Optional

from psycopg.types.json import Jsonb

from rhosocial.activerecord.backend.type_adapter import SQLTypeAdapter
from rhosocial.activerecord.backend.schema import DatabaseType


class PostgresListAdapter(SQLTypeAdapter):
    """
    Adapts Python list to PostgreSQL array types.
    
    This adapter does not perform any conversion - psycopg handles
    Python lists natively for PostgreSQL array types.
    """
    @property
    def supported_types(self) -> Dict[Type, List[Any]]:
        return {list: [list]}

    def to_database(self, value: list, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        if value is None:
            return None
        # psycopg handles list natively for PostgreSQL arrays
        return value

    def from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> list:
        if value is None:
            return None
        if isinstance(value, list):
            return value
        raise TypeError(f"Cannot convert {type(value).__name__} to list")


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

    def from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Union[dict, list]:
        if value is None:
            return None
        # For string target type, serialize dict/list back to JSON string
        # This is needed because psycopg auto-deserializes JSON to dict,
        # but model fields may be defined as str type
        if target_type == str:
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



class PostgresNetworkAddressAdapter(SQLTypeAdapter):
    """
    Adapts Python ipaddress objects to PostgreSQL network types.
    """
    @property
    def supported_types(self) -> Dict[Type, List[Any]]:
        try:
            import ipaddress
            return {
                ipaddress.IPv4Address: [str],
                ipaddress.IPv6Address: [str],
                ipaddress.IPv4Network: [str],
                ipaddress.IPv6Network: [str],
            }
        except ImportError:
            return {}

    def to_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        if value is None:
            return None
        return str(value)

    def from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        if value is None:
            return None
        try:
            import ipaddress
            return ipaddress.ip_address(value)
        except (ImportError, ValueError):
            try:
                # try network
                import ipaddress
                return ipaddress.ip_network(value)
            except (ImportError, ValueError):
                return value
