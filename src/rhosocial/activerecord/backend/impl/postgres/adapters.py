# src/rhosocial/activerecord/backend/impl/postgres/adapters.py
import datetime
import json
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Type, Union, Optional

from psycopg.types.json import Jsonb

from rhosocial.activerecord.backend.type_adapter import SQLTypeAdapter
from rhosocial.activerecord.backend.schema import DatabaseType


class PostgresJSONBAdapter(SQLTypeAdapter):
    """
    Adapts Python dict/list to PostgreSQL JSONB and vice-versa.
    """
    @property
    def supported_types(self) -> Dict[Type, List[Any]]:
        return {dict: [Jsonb], list: [Jsonb]}

    def to_database(self, value: Union[dict, list], target_type: Type, options: Optional[Dict[str, Any]] = None) -> Any:
        if value is None:
            return None
        return Jsonb(value)

    def from_database(self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None) -> Union[dict, list]:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
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
