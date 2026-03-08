# src/rhosocial/activerecord/backend/impl/postgres/adapters/xml.py
"""
PostgreSQL XML Type Adapter.

This module provides type adapter for PostgreSQL XML type.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-xml.html

The XML type stores well-formed XML documents.
PostgreSQL provides XML functions and XPath support.
"""
from typing import Any, Dict, List, Optional, Set, Type

from ..types.xml import PostgresXML


class PostgresXMLAdapter:
    """PostgreSQL XML type adapter.

    This adapter converts between Python strings/PostgresXML and
    PostgreSQL XML values.

    Note: PostgreSQL validates XML on input and will raise an error
    for malformed XML.

    **IMPORTANT: This adapter is NOT registered by default in the backend's
    type registry.**

    Reason for not registering by default:
    This adapter registers str -> str type mapping, which would conflict
    with the default string handling in the base adapter registry.

    To use this adapter:

    1. Register it explicitly on your backend instance:
    ```python
    from rhosocial.activerecord.backend.impl.postgres import PostgresXMLAdapter
    adapter = PostgresXMLAdapter()
    backend.adapter_registry.register(adapter, PostgresXML, str)
    ```

    2. Specify it directly when executing queries:
    ```python
    from rhosocial.activerecord.backend.impl.postgres import PostgresXML, PostgresXMLAdapter
    xml_adapter = PostgresXMLAdapter()
    result = backend.execute(
        "INSERT INTO docs (content) VALUES (%s)",
        [PostgresXML("<root>value</root>")],
        type_adapters={PostgresXML: xml_adapter}
    )
    ```
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            PostgresXML: {str},
        }

    def to_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Convert Python value to PostgreSQL XML.

        Args:
            value: PostgresXML, str, or None
            target_type: Target type
            options: Optional conversion options

        Returns:
            XML string, or None
        """
        if value is None:
            return None

        if isinstance(value, PostgresXML):
            return value.content

        if isinstance(value, str):
            return value

        raise TypeError(f"Cannot convert {type(value).__name__} to XML")

    def from_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[PostgresXML]:
        """Convert PostgreSQL XML value to Python.

        Args:
            value: XML value from database
            target_type: Target Python type
            options: Optional conversion options

        Returns:
            PostgresXML object, or None
        """
        if value is None:
            return None

        if isinstance(value, PostgresXML):
            return value

        if isinstance(value, str):
            return PostgresXML(value)

        raise TypeError(f"Cannot convert {type(value).__name__} from XML")

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
    ) -> List[Optional[PostgresXML]]:
        """Batch convert values from database format."""
        return [self.from_database(v, target_type, options) for v in values]


__all__ = ['PostgresXMLAdapter']
