# src/rhosocial/activerecord/backend/impl/postgres/protocols/types/jsonb_enhanced.py
"""PostgreSQL JSONB enhanced support protocol definition.

This module defines the protocol for PostgreSQL-specific JSON/JSONB
enhancements beyond the core JSON support.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresJSONBEnhancedSupport(Protocol):
    """PostgreSQL JSONB enhanced support protocol.

    PostgreSQL provides the JSONB data type with indexing support,
    JSON path expressions, and various query functions that go
    beyond basic JSON operations.

    JSONB enhanced features:
    - JSON type (PG 9.2+)
    - JSONB type with indexing (PG 9.4+)
    - JSON path functions (PG 12+): jsonb_path_query, jsonb_path_exists, etc.
    - JSON_TABLE function (PG 12+)
    - JSONB subscript notation (PG 14+)
    - Numeric infinity in JSONB (PG 17+)

    Feature Source: Native support (no extension required)

    Official Documentation:
    https://www.postgresql.org/docs/current/datatype-json.html
    https://www.postgresql.org/docs/current/functions-json.html

    Version Requirements:
    - JSON type: PostgreSQL 9.2+
    - JSONB type: PostgreSQL 9.4+
    - JSON path / JSON_TABLE: PostgreSQL 12+
    - JSONB subscript: PostgreSQL 14+
    - Numeric infinity in JSONB: PostgreSQL 17+
    """

    def supports_json_type(self) -> bool:
        """Whether JSON data type is supported.

        Native feature, PostgreSQL 9.2+.
        The JSON type stores text-based JSON data.
        """
        ...

    def supports_jsonb(self) -> bool:
        """Whether JSONB data type is supported.

        Native feature, PostgreSQL 9.4+.
        The JSONB type stores binary-format JSON data with
        indexing support for efficient querying.
        """
        ...

    def supports_json_path(self) -> bool:
        """Whether JSON path expressions are supported.

        Native feature, PostgreSQL 12+.
        Includes jsonb_path_query, jsonb_path_query_first,
        jsonb_path_exists, and jsonb_path_match functions.
        """
        ...

    def supports_json_table(self) -> bool:
        """Whether JSON_TABLE function is supported.

        Native feature, PostgreSQL 12+.
        Converts JSON data into relational table format.
        """
        ...

    def supports_jsonb_subscript(self) -> bool:
        """Whether JSONB subscript notation is supported.

        Native feature, PostgreSQL 14+.
        Enables jsonb['key'] subscript syntax for accessing values.
        """
        ...

    def supports_infinity_numeric_infinity_jsonb(self) -> bool:
        """Whether numeric infinity values are allowed in JSONB.

        Native feature, PostgreSQL 17+.
        Allows Infinity and -Infinity in numeric context within JSONB.
        """
        ...
