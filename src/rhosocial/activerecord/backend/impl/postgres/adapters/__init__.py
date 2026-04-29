# src/rhosocial/activerecord/backend/impl/postgres/adapters/__init__.py
"""
PostgreSQL type adapters.

This module exports all type adapters for PostgreSQL-specific types,
organized according to PostgreSQL documentation structure.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype.html

Adapter modules and their corresponding PostgreSQL documentation:
- base: Basic types (array, jsonb, network address, enum)
- monetary: MONEY type - https://www.postgresql.org/docs/current/datatype-money.html
- network_address: Network address types (inet, cidr, macaddr, macaddr8) - https://www.postgresql.org/docs/current/datatype-net-types.html
- geometric: Geometric types (point, line, lseg, box, path, polygon, circle) - https://www.postgresql.org/docs/current/datatype-geometric.html
- bit_string: Bit string types (bit, varbit) - https://www.postgresql.org/docs/current/datatype-bit.html
- text_search: Text search types (tsvector, tsquery) - https://www.postgresql.org/docs/current/datatype-textsearch.html
- object_identifier: Object identifier types (oid, regclass, etc.) - https://www.postgresql.org/docs/current/datatype-oid.html
- json: JSON types (json, jsonb, jsonpath) - https://www.postgresql.org/docs/current/datatype-json.html
- range: Range types - https://www.postgresql.org/docs/current/rangetypes.html
- enum: Enum types - https://www.postgresql.org/docs/current/datatype-enum.html
- xml: XML type - https://www.postgresql.org/docs/current/datatype-xml.html
- pg_lsn: pg_lsn type - https://www.postgresql.org/docs/current/datatype-pg-lsn.html
- uuid: UUID type - https://www.postgresql.org/docs/current/datatype-uuid.html
- pgvector: pgvector vector type - https://github.com/pgvector/pgvector
"""

# Base types
from .base import (
    PostgresListAdapter,
    PostgresJSONBAdapter,
    PostgresEnumAdapter,
)

# Monetary types
from .monetary import PostgresMoneyAdapter

# Network address types
from .network_address import (
    PostgresNetworkAddressAdapter,
    PostgresMacaddrAdapter,
    PostgresMacaddr8Adapter,
)

# Geometric types
from .geometric import PostgresGeometryAdapter

# Bit string types
from .bit_string import PostgresBitStringAdapter

# Text search types
from .text_search import (
    PostgresTsVectorAdapter,
    PostgresTsQueryAdapter,
)

# Object identifier types
from .object_identifier import (
    PostgresOidAdapter,
    PostgresXidAdapter,
    PostgresTidAdapter,
)

# JSON types
from .json import PostgresJsonPathAdapter

# Range types
from .range import (
    PostgresRangeAdapter,
    PostgresMultirangeAdapter,
)

# pg_lsn type
from .pg_lsn import PostgresLsnAdapter

# XML type
from .xml import PostgresXMLAdapter

# UUID type
from .uuid import PostgresUUIDAdapter

# pgvector types
from .pgvector import PostgresVectorAdapter

# PostGIS types
from .postgis import PostgresGeometryAdapter

# hstore types
from .hstore import PostgresHstoreAdapter

__all__ = [
    # Basic types
    "PostgresListAdapter",
    "PostgresJSONBAdapter",
    "PostgresNetworkAddressAdapter",
    "PostgresEnumAdapter",
    # Monetary types
    "PostgresMoneyAdapter",
    # Network address types
    "PostgresMacaddrAdapter",
    "PostgresMacaddr8Adapter",
    # Geometric types
    "PostgresGeometryAdapter",
    # Bit string types
    "PostgresBitStringAdapter",
    # Text search types
    "PostgresTsVectorAdapter",
    "PostgresTsQueryAdapter",
    # Object identifier types
    "PostgresOidAdapter",
    "PostgresXidAdapter",
    "PostgresTidAdapter",
    # JSON types
    "PostgresJsonPathAdapter",
    # Range types
    "PostgresRangeAdapter",
    "PostgresMultirangeAdapter",
    # pg_lsn type
    "PostgresLsnAdapter",
    # XML type
    "PostgresXMLAdapter",
    # UUID type
    "PostgresUUIDAdapter",
    # pgvector types
    "PostgresVectorAdapter",
    # PostGIS types
    "PostgresGeometryAdapter",
    # hstore types
    "PostgresHstoreAdapter",
]
