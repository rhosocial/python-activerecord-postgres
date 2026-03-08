# src/rhosocial/activerecord/backend/impl/postgres/types/__init__.py
"""
PostgreSQL type definitions.

This module exports all PostgreSQL-specific type classes, organized
according to PostgreSQL documentation structure.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype.html

Type modules and their corresponding PostgreSQL documentation:
- enum: Enum types - https://www.postgresql.org/docs/current/datatype-enum.html
- bit_string: Bit string types (bit, varbit) - https://www.postgresql.org/docs/current/datatype-bit.html
- geometric: Geometric types (point, line, lseg, box, path, polygon, circle) - https://www.postgresql.org/docs/current/datatype-geometric.html
- range: Range types - https://www.postgresql.org/docs/current/rangetypes.html
- text_search: Text search types (tsvector, tsquery) - https://www.postgresql.org/docs/current/datatype-textsearch.html
- object_identifier: Object identifier types (oid, regclass, etc.) - https://www.postgresql.org/docs/current/datatype-oid.html
- json: JSON types (jsonpath) - https://www.postgresql.org/docs/current/datatype-json.html
- pg_lsn: pg_lsn type - https://www.postgresql.org/docs/current/datatype-pg-lsn.html
- network_address: Network address types (macaddr8) - https://www.postgresql.org/docs/current/datatype-net-types.html
- monetary: MONEY type - https://www.postgresql.org/docs/current/datatype-money.html
- xml: XML type - https://www.postgresql.org/docs/current/datatype-xml.html
"""

# Enum types
from .enum import (
    PostgresEnumType,
    EnumTypeManager,
)

# Bit string types
from .bit_string import (
    PostgresBitString,
)

# Geometric types
from .geometric import (
    Point,
    Line,
    LineSegment,
    Box,
    Path,
    Polygon,
    Circle,
)

# Range types
from .range import (
    PostgresRange,
    PostgresMultirange,
)

# Text search types
from .text_search import (
    TsVectorLexeme,
    PostgresTsVector,
    PostgresTsQuery,
)

# Object identifier types
from .object_identifier import (
    OID,
    RegClass,
    RegType,
    RegProc,
    RegProcedure,
    RegOper,
    RegOperator,
    RegConfig,
    RegDictionary,
    RegNamespace,
    RegRole,
    RegCollation,
    XID,
    XID8,
    CID,
    TID,
)

# JSON types
from .json import (
    PostgresJsonPath,
)

# pg_lsn type
from .pg_lsn import (
    PostgresLsn,
)

# Network address types
from .network_address import (
    PostgresMacaddr,
    PostgresMacaddr8,
)

# Monetary types
from .monetary import (
    PostgresMoney,
)

# XML type
from .xml import (
    PostgresXML,
    xmlparse,
    xpath_query,
    xpath_exists,
    xml_is_well_formed,
)

# Type constants (convenience references)
from .constants import (
    # Numeric Types
    SMALLINT, INTEGER, INT, BIGINT, DECIMAL, NUMERIC,
    REAL, DOUBLE_PRECISION, FLOAT8, FLOAT4,
    SMALLSERIAL, SERIAL, BIGSERIAL,
    # Monetary Types
    MONEY,
    # Character Types
    CHARACTER, CHAR, CHARACTER_VARYING, VARCHAR, TEXT, NAME,
    # Binary Data Types
    BYTEA,
    # Date/Time Types
    DATE, TIME, TIMETZ, TIMESTAMP, TIMESTAMPTZ, INTERVAL,
    # Boolean Type
    BOOLEAN, BOOL,
    # Geometric Types
    POINT as GEOM_POINT, LINE as GEOM_LINE, LSEG, BOX, PATH, POLYGON, CIRCLE,
    # Network Address Types
    INET, CIDR, MACADDR, MACADDR8,
    # Bit String Types
    BIT, VARBIT,
    # Text Search Types
    TSVECTOR, TSQUERY,
    # UUID Type
    UUID,
    # XML Type
    XML,
    # JSON Types
    JSON, JSONB, JSONPATH,
    # Array Types
    INT_ARRAY, INT4_ARRAY, INT8_ARRAY, TEXT_ARRAY,
    VARCHAR_ARRAY, NUMERIC_ARRAY, BOOLEAN_ARRAY, JSONB_ARRAY,
    # Range Types
    INT4RANGE, INT8RANGE, NUMRANGE, TSRANGE, TSTZRANGE, DATERANGE,
    # Multirange Types
    INT4MULTIRANGE, INT8MULTIRANGE, NUMMULTIRANGE,
    TSMULTIRANGE, TSTZMULTIRANGE, DATEMULTIRANGE,
    # Object Identifier Types
    OID as OID_TYPE, REGCLASS, REGTYPE, REGPROC, REGPROCEDURE,
    REGOPER, REGOPERATOR, REGCONFIG, REGDICTIONARY,
    REGNAMESPACE, REGROLE, REGCOLLATION,
    # pg_lsn Type
    PG_LSN,
)


__all__ = [
    # Enum types
    'PostgresEnumType',
    'EnumTypeManager',

    # Bit string types
    'PostgresBitString',

    # Geometric types
    'Point',
    'Line',
    'LineSegment',
    'Box',
    'Path',
    'Polygon',
    'Circle',

    # Range types
    'PostgresRange',
    'PostgresMultirange',

    # Text search types
    'TsVectorLexeme',
    'PostgresTsVector',
    'PostgresTsQuery',

    # Object identifier types
    'OID',
    'RegClass',
    'RegType',
    'RegProc',
    'RegProcedure',
    'RegOper',
    'RegOperator',
    'RegConfig',
    'RegDictionary',
    'RegNamespace',
    'RegRole',
    'RegCollation',
    'XID',
    'XID8',
    'CID',
    'TID',

    # JSON types
    'PostgresJsonPath',

    # pg_lsn type
    'PostgresLsn',

    # Network address types
    'PostgresMacaddr',
    'PostgresMacaddr8',

    # Monetary types
    'PostgresMoney',

    # XML type
    'PostgresXML',
    'xmlparse',
    'xpath_query',
    'xpath_exists',
    'xml_is_well_formed',

    # Type constants - Numeric
    'SMALLINT', 'INTEGER', 'INT', 'BIGINT', 'DECIMAL', 'NUMERIC',
    'REAL', 'DOUBLE_PRECISION', 'FLOAT8', 'FLOAT4',
    'SMALLSERIAL', 'SERIAL', 'BIGSERIAL',
    # Type constants - Monetary
    'MONEY',
    # Type constants - Character
    'CHARACTER', 'CHAR', 'CHARACTER_VARYING', 'VARCHAR', 'TEXT', 'NAME',
    # Type constants - Binary
    'BYTEA',
    # Type constants - Date/Time
    'DATE', 'TIME', 'TIMETZ', 'TIMESTAMP', 'TIMESTAMPTZ', 'INTERVAL',
    # Type constants - Boolean
    'BOOLEAN', 'BOOL',
    # Type constants - Geometric
    'GEOM_POINT', 'GEOM_LINE', 'LSEG', 'BOX', 'PATH', 'POLYGON', 'CIRCLE',
    # Type constants - Network Address
    'INET', 'CIDR', 'MACADDR', 'MACADDR8',
    # Type constants - Bit String
    'BIT', 'VARBIT',
    # Type constants - Text Search
    'TSVECTOR', 'TSQUERY',
    # Type constants - UUID
    'UUID',
    # Type constants - XML
    'XML',
    # Type constants - JSON
    'JSON', 'JSONB', 'JSONPATH',
    # Type constants - Array
    'INT_ARRAY', 'INT4_ARRAY', 'INT8_ARRAY', 'TEXT_ARRAY',
    'VARCHAR_ARRAY', 'NUMERIC_ARRAY', 'BOOLEAN_ARRAY', 'JSONB_ARRAY',
    # Type constants - Range
    'INT4RANGE', 'INT8RANGE', 'NUMRANGE', 'TSRANGE', 'TSTZRANGE', 'DATERANGE',
    # Type constants - Multirange
    'INT4MULTIRANGE', 'INT8MULTIRANGE', 'NUMMULTIRANGE',
    'TSMULTIRANGE', 'TSTZMULTIRANGE', 'DATEMULTIRANGE',
    # Type constants - Object Identifier (with alias)
    'OID_TYPE', 'REGCLASS', 'REGTYPE', 'REGPROC', 'REGPROCEDURE',
    'REGOPER', 'REGOPERATOR', 'REGCONFIG', 'REGDICTIONARY',
    'REGNAMESPACE', 'REGROLE', 'REGCOLLATION',
    # Type constants - pg_lsn
    'PG_LSN',
]
