# src/rhosocial/activerecord/backend/impl/postgres/types/constants.py
"""PostgreSQL data type constants.

These constants provide convenient type name references for use with
type casting operations. Users can also use string type names directly.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype.html

Example:
    >>> from rhosocial.activerecord.backend.impl.postgres.types.constants import MONEY, NUMERIC
    >>> col.cast(MONEY)  # Equivalent to col.cast("money")
    >>> col.cast("money")  # Also valid

Note:
    These constants are provided for convenience and code clarity.
    Users are free to use string literals directly if preferred.
"""

# =============================================================================
# Numeric Types
# =============================================================================
SMALLINT = 'smallint'
INTEGER = 'integer'
INT = 'int'  # Alias for integer
BIGINT = 'bigint'
DECIMAL = 'decimal'
NUMERIC = 'numeric'
REAL = 'real'
DOUBLE_PRECISION = 'double precision'
FLOAT8 = 'float8'  # Alias for double precision
FLOAT4 = 'float4'  # Alias for real
SMALLSERIAL = 'smallserial'
SERIAL = 'serial'
BIGSERIAL = 'bigserial'

# =============================================================================
# Monetary Types
# =============================================================================
MONEY = 'money'

# =============================================================================
# Character Types
# =============================================================================
CHARACTER = 'character'
CHAR = 'char'
CHARACTER_VARYING = 'character varying'
VARCHAR = 'varchar'
TEXT = 'text'
NAME = 'name'  # Internal type for identifiers

# =============================================================================
# Binary Data Types
# =============================================================================
BYTEA = 'bytea'

# =============================================================================
# Date/Time Types
# =============================================================================
DATE = 'date'
TIME = 'time'
TIMETZ = 'timetz'
TIMESTAMP = 'timestamp'
TIMESTAMPTZ = 'timestamptz'
INTERVAL = 'interval'

# =============================================================================
# Boolean Type
# =============================================================================
BOOLEAN = 'boolean'
BOOL = 'bool'  # Alias for boolean

# =============================================================================
# Geometric Types
# =============================================================================
POINT = 'point'
LINE = 'line'
LSEG = 'lseg'
BOX = 'box'
PATH = 'path'
POLYGON = 'polygon'
CIRCLE = 'circle'

# =============================================================================
# Network Address Types
# =============================================================================
INET = 'inet'
CIDR = 'cidr'
MACADDR = 'macaddr'
MACADDR8 = 'macaddr8'

# =============================================================================
# Bit String Types
# =============================================================================
BIT = 'bit'
VARBIT = 'varbit'

# =============================================================================
# Text Search Types
# =============================================================================
TSVECTOR = 'tsvector'
TSQUERY = 'tsquery'

# =============================================================================
# UUID Type
# =============================================================================
UUID = 'uuid'

# =============================================================================
# XML Type
# =============================================================================
XML = 'xml'

# =============================================================================
# JSON Types
# =============================================================================
JSON = 'json'
JSONB = 'jsonb'
JSONPATH = 'jsonpath'

# =============================================================================
# Array Types (examples)
# =============================================================================
# Note: Array types are typically written as base_type[]
# These are common examples; users can create any array type
INT_ARRAY = 'integer[]'
INT4_ARRAY = 'int4[]'
INT8_ARRAY = 'int8[]'
TEXT_ARRAY = 'text[]'
VARCHAR_ARRAY = 'varchar[]'
NUMERIC_ARRAY = 'numeric[]'
BOOLEAN_ARRAY = 'boolean[]'
JSONB_ARRAY = 'jsonb[]'

# =============================================================================
# Composite Types
# =============================================================================
# Composite types are user-defined; no predefined constants

# =============================================================================
# Range Types
# =============================================================================
INT4RANGE = 'int4range'
INT8RANGE = 'int8range'
NUMRANGE = 'numrange'
TSRANGE = 'tsrange'
TSTZRANGE = 'tstzrange'
DATERANGE = 'daterange'

# =============================================================================
# Multirange Types (PostgreSQL 14+)
# =============================================================================
INT4MULTIRANGE = 'int4multirange'
INT8MULTIRANGE = 'int8multirange'
NUMMULTIRANGE = 'nummultirange'
TSMULTIRANGE = 'tsmultirange'
TSTZMULTIRANGE = 'tstzmultirange'
DATEMULTIRANGE = 'datemultirange'

# =============================================================================
# Object Identifier Types
# =============================================================================
OID = 'oid'
REGCLASS = 'regclass'
REGTYPE = 'regtype'
REGPROC = 'regproc'
REGPROCEDURE = 'regprocedure'
REGOPER = 'regoper'
REGOPERATOR = 'regoperator'
REGCONFIG = 'regconfig'
REGDICTIONARY = 'regdictionary'
REGNAMESPACE = 'regnamespace'
REGROLE = 'regrole'
REGCOLLATION = 'regcollation'

# =============================================================================
# pg_lsn Type
# =============================================================================
PG_LSN = 'pg_lsn'

# =============================================================================
# Pseudo-Types
# =============================================================================
ANYTYPE = 'any'
ANYELEMENT = 'anyelement'
ANYARRAY = 'anyarray'
ANYNONARRAY = 'anynonarray'
ANYENUM = 'anyenum'
ANYRANGE = 'anyrange'
ANYMULTIRANGE = 'anymultirange'
VOID = 'void'
TRIGGER = 'trigger'
EVENT_TRIGGER = 'event_trigger'
LANGUAGE_HANDLER = 'language_handler'
FDW_HANDLER = 'fdw_handler'
TABLE_AM_HANDLER = 'table_am_handler'
INDEX_AM_HANDLER = 'index_am_handler'
TSM_HANDLER = 'tsm_handler'
INTERNAL = 'internal'
OPAQUE = 'opaque'
UNKNOWN = 'unknown'
RECORD = 'record'

# =============================================================================
# Type Category Constants (for type compatibility checking)
# =============================================================================
CATEGORY_BOOLEAN = 'B'
CATEGORY_COMPOSITE = 'C'
CATEGORY_DATE_TIME = 'D'
CATEGORY_ENUM = 'E'
CATEGORY_GEOMETRIC = 'G'
CATEGORY_NETWORK_ADDR = 'I'
CATEGORY_NUMERIC = 'N'
CATEGORY_PSEUDOTYPE = 'P'
CATEGORY_STRING = 'S'
CATEGORY_TIMESPAN = 'T'
CATEGORY_USER_DEFINED = 'U'
CATEGORY_BIT_STRING = 'V'
CATEGORY_UNKNOWN = 'X'

__all__ = [
    # Numeric Types
    'SMALLINT', 'INTEGER', 'INT', 'BIGINT', 'DECIMAL', 'NUMERIC',
    'REAL', 'DOUBLE_PRECISION', 'FLOAT8', 'FLOAT4',
    'SMALLSERIAL', 'SERIAL', 'BIGSERIAL',
    # Monetary Types
    'MONEY',
    # Character Types
    'CHARACTER', 'CHAR', 'CHARACTER_VARYING', 'VARCHAR', 'TEXT', 'NAME',
    # Binary Data Types
    'BYTEA',
    # Date/Time Types
    'DATE', 'TIME', 'TIMETZ', 'TIMESTAMP', 'TIMESTAMPTZ', 'INTERVAL',
    # Boolean Type
    'BOOLEAN', 'BOOL',
    # Geometric Types
    'POINT', 'LINE', 'LSEG', 'BOX', 'PATH', 'POLYGON', 'CIRCLE',
    # Network Address Types
    'INET', 'CIDR', 'MACADDR', 'MACADDR8',
    # Bit String Types
    'BIT', 'VARBIT',
    # Text Search Types
    'TSVECTOR', 'TSQUERY',
    # UUID Type
    'UUID',
    # XML Type
    'XML',
    # JSON Types
    'JSON', 'JSONB', 'JSONPATH',
    # Array Types
    'INT_ARRAY', 'INT4_ARRAY', 'INT8_ARRAY', 'TEXT_ARRAY',
    'VARCHAR_ARRAY', 'NUMERIC_ARRAY', 'BOOLEAN_ARRAY', 'JSONB_ARRAY',
    # Range Types
    'INT4RANGE', 'INT8RANGE', 'NUMRANGE', 'TSRANGE', 'TSTZRANGE', 'DATERANGE',
    # Multirange Types
    'INT4MULTIRANGE', 'INT8MULTIRANGE', 'NUMMULTIRANGE',
    'TSMULTIRANGE', 'TSTZMULTIRANGE', 'DATEMULTIRANGE',
    # Object Identifier Types
    'OID', 'REGCLASS', 'REGTYPE', 'REGPROC', 'REGPROCEDURE',
    'REGOPER', 'REGOPERATOR', 'REGCONFIG', 'REGDICTIONARY',
    'REGNAMESPACE', 'REGROLE', 'REGCOLLATION',
    # pg_lsn Type
    'PG_LSN',
    # Pseudo-Types
    'ANYTYPE', 'ANYELEMENT', 'ANYARRAY', 'ANYNONARRAY', 'ANYENUM',
    'ANYRANGE', 'ANYMULTIRANGE', 'VOID', 'TRIGGER', 'EVENT_TRIGGER',
    'LANGUAGE_HANDLER', 'FDW_HANDLER', 'TABLE_AM_HANDLER',
    'INDEX_AM_HANDLER', 'TSM_HANDLER', 'INTERNAL', 'OPAQUE',
    'UNKNOWN', 'RECORD',
    # Type Categories
    'CATEGORY_BOOLEAN', 'CATEGORY_COMPOSITE', 'CATEGORY_DATE_TIME',
    'CATEGORY_ENUM', 'CATEGORY_GEOMETRIC', 'CATEGORY_NETWORK_ADDR',
    'CATEGORY_NUMERIC', 'CATEGORY_PSEUDOTYPE', 'CATEGORY_STRING',
    'CATEGORY_TIMESPAN', 'CATEGORY_USER_DEFINED', 'CATEGORY_BIT_STRING',
    'CATEGORY_UNKNOWN',
]
