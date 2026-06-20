# src/rhosocial/activerecord/backend/impl/postgres/expression/types.py
"""PostgreSQL-specific DataType subclasses.

Naming convention
-----------------
PostgreSQL-specific types use the ``Postgres`` prefix to distinguish them
from the core types (which have no prefix).  This avoids ambiguity when both
core and backend types are used together.

Usage scope
-----------
These types are used **only** for PostgreSQL backend DDL column definitions,
introspection result parsing, and schema comparison.  They should **not**
be used by application code directly — always use the core types for
DDL definition expressions (``ColumnDefinition.data_type``).
"""

from __future__ import annotations

from typing import Optional, Set

from rhosocial.activerecord.backend.expression.types import (
    ArrayType,
    BlobType,
    DataType,
    IntegerType,
    SmallIntType,
    BigIntType,
    VarCharType,
)


# ---------------------------------------------------------------------------
# Character varying alias
# ---------------------------------------------------------------------------

class PostgresCharacterVaryingType(VarCharType, backend="postgres"):
    """PostgreSQL ``CHARACTER VARYING(n)`` — alias for ``VARCHAR(n)``."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'VarCharType'}

    def _default_sql(self) -> str:
        if self.length is not None:
            return f"CHARACTER VARYING({self.length})"
        return "CHARACTER VARYING"


# ---------------------------------------------------------------------------
# Binary data: BYTEA
# ---------------------------------------------------------------------------

class PostgresByteaType(BlobType, backend="postgres"):
    """PostgreSQL ``BYTEA`` — variable-length binary string."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'BlobType'}

    def _default_sql(self) -> str:
        return "BYTEA"


# ---------------------------------------------------------------------------
# Serial (auto-increment) types
# ---------------------------------------------------------------------------

class PostgresSmallSerialType(DataType, backend="postgres"):
    """PostgreSQL ``SMALLSERIAL`` — auto-incrementing SMALLINT (2 bytes)."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'SmallSerialType', 'SmallIntType'}

    def _default_sql(self) -> str:
        return "SMALLSERIAL"


class PostgresSerialType(DataType, backend="postgres"):
    """PostgreSQL ``SERIAL`` — auto-incrementing INTEGER (4 bytes)."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'SerialType', 'IntegerType'}

    def _default_sql(self) -> str:
        return "SERIAL"


class PostgresBigSerialType(DataType, backend="postgres"):
    """PostgreSQL ``BIGSERIAL`` — auto-incrementing BIGINT (8 bytes)."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'BigSerialType', 'BigIntType'}

    def _default_sql(self) -> str:
        return "BIGSERIAL"


# ---------------------------------------------------------------------------
# UUID
# ---------------------------------------------------------------------------

class PostgresUUIDType(DataType, backend="postgres"):
    """PostgreSQL ``UUID`` — universally unique identifier."""

    def _default_sql(self) -> str:
        return "UUID"


# ---------------------------------------------------------------------------
# XML
# ---------------------------------------------------------------------------

class PostgresXMLType(DataType, backend="postgres"):
    """PostgreSQL ``XML`` — XML data type."""

    def _default_sql(self) -> str:
        return "XML"


# ---------------------------------------------------------------------------
# Text search
# ---------------------------------------------------------------------------

class PostgresTSVectorType(DataType, backend="postgres"):
    """PostgreSQL ``TSVECTOR`` — text search document."""

    def _default_sql(self) -> str:
        return "TSVECTOR"


class PostgresTSQueryType(DataType, backend="postgres"):
    """PostgreSQL ``TSQUERY`` — text search query."""

    def _default_sql(self) -> str:
        return "TSQUERY"


# ---------------------------------------------------------------------------
# JSON path
# ---------------------------------------------------------------------------

class PostgresJsonPathType(DataType, backend="postgres"):
    """PostgreSQL ``JSONPATH`` — SQL/JSON path expression (PG 12+)."""

    def _default_sql(self) -> str:
        return "JSONPATH"


# ---------------------------------------------------------------------------
# Bit string types
# ---------------------------------------------------------------------------

class PostgresBitType(DataType, backend="postgres"):
    """PostgreSQL ``BIT(n)`` — fixed-length bit string."""

    n: Optional[int] = None

    def __init__(self, n: Optional[int] = None):
        super().__init__()
        self.n = n

    def __eq__(self, other: object) -> bool:
        if type(self) is not type(other):
            return False
        return self.n == other.n

    def __hash__(self) -> int:
        return hash((type(self), self.n))

    def _default_sql(self) -> str:
        if self.n is not None:
            return f"BIT({self.n})"
        return "BIT"


class PostgresVarBitType(DataType, backend="postgres"):
    """PostgreSQL ``VARBIT(n)`` — variable-length bit string."""

    n: Optional[int] = None

    def __init__(self, n: Optional[int] = None):
        super().__init__()
        self.n = n

    def __eq__(self, other: object) -> bool:
        if type(self) is not type(other):
            return False
        return self.n == other.n

    def __hash__(self) -> int:
        return hash((type(self), self.n))

    def _default_sql(self) -> str:
        if self.n is not None:
            return f"VARBIT({self.n})"
        return "VARBIT"


# ---------------------------------------------------------------------------
# Network address types
# ---------------------------------------------------------------------------

class PostgresInetType(DataType, backend="postgres"):
    """PostgreSQL ``INET`` — IPv4 or IPv6 address."""

    def _default_sql(self) -> str:
        return "INET"


class PostgresCidrType(DataType, backend="postgres"):
    """PostgreSQL ``CIDR`` — IPv4 or IPv6 network."""

    def _default_sql(self) -> str:
        return "CIDR"


class PostgresMacAddrType(DataType, backend="postgres"):
    """PostgreSQL ``MACADDR`` — MAC address (EUI-48)."""

    def _default_sql(self) -> str:
        return "MACADDR"


class PostgresMacAddr8Type(DataType, backend="postgres"):
    """PostgreSQL ``MACADDR8`` — MAC address (EUI-64, PG 10+)."""

    def _default_sql(self) -> str:
        return "MACADDR8"


# ---------------------------------------------------------------------------
# Geometric types
# ---------------------------------------------------------------------------

class PostgresPointType(DataType, backend="postgres"):
    """PostgreSQL ``POINT`` — geometric point (x, y)."""

    def _default_sql(self) -> str:
        return "POINT"


class PostgresLineType(DataType, backend="postgres"):
    """PostgreSQL ``LINE`` — infinite line."""

    def _default_sql(self) -> str:
        return "LINE"


class PostgresLineSegmentType(DataType, backend="postgres"):
    """PostgreSQL ``LSEG`` — line segment."""

    def _default_sql(self) -> str:
        return "LSEG"


class PostgresBoxType(DataType, backend="postgres"):
    """PostgreSQL ``BOX`` — rectangular box."""

    def _default_sql(self) -> str:
        return "BOX"


class PostgresPathType(DataType, backend="postgres"):
    """PostgreSQL ``PATH`` — open or closed geometric path."""

    def _default_sql(self) -> str:
        return "PATH"


class PostgresPolygonType(DataType, backend="postgres"):
    """PostgreSQL ``POLYGON`` — closed geometric polygon."""

    def _default_sql(self) -> str:
        return "POLYGON"


class PostgresCircleType(DataType, backend="postgres"):
    """PostgreSQL ``CIRCLE`` — circle (center + radius)."""

    def _default_sql(self) -> str:
        return "CIRCLE"


# ---------------------------------------------------------------------------
# Monetary type
# ---------------------------------------------------------------------------

class PostgresMoneyType(DataType, backend="postgres"):
    """PostgreSQL ``MONEY`` — currency amount."""

    def _default_sql(self) -> str:
        return "MONEY"


# ---------------------------------------------------------------------------
# Range types
# ---------------------------------------------------------------------------

class PostgresInt4RangeType(DataType, backend="postgres"):
    """PostgreSQL ``INT4RANGE`` — range of integer."""

    def _default_sql(self) -> str:
        return "INT4RANGE"


class PostgresInt8RangeType(DataType, backend="postgres"):
    """PostgreSQL ``INT8RANGE`` — range of bigint."""

    def _default_sql(self) -> str:
        return "INT8RANGE"


class PostgresNumRangeType(DataType, backend="postgres"):
    """PostgreSQL ``NUMRANGE`` — range of numeric."""

    def _default_sql(self) -> str:
        return "NUMRANGE"


class PostgresTsRangeType(DataType, backend="postgres"):
    """PostgreSQL ``TSRANGE`` — range of timestamp without time zone."""

    def _default_sql(self) -> str:
        return "TSRANGE"


class PostgresTsTzRangeType(DataType, backend="postgres"):
    """PostgreSQL ``TSTZRANGE`` — range of timestamp with time zone."""

    def _default_sql(self) -> str:
        return "TSTZRANGE"


class PostgresDateRangeType(DataType, backend="postgres"):
    """PostgreSQL ``DATERANGE`` — range of date."""

    def _default_sql(self) -> str:
        return "DATERANGE"


# ---------------------------------------------------------------------------
# Multirange types (PG 14+)
# ---------------------------------------------------------------------------

class PostgresInt4MultirangeType(DataType, backend="postgres"):
    """PostgreSQL ``INT4MULTIRANGE`` — multirange of integer (PG 14+)."""

    def _default_sql(self) -> str:
        return "INT4MULTIRANGE"


class PostgresInt8MultirangeType(DataType, backend="postgres"):
    """PostgreSQL ``INT8MULTIRANGE`` — multirange of bigint (PG 14+)."""

    def _default_sql(self) -> str:
        return "INT8MULTIRANGE"


class PostgresNumMultirangeType(DataType, backend="postgres"):
    """PostgreSQL ``NUMMULTIRANGE`` — multirange of numeric (PG 14+)."""

    def _default_sql(self) -> str:
        return "NUMMULTIRANGE"


class PostgresTsMultirangeType(DataType, backend="postgres"):
    """PostgreSQL ``TSMULTIRANGE`` — multirange of timestamp (PG 14+)."""

    def _default_sql(self) -> str:
        return "TSMULTIRANGE"


class PostgresTsTzMultirangeType(DataType, backend="postgres"):
    """PostgreSQL ``TSTZMULTIRANGE`` — multirange of timestamptz (PG 14+)."""

    def _default_sql(self) -> str:
        return "TSTZMULTIRANGE"


class PostgresDateMultirangeType(DataType, backend="postgres"):
    """PostgreSQL ``DATEMULTIRANGE`` — multirange of date (PG 14+)."""

    def _default_sql(self) -> str:
        return "DATEMULTIRANGE"


# ---------------------------------------------------------------------------
# Object identifier types
# ---------------------------------------------------------------------------

class PostgresOIDType(DataType, backend="postgres"):
    """PostgreSQL ``OID`` — object identifier."""

    def _default_sql(self) -> str:
        return "OID"


class PostgresRegClassType(DataType, backend="postgres"):
    """PostgreSQL ``REGCLASS`` — relation name (OID alias)."""

    def _default_sql(self) -> str:
        return "REGCLASS"


class PostgresRegTypeType(DataType, backend="postgres"):
    """PostgreSQL ``REGTYPE`` — type name (OID alias)."""

    def _default_sql(self) -> str:
        return "REGTYPE"


class PostgresXIDType(DataType, backend="postgres"):
    """PostgreSQL ``XID`` — transaction ID."""

    def _default_sql(self) -> str:
        return "XID"


class PostgresXID8Type(DataType, backend="postgres"):
    """PostgreSQL ``XID8`` — 64-bit transaction ID (PG 13+)."""

    def _default_sql(self) -> str:
        return "XID8"


class PostgresCIDType(DataType, backend="postgres"):
    """PostgreSQL ``CID`` — command ID."""

    def _default_sql(self) -> str:
        return "CID"


class PostgresTIDType(DataType, backend="postgres"):
    """PostgreSQL ``TID`` — tuple ID (page, tuple)."""

    def _default_sql(self) -> str:
        return "TID"


# ---------------------------------------------------------------------------
# pg_lsn type
# ---------------------------------------------------------------------------

class PostgresPgLSNType(DataType, backend="postgres"):
    """PostgreSQL ``PG_LSN`` — WAL log sequence number."""

    def _default_sql(self) -> str:
        return "PG_LSN"


# ---------------------------------------------------------------------------
# Extension-provided types (minimal DataType wrappers)
# ---------------------------------------------------------------------------

class PostgresHstoreType(DataType, backend="postgres"):
    """PostgreSQL ``HSTORE`` — key-value store (hstore extension)."""

    def _default_sql(self) -> str:
        return "HSTORE"


class PostgresGeometryType(DataType, backend="postgres"):
    """PostGIS ``GEOMETRY`` — generic spatial geometry (PostGIS extension).

    For production use, subclass with SRID support as needed.
    """

    def _default_sql(self) -> str:
        return "GEOMETRY"


class PostgresGeographyType(DataType, backend="postgres"):
    """PostGIS ``GEOGRAPHY`` — geodetic spatial type (PostGIS extension)."""

    def _default_sql(self) -> str:
        return "GEOGRAPHY"


class PostgresVectorType(DataType, backend="postgres"):
    """pgvector ``VECTOR(n)`` — vector embedding (pgvector extension).

    Args:
        dim: Number of dimensions.
    """

    dim: int

    def __init__(self, dim: int):
        super().__init__()
        self.dim = dim

    def __eq__(self, other: object) -> bool:
        if type(self) is not type(other):
            return False
        return self.dim == other.dim

    def __hash__(self) -> int:
        return hash((type(self), self.dim))

    def _default_sql(self) -> str:
        return f"VECTOR({self.dim})"


# ---------------------------------------------------------------------------
# Array container
# ---------------------------------------------------------------------------

# Backward-compatible alias — core ``ArrayType`` provides the implementation.
PostgresArrayType = ArrayType
