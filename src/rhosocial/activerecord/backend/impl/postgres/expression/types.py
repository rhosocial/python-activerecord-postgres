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

from typing import ClassVar, Optional, Set, Tuple

from rhosocial.activerecord.backend.expression.types import (
    BlobType,
    DataType,
    IntegerType,
    SmallIntType,
    BigIntType,
)


# ---------------------------------------------------------------------------
# Binary data: BYTEA
# ---------------------------------------------------------------------------

class PostgresByteaType(BlobType):
    """PostgreSQL ``BYTEA`` — variable-length binary string."""

    def _default_sql(self) -> str:
        return "BYTEA"


# ---------------------------------------------------------------------------
# Serial (auto-increment) types
# ---------------------------------------------------------------------------

class PostgresSmallSerialType(DataType):
    """PostgreSQL ``SMALLSERIAL`` — auto-incrementing SMALLINT (2 bytes)."""

    def _default_sql(self) -> str:
        return "SMALLSERIAL"

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'SmallSerialType'}


class PostgresSerialType(DataType):
    """PostgreSQL ``SERIAL`` — auto-incrementing INTEGER (4 bytes)."""

    def _default_sql(self) -> str:
        return "SERIAL"

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'SerialType', 'IntegerType'}


class PostgresBigSerialType(DataType):
    """PostgreSQL ``BIGSERIAL`` — auto-incrementing BIGINT (8 bytes)."""

    def _default_sql(self) -> str:
        return "BIGSERIAL"

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'BigSerialType'}


# ---------------------------------------------------------------------------
# UUID
# ---------------------------------------------------------------------------

class PostgresUUIDType(DataType):
    """PostgreSQL ``UUID`` — universally unique identifier."""

    def _default_sql(self) -> str:
        return "UUID"


# ---------------------------------------------------------------------------
# XML
# ---------------------------------------------------------------------------

class PostgresXMLType(DataType):
    """PostgreSQL ``XML`` — XML data type."""

    def _default_sql(self) -> str:
        return "XML"


# ---------------------------------------------------------------------------
# Text search
# ---------------------------------------------------------------------------

class PostgresTSVectorType(DataType):
    """PostgreSQL ``TSVECTOR`` — text search document."""

    def _default_sql(self) -> str:
        return "TSVECTOR"


class PostgresTSQueryType(DataType):
    """PostgreSQL ``TSQUERY`` — text search query."""

    def _default_sql(self) -> str:
        return "TSQUERY"


# ---------------------------------------------------------------------------
# JSON path
# ---------------------------------------------------------------------------

class PostgresJsonPathType(DataType):
    """PostgreSQL ``JSONPATH`` — SQL/JSON path expression (PG 12+)."""

    def _default_sql(self) -> str:
        return "JSONPATH"


# ---------------------------------------------------------------------------
# Bit string types
# ---------------------------------------------------------------------------

class PostgresBitType(DataType):
    """PostgreSQL ``BIT(n)`` — fixed-length bit string."""

    n: Optional[int] = None

    def __init__(self, n: Optional[int] = None):
        super().__init__()
        self.n = n

    def _type_params(self) -> Tuple:
        return (self.n,)

    def _default_sql(self) -> str:
        if self.n is not None:
            return f"BIT({self.n})"
        return "BIT"


class PostgresVarBitType(DataType):
    """PostgreSQL ``VARBIT(n)`` — variable-length bit string."""

    n: Optional[int] = None

    def __init__(self, n: Optional[int] = None):
        super().__init__()
        self.n = n

    def _type_params(self) -> Tuple:
        return (self.n,)

    def _default_sql(self) -> str:
        if self.n is not None:
            return f"VARBIT({self.n})"
        return "VARBIT"


# ---------------------------------------------------------------------------
# Network address types
# ---------------------------------------------------------------------------

class PostgresInetType(DataType):
    """PostgreSQL ``INET`` — IPv4 or IPv6 address."""

    def _default_sql(self) -> str:
        return "INET"


class PostgresCidrType(DataType):
    """PostgreSQL ``CIDR`` — IPv4 or IPv6 network."""

    def _default_sql(self) -> str:
        return "CIDR"


class PostgresMacAddrType(DataType):
    """PostgreSQL ``MACADDR`` — MAC address (EUI-48)."""

    def _default_sql(self) -> str:
        return "MACADDR"


class PostgresMacAddr8Type(DataType):
    """PostgreSQL ``MACADDR8`` — MAC address (EUI-64, PG 10+)."""

    def _default_sql(self) -> str:
        return "MACADDR8"


# ---------------------------------------------------------------------------
# Geometric types
# ---------------------------------------------------------------------------

class PostgresPointType(DataType):
    """PostgreSQL ``POINT`` — geometric point (x, y)."""

    def _default_sql(self) -> str:
        return "POINT"


class PostgresLineType(DataType):
    """PostgreSQL ``LINE`` — infinite line."""

    def _default_sql(self) -> str:
        return "LINE"


class PostgresLineSegmentType(DataType):
    """PostgreSQL ``LSEG`` — line segment."""

    def _default_sql(self) -> str:
        return "LSEG"


class PostgresBoxType(DataType):
    """PostgreSQL ``BOX`` — rectangular box."""

    def _default_sql(self) -> str:
        return "BOX"


class PostgresPathType(DataType):
    """PostgreSQL ``PATH`` — open or closed geometric path."""

    def _default_sql(self) -> str:
        return "PATH"


class PostgresPolygonType(DataType):
    """PostgreSQL ``POLYGON`` — closed geometric polygon."""

    def _default_sql(self) -> str:
        return "POLYGON"


class PostgresCircleType(DataType):
    """PostgreSQL ``CIRCLE`` — circle (center + radius)."""

    def _default_sql(self) -> str:
        return "CIRCLE"


# ---------------------------------------------------------------------------
# Monetary type
# ---------------------------------------------------------------------------

class PostgresMoneyType(DataType):
    """PostgreSQL ``MONEY`` — currency amount."""

    def _default_sql(self) -> str:
        return "MONEY"


# ---------------------------------------------------------------------------
# Range types
# ---------------------------------------------------------------------------

class PostgresInt4RangeType(DataType):
    """PostgreSQL ``INT4RANGE`` — range of integer."""

    def _default_sql(self) -> str:
        return "INT4RANGE"


class PostgresInt8RangeType(DataType):
    """PostgreSQL ``INT8RANGE`` — range of bigint."""

    def _default_sql(self) -> str:
        return "INT8RANGE"


class PostgresNumRangeType(DataType):
    """PostgreSQL ``NUMRANGE`` — range of numeric."""

    def _default_sql(self) -> str:
        return "NUMRANGE"


class PostgresTsRangeType(DataType):
    """PostgreSQL ``TSRANGE`` — range of timestamp without time zone."""

    def _default_sql(self) -> str:
        return "TSRANGE"


class PostgresTsTzRangeType(DataType):
    """PostgreSQL ``TSTZRANGE`` — range of timestamp with time zone."""

    def _default_sql(self) -> str:
        return "TSTZRANGE"


class PostgresDateRangeType(DataType):
    """PostgreSQL ``DATERANGE`` — range of date."""

    def _default_sql(self) -> str:
        return "DATERANGE"


# ---------------------------------------------------------------------------
# Multirange types (PG 14+)
# ---------------------------------------------------------------------------

class PostgresInt4MultirangeType(DataType):
    """PostgreSQL ``INT4MULTIRANGE`` — multirange of integer (PG 14+)."""

    def _default_sql(self) -> str:
        return "INT4MULTIRANGE"


class PostgresInt8MultirangeType(DataType):
    """PostgreSQL ``INT8MULTIRANGE`` — multirange of bigint (PG 14+)."""

    def _default_sql(self) -> str:
        return "INT8MULTIRANGE"


class PostgresNumMultirangeType(DataType):
    """PostgreSQL ``NUMMULTIRANGE`` — multirange of numeric (PG 14+)."""

    def _default_sql(self) -> str:
        return "NUMMULTIRANGE"


class PostgresTsMultirangeType(DataType):
    """PostgreSQL ``TSMULTIRANGE`` — multirange of timestamp (PG 14+)."""

    def _default_sql(self) -> str:
        return "TSMULTIRANGE"


class PostgresTsTzMultirangeType(DataType):
    """PostgreSQL ``TSTZMULTIRANGE`` — multirange of timestamptz (PG 14+)."""

    def _default_sql(self) -> str:
        return "TSTZMULTIRANGE"


class PostgresDateMultirangeType(DataType):
    """PostgreSQL ``DATEMULTIRANGE`` — multirange of date (PG 14+)."""

    def _default_sql(self) -> str:
        return "DATEMULTIRANGE"


# ---------------------------------------------------------------------------
# Object identifier types
# ---------------------------------------------------------------------------

class PostgresOIDType(DataType):
    """PostgreSQL ``OID`` — object identifier."""

    def _default_sql(self) -> str:
        return "OID"


class PostgresRegClassType(DataType):
    """PostgreSQL ``REGCLASS`` — relation name (OID alias)."""

    def _default_sql(self) -> str:
        return "REGCLASS"


class PostgresRegTypeType(DataType):
    """PostgreSQL ``REGTYPE`` — type name (OID alias)."""

    def _default_sql(self) -> str:
        return "REGTYPE"


class PostgresXIDType(DataType):
    """PostgreSQL ``XID`` — transaction ID."""

    def _default_sql(self) -> str:
        return "XID"


class PostgresXID8Type(DataType):
    """PostgreSQL ``XID8`` — 64-bit transaction ID (PG 13+)."""

    def _default_sql(self) -> str:
        return "XID8"


class PostgresCIDType(DataType):
    """PostgreSQL ``CID`` — command ID."""

    def _default_sql(self) -> str:
        return "CID"


class PostgresTIDType(DataType):
    """PostgreSQL ``TID`` — tuple ID (page, tuple)."""

    def _default_sql(self) -> str:
        return "TID"


# ---------------------------------------------------------------------------
# pg_lsn type
# ---------------------------------------------------------------------------

class PostgresPgLSNType(DataType):
    """PostgreSQL ``PG_LSN`` — WAL log sequence number."""

    def _default_sql(self) -> str:
        return "PG_LSN"


# ---------------------------------------------------------------------------
# Extension-provided types (minimal DataType wrappers)
# ---------------------------------------------------------------------------

class PostgresHstoreType(DataType):
    """PostgreSQL ``HSTORE`` — key-value store (hstore extension)."""

    def _default_sql(self) -> str:
        return "HSTORE"


class PostgresGeometryType(DataType):
    """PostGIS ``GEOMETRY`` — generic spatial geometry (PostGIS extension).

    For production use, subclass with SRID support as needed.
    """

    def _default_sql(self) -> str:
        return "GEOMETRY"


class PostgresGeographyType(DataType):
    """PostGIS ``GEOGRAPHY`` — geodetic spatial type (PostGIS extension)."""

    def _default_sql(self) -> str:
        return "GEOGRAPHY"


class PostgresVectorType(DataType):
    """pgvector ``VECTOR(n)`` — vector embedding (pgvector extension).

    Args:
        dim: Number of dimensions.
    """

    dim: int

    def __init__(self, dim: int):
        super().__init__()
        self.dim = dim

    def _type_params(self) -> Tuple:
        return (self.dim,)

    def _default_sql(self) -> str:
        return f"VECTOR({self.dim})"
