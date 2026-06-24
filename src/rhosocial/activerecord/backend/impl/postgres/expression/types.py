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


# ---------------------------------------------------------------------------
# Binary data: BYTEA
# ---------------------------------------------------------------------------

class PostgresByteaType(BlobType, backend="postgres"):
    """PostgreSQL ``BYTEA`` — variable-length binary string."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'BlobType'}


# ---------------------------------------------------------------------------
# Serial (auto-increment) types
# ---------------------------------------------------------------------------

class PostgresSmallSerialType(DataType, backend="postgres"):
    """PostgreSQL ``SMALLSERIAL`` — auto-incrementing SMALLINT (2 bytes)."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'SmallSerialType', 'SmallIntType'}


class PostgresSerialType(DataType, backend="postgres"):
    """PostgreSQL ``SERIAL`` — auto-incrementing INTEGER (4 bytes)."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'SerialType', 'IntegerType'}


class PostgresBigSerialType(DataType, backend="postgres"):
    """PostgreSQL ``BIGSERIAL`` — auto-incrementing BIGINT (8 bytes)."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'BigSerialType', 'BigIntType'}


# ---------------------------------------------------------------------------
# UUID
# ---------------------------------------------------------------------------

class PostgresUUIDType(DataType, backend="postgres"):
    """PostgreSQL ``UUID`` — universally unique identifier."""


# ---------------------------------------------------------------------------
# XML
# ---------------------------------------------------------------------------

class PostgresXMLType(DataType, backend="postgres"):
    """PostgreSQL ``XML`` — XML data type."""


# ---------------------------------------------------------------------------
# Text search
# ---------------------------------------------------------------------------

class PostgresTSVectorType(DataType, backend="postgres"):
    """PostgreSQL ``TSVECTOR`` — text search document."""


class PostgresTSQueryType(DataType, backend="postgres"):
    """PostgreSQL ``TSQUERY`` — text search query."""


# ---------------------------------------------------------------------------
# JSON path
# ---------------------------------------------------------------------------

class PostgresJsonPathType(DataType, backend="postgres"):
    """PostgreSQL ``JSONPATH`` — SQL/JSON path expression (PG 12+)."""


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


# ---------------------------------------------------------------------------
# Network address types
# ---------------------------------------------------------------------------

class PostgresInetType(DataType, backend="postgres"):
    """PostgreSQL ``INET`` — IPv4 or IPv6 address."""


class PostgresCidrType(DataType, backend="postgres"):
    """PostgreSQL ``CIDR`` — IPv4 or IPv6 network."""


class PostgresMacAddrType(DataType, backend="postgres"):
    """PostgreSQL ``MACADDR`` — MAC address (EUI-48)."""


class PostgresMacAddr8Type(DataType, backend="postgres"):
    """PostgreSQL ``MACADDR8`` — MAC address (EUI-64, PG 10+)."""


# ---------------------------------------------------------------------------
# Geometric types
# ---------------------------------------------------------------------------

class PostgresPointType(DataType, backend="postgres"):
    """PostgreSQL ``POINT`` — geometric point (x, y)."""


class PostgresLineType(DataType, backend="postgres"):
    """PostgreSQL ``LINE`` — infinite line."""


class PostgresLineSegmentType(DataType, backend="postgres"):
    """PostgreSQL ``LSEG`` — line segment."""


class PostgresBoxType(DataType, backend="postgres"):
    """PostgreSQL ``BOX`` — rectangular box."""


class PostgresPathType(DataType, backend="postgres"):
    """PostgreSQL ``PATH`` — open or closed geometric path."""


class PostgresPolygonType(DataType, backend="postgres"):
    """PostgreSQL ``POLYGON`` — closed geometric polygon."""


class PostgresCircleType(DataType, backend="postgres"):
    """PostgreSQL ``CIRCLE`` — circle (center + radius)."""


# ---------------------------------------------------------------------------
# Monetary type
# ---------------------------------------------------------------------------

class PostgresMoneyType(DataType, backend="postgres"):
    """PostgreSQL ``MONEY`` — currency amount."""


# ---------------------------------------------------------------------------
# Range types
# ---------------------------------------------------------------------------

class PostgresInt4RangeType(DataType, backend="postgres"):
    """PostgreSQL ``INT4RANGE`` — range of integer."""


class PostgresInt8RangeType(DataType, backend="postgres"):
    """PostgreSQL ``INT8RANGE`` — range of bigint."""


class PostgresNumRangeType(DataType, backend="postgres"):
    """PostgreSQL ``NUMRANGE`` — range of numeric."""


class PostgresTsRangeType(DataType, backend="postgres"):
    """PostgreSQL ``TSRANGE`` — range of timestamp without time zone."""


class PostgresTsTzRangeType(DataType, backend="postgres"):
    """PostgreSQL ``TSTZRANGE`` — range of timestamp with time zone."""


class PostgresDateRangeType(DataType, backend="postgres"):
    """PostgreSQL ``DATERANGE`` — range of date."""


# ---------------------------------------------------------------------------
# Multirange types (PG 14+)
# ---------------------------------------------------------------------------

class PostgresInt4MultirangeType(DataType, backend="postgres"):
    """PostgreSQL ``INT4MULTIRANGE`` — multirange of integer (PG 14+)."""


class PostgresInt8MultirangeType(DataType, backend="postgres"):
    """PostgreSQL ``INT8MULTIRANGE`` — multirange of bigint (PG 14+)."""


class PostgresNumMultirangeType(DataType, backend="postgres"):
    """PostgreSQL ``NUMMULTIRANGE`` — multirange of numeric (PG 14+)."""


class PostgresTsMultirangeType(DataType, backend="postgres"):
    """PostgreSQL ``TSMULTIRANGE`` — multirange of timestamp (PG 14+)."""


class PostgresTsTzMultirangeType(DataType, backend="postgres"):
    """PostgreSQL ``TSTZMULTIRANGE`` — multirange of timestamptz (PG 14+)."""


class PostgresDateMultirangeType(DataType, backend="postgres"):
    """PostgreSQL ``DATEMULTIRANGE`` — multirange of date (PG 14+)."""


# ---------------------------------------------------------------------------
# Object identifier types
# ---------------------------------------------------------------------------

class PostgresOIDType(DataType, backend="postgres"):
    """PostgreSQL ``OID`` — object identifier."""


class PostgresRegClassType(DataType, backend="postgres"):
    """PostgreSQL ``REGCLASS`` — relation name (OID alias)."""


class PostgresRegTypeType(DataType, backend="postgres"):
    """PostgreSQL ``REGTYPE`` — type name (OID alias)."""


class PostgresXIDType(DataType, backend="postgres"):
    """PostgreSQL ``XID`` — transaction ID."""


class PostgresXID8Type(DataType, backend="postgres"):
    """PostgreSQL ``XID8`` — 64-bit transaction ID (PG 13+)."""


class PostgresCIDType(DataType, backend="postgres"):
    """PostgreSQL ``CID`` — command ID."""


class PostgresTIDType(DataType, backend="postgres"):
    """PostgreSQL ``TID`` — tuple ID (page, tuple)."""


# ---------------------------------------------------------------------------
# pg_lsn type
# ---------------------------------------------------------------------------

class PostgresPgLSNType(DataType, backend="postgres"):
    """PostgreSQL ``PG_LSN`` — WAL log sequence number."""


# ---------------------------------------------------------------------------
# Extension-provided types (minimal DataType wrappers)
# ---------------------------------------------------------------------------

class PostgresHstoreType(DataType, backend="postgres"):
    """PostgreSQL ``HSTORE`` — key-value store (hstore extension)."""


class PostgresGeometryType(DataType, backend="postgres"):
    """PostGIS ``GEOMETRY`` — generic spatial geometry (PostGIS extension).

    For production use, subclass with SRID support as needed.
    """


class PostgresGeographyType(DataType, backend="postgres"):
    """PostGIS ``GEOGRAPHY`` — geodetic spatial type (PostGIS extension)."""


class PostgresCitextType(DataType, backend="postgres"):
    """PostgreSQL ``CITEXT`` — case-insensitive text (citext extension)."""


class PostgresCubeType(DataType, backend="postgres"):
    """PostgreSQL ``CUBE`` — multi-dimensional cube (cube extension)."""


class PostgresLtreeType(DataType, backend="postgres"):
    """PostgreSQL ``LTREE`` — label tree (ltree extension)."""


class PostgresRasterType(DataType, backend="postgres"):
    """PostgreSQL ``RASTER`` — raster (PostGIS raster extension)."""


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


# ---------------------------------------------------------------------------
# Array container
# ---------------------------------------------------------------------------

class PostgresArrayType(ArrayType):
    """PostgreSQL array type.

    PostgreSQL normalises all multi-dimensional array declarations to a
    single-dimensional internal representation at the storage level.
    Therefore ``integer[][]`` and ``integer[]`` are considered equivalent
    during schema comparison: ``is_equivalent`` intentionally ignores
    ``dimensions``.
    """

    def is_equivalent(self, other: "DataType") -> bool:
        if not isinstance(other, ArrayType):
            return False
        return self.element_type.is_equivalent(other.element_type)