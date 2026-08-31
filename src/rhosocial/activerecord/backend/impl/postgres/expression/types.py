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
    VarCharType,
)


# ---------------------------------------------------------------------------
# Character varying alias
# ---------------------------------------------------------------------------

class PostgresCharacterVaryingType(VarCharType):
    """PostgreSQL ``CHARACTER VARYING(n)`` — alias for ``VARCHAR(n)``."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'VarCharType'}


# ---------------------------------------------------------------------------
# Binary data: BYTEA
# ---------------------------------------------------------------------------

class PostgresByteaType(BlobType):
    """PostgreSQL ``BYTEA`` — variable-length binary string."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'BlobType'}


# ---------------------------------------------------------------------------
# Serial (auto-increment) types
# ---------------------------------------------------------------------------

class PostgresSmallSerialType(DataType):
    """PostgreSQL ``SMALLSERIAL`` — auto-incrementing SMALLINT (2 bytes)."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'SmallSerialType', 'SmallIntType'}


class PostgresSerialType(DataType):
    """PostgreSQL ``SERIAL`` — auto-incrementing INTEGER (4 bytes)."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'SerialType', 'IntegerType'}


class PostgresBigSerialType(DataType):
    """PostgreSQL ``BIGSERIAL`` — auto-incrementing BIGINT (8 bytes)."""

    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'BigSerialType', 'BigIntType'}


# ---------------------------------------------------------------------------
# UUID
# ---------------------------------------------------------------------------

class PostgresUUIDType(DataType):
    """PostgreSQL ``UUID`` — universally unique identifier."""


# ---------------------------------------------------------------------------
# XML
# ---------------------------------------------------------------------------

class PostgresXMLType(DataType):
    """PostgreSQL ``XML`` — XML data type."""


# ---------------------------------------------------------------------------
# Text search
# ---------------------------------------------------------------------------

class PostgresTSVectorType(DataType):
    """PostgreSQL ``TSVECTOR`` — text search document."""


class PostgresTSQueryType(DataType):
    """PostgreSQL ``TSQUERY`` — text search query."""


# ---------------------------------------------------------------------------
# JSON path
# ---------------------------------------------------------------------------

class PostgresJsonPathType(DataType):
    """PostgreSQL ``JSONPATH`` — SQL/JSON path expression (PG 12+)."""


# ---------------------------------------------------------------------------
# Bit string types
# ---------------------------------------------------------------------------

class PostgresBitType(DataType):
    """PostgreSQL ``BIT(n)`` — fixed-length bit string."""

    n: Optional[int] = None

    def __init__(self, dialect=None, *, n: Optional[int] = None):
        super().__init__(dialect)
        self.n = n

    def __eq__(self, other: object) -> bool:
        if type(self) is not type(other):
            return False
        return self.n == other.n

    def __hash__(self) -> int:
        return hash((type(self), self.n))


class PostgresVarBitType(DataType):
    """PostgreSQL ``VARBIT(n)`` — variable-length bit string."""

    n: Optional[int] = None

    def __init__(self, dialect=None, *, n: Optional[int] = None):
        super().__init__(dialect)
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

class PostgresInetType(DataType):
    """PostgreSQL ``INET`` — IPv4 or IPv6 address."""


class PostgresCidrType(DataType):
    """PostgreSQL ``CIDR`` — IPv4 or IPv6 network."""


class PostgresMacAddrType(DataType):
    """PostgreSQL ``MACADDR`` — MAC address (EUI-48)."""


class PostgresMacAddr8Type(DataType):
    """PostgreSQL ``MACADDR8`` — MAC address (EUI-64, PG 10+)."""


# ---------------------------------------------------------------------------
# Geometric types
# ---------------------------------------------------------------------------

class PostgresPointType(DataType):
    """PostgreSQL ``POINT`` — geometric point (x, y)."""


class PostgresLineType(DataType):
    """PostgreSQL ``LINE`` — infinite line."""


class PostgresLineSegmentType(DataType):
    """PostgreSQL ``LSEG`` — line segment."""


class PostgresBoxType(DataType):
    """PostgreSQL ``BOX`` — rectangular box."""


class PostgresPathType(DataType):
    """PostgreSQL ``PATH`` — open or closed geometric path."""


class PostgresPolygonType(DataType):
    """PostgreSQL ``POLYGON`` — closed geometric polygon."""


class PostgresCircleType(DataType):
    """PostgreSQL ``CIRCLE`` — circle (center + radius)."""


# ---------------------------------------------------------------------------
# Monetary type
# ---------------------------------------------------------------------------

class PostgresMoneyType(DataType):
    """PostgreSQL ``MONEY`` — currency amount."""


# ---------------------------------------------------------------------------
# Range types
# ---------------------------------------------------------------------------

class PostgresInt4RangeType(DataType):
    """PostgreSQL ``INT4RANGE`` — range of integer."""


class PostgresInt8RangeType(DataType):
    """PostgreSQL ``INT8RANGE`` — range of bigint."""


class PostgresNumRangeType(DataType):
    """PostgreSQL ``NUMRANGE`` — range of numeric."""


class PostgresTsRangeType(DataType):
    """PostgreSQL ``TSRANGE`` — range of timestamp without time zone."""


class PostgresTsTzRangeType(DataType):
    """PostgreSQL ``TSTZRANGE`` — range of timestamp with time zone."""


class PostgresDateRangeType(DataType):
    """PostgreSQL ``DATERANGE`` — range of date."""


# ---------------------------------------------------------------------------
# Multirange types (PG 14+)
# ---------------------------------------------------------------------------

class PostgresInt4MultirangeType(DataType):
    """PostgreSQL ``INT4MULTIRANGE`` — multirange of integer (PG 14+)."""


class PostgresInt8MultirangeType(DataType):
    """PostgreSQL ``INT8MULTIRANGE`` — multirange of bigint (PG 14+)."""


class PostgresNumMultirangeType(DataType):
    """PostgreSQL ``NUMMULTIRANGE`` — multirange of numeric (PG 14+)."""


class PostgresTsMultirangeType(DataType):
    """PostgreSQL ``TSMULTIRANGE`` — multirange of timestamp (PG 14+)."""


class PostgresTsTzMultirangeType(DataType):
    """PostgreSQL ``TSTZMULTIRANGE`` — multirange of timestamptz (PG 14+)."""


class PostgresDateMultirangeType(DataType):
    """PostgreSQL ``DATEMULTIRANGE`` — multirange of date (PG 14+)."""


# ---------------------------------------------------------------------------
# Object identifier types
# ---------------------------------------------------------------------------

class PostgresOIDType(DataType):
    """PostgreSQL ``OID`` — object identifier."""


class PostgresRegClassType(DataType):
    """PostgreSQL ``REGCLASS`` — relation name (OID alias)."""


class PostgresRegTypeType(DataType):
    """PostgreSQL ``REGTYPE`` — type name (OID alias)."""


class PostgresXIDType(DataType):
    """PostgreSQL ``XID`` — transaction ID."""


class PostgresXID8Type(DataType):
    """PostgreSQL ``XID8`` — 64-bit transaction ID (PG 13+)."""


class PostgresCIDType(DataType):
    """PostgreSQL ``CID`` — command ID."""


class PostgresTIDType(DataType):
    """PostgreSQL ``TID`` — tuple ID (page, tuple)."""


# ---------------------------------------------------------------------------
# pg_lsn type
# ---------------------------------------------------------------------------

class PostgresPgLSNType(DataType):
    """PostgreSQL ``PG_LSN`` — WAL log sequence number."""


# ---------------------------------------------------------------------------
# Extension-provided types (minimal DataType wrappers)
# ---------------------------------------------------------------------------

class PostgresHstoreType(DataType):
    """PostgreSQL ``HSTORE`` — key-value store (hstore extension)."""


class PostgresGeometryType(DataType):
    """PostGIS ``GEOMETRY`` — generic spatial geometry (PostGIS extension).

    For production use, subclass with SRID support as needed.
    """


class PostgresGeographyType(DataType):
    """PostGIS ``GEOGRAPHY`` — geodetic spatial type (PostGIS extension)."""


class PostgresCitextType(DataType):
    """PostgreSQL ``CITEXT`` — case-insensitive text (citext extension)."""


class PostgresCubeType(DataType):
    """PostgreSQL ``CUBE`` — multi-dimensional cube (cube extension)."""


class PostgresLtreeType(DataType):
    """PostgreSQL ``LTREE`` — label tree (ltree extension)."""


class PostgresRasterType(DataType):
    """PostgreSQL ``RASTER`` — raster (PostGIS raster extension)."""


class PostgresVectorType(DataType):
    """pgvector ``VECTOR(n)`` — vector embedding (pgvector extension).

    Args:
        dim: Number of dimensions.
    """

    dim: int

    def __init__(self, dialect=None, *, dim: int):
        super().__init__(dialect)
        self.dim = dim

    def __eq__(self, other: object) -> bool:
        if type(self) is not type(other):
            return False
        return self.dim == other.dim

    def __hash__(self) -> int:
        return hash((type(self), self.dim))


class PostgresHalfvecType(DataType):
    """pgvector ``HALFVEC(n)`` — half-precision vector (pgvector 0.5.0+).

    Stores each component as a half-precision float, halving memory compared
    to ``VECTOR``. Requires the pgvector extension.

    Args:
        dim: Number of dimensions.
    """

    dim: int

    def __init__(self, dialect=None, *, dim: int):
        super().__init__(dialect)
        self.dim = dim

    def __eq__(self, other: object) -> bool:
        if type(self) is not type(other):
            return False
        return self.dim == other.dim

    def __hash__(self) -> int:
        return hash((type(self), self.dim))


class PostgresSparsevecType(DataType):
    """pgvector ``SPARSEVEC(n)`` — sparse vector (pgvector 0.7.0+).

    Stores only non-zero components as ``{idx:value,...}/dim``, suitable for
    high-dimensional sparse embeddings. Requires the pgvector extension.

    Args:
        dim: Number of dimensions.
    """

    dim: int

    def __init__(self, dialect=None, *, dim: int):
        super().__init__(dialect)
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