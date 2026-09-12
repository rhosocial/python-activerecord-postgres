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

from typing import Any, Dict, Optional, Set, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.types import (
    ArrayType,
    BlobType,
    DataType,
    VarCharType,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


# ---------------------------------------------------------------------------
# Character varying alias
# ---------------------------------------------------------------------------

class PostgresCharacterVaryingType(VarCharType):
    """PostgreSQL ``CHARACTER VARYING(n)`` — alias for ``VARCHAR(n)``."""

    name = "postgres_character_varying"
    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'VarCharType'}


# ---------------------------------------------------------------------------
# Binary data: BYTEA
# ---------------------------------------------------------------------------

class PostgresByteaType(BlobType):
    """PostgreSQL ``BYTEA`` — variable-length binary string."""

    name = "postgres_bytea"
    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'BlobType'}


# ---------------------------------------------------------------------------
# Serial (auto-increment) types
# ---------------------------------------------------------------------------

class PostgresSmallSerialType(DataType):
    """PostgreSQL ``SMALLSERIAL`` — auto-incrementing SMALLINT (2 bytes)."""

    name = "postgres_smallserial"
    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'SmallSerialType', 'SmallIntType'}


class PostgresSerialType(DataType):
    """PostgreSQL ``SERIAL`` — auto-incrementing INTEGER (4 bytes)."""

    name = "postgres_serial"
    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'SerialType', 'IntegerType'}


class PostgresBigSerialType(DataType):
    """PostgreSQL ``BIGSERIAL`` — auto-incrementing BIGINT (8 bytes)."""

    name = "postgres_bigserial"
    @classmethod
    def synonyms(cls) -> Set[str]:
        return {'BigSerialType', 'BigIntType'}


# ---------------------------------------------------------------------------
# UUID
# ---------------------------------------------------------------------------

class PostgresUUIDType(DataType):
    """PostgreSQL ``UUID`` — universally unique identifier."""

    name = "postgres_uuid"
# ---------------------------------------------------------------------------
# XML
# ---------------------------------------------------------------------------

class PostgresXMLType(DataType):
    """PostgreSQL ``XML`` — XML data type."""

    name = "postgres_xml"
# ---------------------------------------------------------------------------
# Text search
# ---------------------------------------------------------------------------

class PostgresTSVectorType(DataType):
    """PostgreSQL ``TSVECTOR`` — text search document."""

    name = "postgres_tsvector"
class PostgresTSQueryType(DataType):
    """PostgreSQL ``TSQUERY`` — text search query."""

    name = "postgres_tsquery"
# ---------------------------------------------------------------------------
# JSON path
# ---------------------------------------------------------------------------

class PostgresJsonPathType(DataType):
    """PostgreSQL ``JSONPATH`` — SQL/JSON path expression (PG 12+)."""

    name = "postgres_jsonpath"
# ---------------------------------------------------------------------------
# Bit string types
# ---------------------------------------------------------------------------

class PostgresBitType(DataType):
    """PostgreSQL ``BIT(n)`` — fixed-length bit string."""

    name = "postgres_bit"
    n: Optional[int] = None

    def __init__(self, n: Optional[int] = None, dialect: Optional["SQLDialectBase"] = None,
                 dialect_options: Optional[Dict[str, Any]] = None):
        super().__init__(dialect, dialect_options=dialect_options)
        self.n = n

    def _type_params(self) -> tuple:
        return (self.n,)


class PostgresVarBitType(DataType):
    """PostgreSQL ``VARBIT(n)`` — variable-length bit string."""

    name = "postgres_varbit"
    n: Optional[int] = None

    def __init__(self, n: Optional[int] = None, dialect: Optional["SQLDialectBase"] = None,
                 dialect_options: Optional[Dict[str, Any]] = None):
        super().__init__(dialect, dialect_options=dialect_options)
        self.n = n

    def _type_params(self) -> tuple:
        return (self.n,)


# ---------------------------------------------------------------------------
# Network address types
# ---------------------------------------------------------------------------

class PostgresInetType(DataType):
    """PostgreSQL ``INET`` — IPv4 or IPv6 address."""

    name = "postgres_inet"
class PostgresCidrType(DataType):
    """PostgreSQL ``CIDR`` — IPv4 or IPv6 network."""

    name = "postgres_cidr"
class PostgresMacAddrType(DataType):
    """PostgreSQL ``MACADDR`` — MAC address (EUI-48)."""

    name = "postgres_macaddr"
class PostgresMacAddr8Type(DataType):
    """PostgreSQL ``MACADDR8`` — MAC address (EUI-64, PG 10+)."""

    name = "postgres_macaddr8"
# ---------------------------------------------------------------------------
# Geometric types
# ---------------------------------------------------------------------------

class PostgresPointType(DataType):
    """PostgreSQL ``POINT`` — geometric point (x, y)."""

    name = "postgres_point"
class PostgresLineType(DataType):
    """PostgreSQL ``LINE`` — infinite line."""

    name = "postgres_line"
class PostgresLineSegmentType(DataType):
    """PostgreSQL ``LSEG`` — line segment."""

    name = "postgres_line_segment"
class PostgresBoxType(DataType):
    """PostgreSQL ``BOX`` — rectangular box."""

    name = "postgres_box"
class PostgresPathType(DataType):
    """PostgreSQL ``PATH`` — open or closed geometric path."""

    name = "postgres_path"
class PostgresPolygonType(DataType):
    """PostgreSQL ``POLYGON`` — closed geometric polygon."""

    name = "postgres_polygon"
class PostgresCircleType(DataType):
    """PostgreSQL ``CIRCLE`` — circle (center + radius)."""

    name = "postgres_circle"
# ---------------------------------------------------------------------------
# Monetary type
# ---------------------------------------------------------------------------

class PostgresMoneyType(DataType):
    """PostgreSQL ``MONEY`` — currency amount."""

    name = "postgres_money"
# ---------------------------------------------------------------------------
# Range types
# ---------------------------------------------------------------------------

class PostgresInt4RangeType(DataType):
    """PostgreSQL ``INT4RANGE`` — range of integer."""

    name = "postgres_int4range"
class PostgresInt8RangeType(DataType):
    """PostgreSQL ``INT8RANGE`` — range of bigint."""

    name = "postgres_int8range"
class PostgresNumRangeType(DataType):
    """PostgreSQL ``NUMRANGE`` — range of numeric."""

    name = "postgres_numrange"
class PostgresTsRangeType(DataType):
    """PostgreSQL ``TSRANGE`` — range of timestamp without time zone."""

    name = "postgres_tsrange"
class PostgresTsTzRangeType(DataType):
    """PostgreSQL ``TSTZRANGE`` — range of timestamp with time zone."""

    name = "postgres_tstzrange"
class PostgresDateRangeType(DataType):
    """PostgreSQL ``DATERANGE`` — range of date."""

    name = "postgres_daterange"
# ---------------------------------------------------------------------------
# Multirange types (PG 14+)
# ---------------------------------------------------------------------------

class PostgresInt4MultirangeType(DataType):
    """PostgreSQL ``INT4MULTIRANGE`` — multirange of integer (PG 14+)."""

    name = "postgres_int4multirange"
class PostgresInt8MultirangeType(DataType):
    """PostgreSQL ``INT8MULTIRANGE`` — multirange of bigint (PG 14+)."""

    name = "postgres_int8multirange"
class PostgresNumMultirangeType(DataType):
    """PostgreSQL ``NUMMULTIRANGE`` — multirange of numeric (PG 14+)."""

    name = "postgres_nummultirange"
class PostgresTsMultirangeType(DataType):
    """PostgreSQL ``TSMULTIRANGE`` — multirange of timestamp (PG 14+)."""

    name = "postgres_tsmultirange"
class PostgresTsTzMultirangeType(DataType):
    """PostgreSQL ``TSTZMULTIRANGE`` — multirange of timestamptz (PG 14+)."""

    name = "postgres_tstzmultirange"
class PostgresDateMultirangeType(DataType):
    """PostgreSQL ``DATEMULTIRANGE`` — multirange of date (PG 14+)."""

    name = "postgres_datemultirange"
# ---------------------------------------------------------------------------
# Object identifier types
# ---------------------------------------------------------------------------

class PostgresOIDType(DataType):
    """PostgreSQL ``OID`` — object identifier."""

    name = "postgres_oid"
class PostgresRegClassType(DataType):
    """PostgreSQL ``REGCLASS`` — relation name (OID alias)."""

    name = "postgres_regclass"
class PostgresRegTypeType(DataType):
    """PostgreSQL ``REGTYPE`` — type name (OID alias)."""

    name = "postgres_regtype"
class PostgresXIDType(DataType):
    """PostgreSQL ``XID`` — transaction ID."""

    name = "postgres_xid"
class PostgresXID8Type(DataType):
    """PostgreSQL ``XID8`` — 64-bit transaction ID (PG 13+)."""

    name = "postgres_xid8"
class PostgresCIDType(DataType):
    """PostgreSQL ``CID`` — command ID."""

    name = "postgres_cid"
class PostgresTIDType(DataType):
    """PostgreSQL ``TID`` — tuple ID (page, tuple)."""

    name = "postgres_tid"
# ---------------------------------------------------------------------------
# pg_lsn type
# ---------------------------------------------------------------------------

class PostgresPgLSNType(DataType):
    """PostgreSQL ``PG_LSN`` — WAL log sequence number."""

    name = "postgres_pg_lsn"
# ---------------------------------------------------------------------------
# Extension-provided types (minimal DataType wrappers)
# ---------------------------------------------------------------------------

class PostgresHstoreType(DataType):
    """PostgreSQL ``HSTORE`` — key-value store (hstore extension)."""

    name = "postgres_hstore"
class PostgresGeometryType(DataType):
    """PostGIS ``GEOMETRY`` — generic spatial geometry (PostGIS extension).

    For production use, subclass with SRID support as needed.
    """

    name = "postgres_geometry"
class PostgresGeographyType(DataType):
    """PostGIS ``GEOGRAPHY`` — geodetic spatial type (PostGIS extension)."""

    name = "postgres_geography"
class PostgresCitextType(DataType):
    """PostgreSQL ``CITEXT`` — case-insensitive text (citext extension)."""

    name = "postgres_citext"
class PostgresCubeType(DataType):
    """PostgreSQL ``CUBE`` — multi-dimensional cube (cube extension)."""

    name = "postgres_cube"
class PostgresLtreeType(DataType):
    """PostgreSQL ``LTREE`` — label tree (ltree extension)."""

    name = "postgres_ltree"
class PostgresRasterType(DataType):
    """PostgreSQL ``RASTER`` — raster (PostGIS raster extension)."""

    name = "postgres_raster"
class PostgresVectorType(DataType):
    """pgvector ``VECTOR(n)`` — vector embedding (pgvector extension).

    Args:
        dim: Number of dimensions.
    """

    name = "postgres_vector"
    dim: int

    def __init__(self, dim: int, dialect: Optional["SQLDialectBase"] = None,
                 dialect_options: Optional[Dict[str, Any]] = None):
        super().__init__(dialect, dialect_options=dialect_options)
        self.dim = dim

    def _type_params(self) -> tuple:
        return (self.dim,)


class PostgresHalfvecType(DataType):
    """pgvector ``HALFVEC(n)`` — half-precision vector (pgvector 0.5.0+).

    Stores each component as a half-precision float, halving memory compared
    to ``VECTOR``. Requires the pgvector extension.

    Args:
        dim: Number of dimensions.
    """

    name = "postgres_halfvec"
    dim: int

    def __init__(self, dim: int, dialect: Optional["SQLDialectBase"] = None,
                 dialect_options: Optional[Dict[str, Any]] = None):
        super().__init__(dialect, dialect_options=dialect_options)
        self.dim = dim

    def _type_params(self) -> tuple:
        return (self.dim,)


class PostgresSparsevecType(DataType):
    """pgvector ``SPARSEVEC(n)`` — sparse vector (pgvector 0.7.0+).

    Stores only non-zero components as ``{idx:value,...}/dim``, suitable for
    high-dimensional sparse embeddings. Requires the pgvector extension.

    Args:
        dim: Number of dimensions.
    """

    name = "postgres_sparsevec"
    dim: int

    def __init__(self, dim: int, dialect: Optional["SQLDialectBase"] = None,
                 dialect_options: Optional[Dict[str, Any]] = None):
        super().__init__(dialect, dialect_options=dialect_options)
        self.dim = dim

    def _type_params(self) -> tuple:
        return (self.dim,)


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

    name = "postgres_array"
    def is_equivalent(self, other: "DataType") -> bool:
        if not isinstance(other, ArrayType):
            return False
        return self.element_type.is_equivalent(other.element_type)