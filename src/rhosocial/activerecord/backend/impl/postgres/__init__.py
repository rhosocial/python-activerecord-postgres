# src/rhosocial/activerecord/backend/impl/postgres/__init__.py
"""
PostgreSQL backend implementation for the Python ORM.

This module provides:
- PostgreSQL synchronous backend with connection management and query execution
- PostgreSQL asynchronous backend with async/await support
- PostgreSQL-specific connection configuration
- Type mapping and value conversion
- Transaction management with savepoint support (sync and async)
- PostgreSQL dialect and expression handling
- PostgreSQL-specific type definitions and mappings

Architecture:
- PostgreSQLBackend: Synchronous implementation using psycopg
- AsyncPostgreSQLBackend: Asynchronous implementation using psycopg
- Independent from ORM frameworks - uses only native drivers
"""

from .backend import PostgresBackend, AsyncPostgresBackend
from .config import PostgresConnectionConfig
from .dialect import PostgresDialect
from .transaction import PostgresTransactionManager, AsyncPostgresTransactionManager
from .types import PostgresEnumType
from .explain import PostgresExplainResult, PostgresExplainPlanLine
from .expression.ddl import (
    PostgresCreateEnumTypeExpression,
    PostgresDropEnumTypeExpression,
    PostgresAlterEnumTypeAddValueExpression,
    PostgresAlterEnumTypeRenameValueExpression,
    PostgresCreateRangeTypeExpression,
)
from .adapters import (
    PostgresEnumAdapter,
    PostgresRangeAdapter,
    PostgresMultirangeAdapter,
)
from .types.range import (
    PostgresRange,
    PostgresMultirange,
)
from .types.geometric import (
    Point,
    Line,
    LineSegment,
    Box,
    Path,
    Polygon,
    Circle,
)
from .adapters.geometric import PostgresGeometryAdapter
from .types.bit_string import PostgresBitString
from .adapters.bit_string import PostgresBitStringAdapter
from .types.monetary import PostgresMoney
from .adapters.monetary import PostgresMoneyAdapter
from .types.xml import PostgresXML
from .adapters.xml import PostgresXMLAdapter
from .types.network_address import PostgresMacaddr, PostgresMacaddr8
from .adapters.network_address import PostgresMacaddrAdapter, PostgresMacaddr8Adapter
from .adapters.uuid import PostgresUUIDAdapter
from .types.pgvector import PostgresVector
from .adapters.pgvector import PostgresVectorAdapter
from .types.postgis import PostgresGeometry
from .adapters.postgis import PostgresPostGISAdapter
from .types.hstore import PostgresHstore
from .adapters.hstore import PostgresHstoreAdapter
from .types.text_search import (
    PostgresTsVector,
    PostgresTsQuery,
)
from .functions.text_search import (
    to_tsvector,
    to_tsquery,
    plainto_tsquery,
    phraseto_tsquery,
    websearch_to_tsquery,
    ts_matches,
    ts_matches_expr,
    ts_rank,
    ts_rank_cd,
    ts_headline,
    tsvector_concat,
    tsvector_strip,
    tsvector_setweight,
    tsvector_length,
)
from .adapters.text_search import PostgresTsVectorAdapter, PostgresTsQueryAdapter
from .types.pg_lsn import PostgresLsn
from .adapters.pg_lsn import PostgresLsnAdapter
from .types.object_identifier import (
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
from .adapters.object_identifier import PostgresOidAdapter, PostgresXidAdapter, PostgresTidAdapter
from .types.json import PostgresJsonPath
from .adapters.json import PostgresJsonPathAdapter
from .types.enum import EnumTypeManager

# Import functions from functions module
from .functions import (
    # Range functions
    range_contains,
    range_contained_by,
    range_contains_range,
    range_overlaps,
    range_adjacent,
    range_strictly_left_of,
    range_strictly_right_of,
    range_not_extend_right,
    range_not_extend_left,
    range_union,
    range_intersection,
    range_difference,
    range_lower,
    range_upper,
    range_is_empty,
    range_lower_inc,
    range_upper_inc,
    range_lower_inf,
    range_upper_inf,
    # Geometry functions
    geometry_distance,
    geometry_contains,
    geometry_contained_by,
    geometry_overlaps,
    geometry_strictly_left,
    geometry_strictly_right,
    geometry_not_extend_right,
    geometry_not_extend_left,
    geometry_area,
    geometry_center,
    geometry_length,
    geometry_width,
    geometry_height,
    geometry_npoints,
    # Enum functions
    enum_range,
    enum_first,
    enum_last,
    enum_lt,
    enum_le,
    enum_gt,
    enum_ge,
    # Bit string functions
    bit_concat,
    bit_and,
    bit_or,
    bit_xor,
    bit_not,
    bit_shift_left,
    bit_shift_right,
    bit_length,
    bit_length_func,
    bit_octet_length,
    bit_get_bit,
    bit_set_bit,
    bit_count,
    # JSON functions
    json_path_root,
    json_path_key,
    json_path_index,
    json_path_wildcard,
    json_path_filter,
    jsonb_path_query,
    jsonb_path_query_first,
    jsonb_path_exists,
    jsonb_path_match,
    # pgvector functions
    vector_l2_distance,
    vector_cosine_distance,
    vector_inner_product,
    vector_cosine_similarity,
    vector_literal,
    # PostGIS functions - Construction
    st_make_point,
    st_geom_from_text,
    st_geog_from_text,
    st_set_srid,
    st_transform,
    # PostGIS functions - Predicates
    st_contains,
    st_intersects,
    st_within,
    st_dwithin,
    st_crosses,
    st_touches,
    st_overlaps,
    # PostGIS functions - Measurements
    st_distance,
    st_area,
    st_length,
    # PostGIS functions - Output
    st_as_geojson,
    st_as_text,
    # PostGIS functions - Operations
    st_buffer,
    st_envelope,
    st_centroid,
    # hstore functions - Constructors
    hstore_from_record,
    hstore_from_key_value,
    # hstore functions - Key/Value Extraction
    hstore_akeys,
    hstore_skeys,
    hstore_avals,
    hstore_svals,
    hstore_each,
    # hstore functions - Conversion
    hstore_to_array,
    hstore_to_matrix,
    hstore_to_json,
    hstore_to_jsonb,
    hstore_to_json_loose,
    hstore_to_jsonb_loose,
    # hstore functions - Subset
    hstore_slice,
    # hstore functions - Existence
    hstore_exist,
    hstore_defined,
    # hstore functions - Delete
    hstore_delete,
    hstore_delete_keys,
    hstore_delete_pairs,
    # hstore functions - Record
    hstore_populate_record,
    # hstore functions - Operators
    hstore_get_value,
    hstore_get_value_as_text,
    hstore_get_values,
    hstore_concat,
    hstore_key_exists,
    hstore_all_keys_exist,
    hstore_any_key_exists,
    hstore_contains,
    hstore_contained_by,
    hstore_subtract_key,
    hstore_subtract_keys,
    hstore_subtract_pairs,
    hstore_to_array_operator,
    hstore_to_matrix_operator,
    hstore_record_update,
    # hstore functions - Subscript
    hstore_subscript_get,
    hstore_subscript_set,
)


__all__ = [
    # Synchronous Backend
    "PostgresBackend",
    # Asynchronous Backend
    "AsyncPostgresBackend",
    # Configuration
    "PostgresConnectionConfig",
    # Dialect related
    "PostgresDialect",
    # Transaction - Sync and Async
    "PostgresTransactionManager",
    "AsyncPostgresTransactionManager",
    # PostgreSQL-specific Type Helpers
    "PostgresEnumType",
    "PostgresEnumAdapter",
    # PostgreSQL EXPLAIN Result Types
    "PostgresExplainResult",
    "PostgresExplainPlanLine",
    # PostgreSQL DDL Statements
    "PostgresCreateEnumTypeExpression",
    "PostgresDropEnumTypeExpression",
    "PostgresAlterEnumTypeAddValueExpression",
    "PostgresAlterEnumTypeRenameValueExpression",
    "PostgresCreateRangeTypeExpression",
    # Range Types
    "PostgresRange",
    "PostgresRangeAdapter",
    "PostgresMultirange",
    "PostgresMultirangeAdapter",
    "range_contains",
    "range_contained_by",
    "range_contains_range",
    "range_overlaps",
    "range_adjacent",
    "range_strictly_left_of",
    "range_strictly_right_of",
    "range_not_extend_right",
    "range_not_extend_left",
    "range_union",
    "range_intersection",
    "range_difference",
    "range_lower",
    "range_upper",
    "range_is_empty",
    "range_lower_inc",
    "range_upper_inc",
    "range_lower_inf",
    "range_upper_inf",
    # Geometry Types
    "Point",
    "Line",
    "LineSegment",
    "Box",
    "Path",
    "Polygon",
    "Circle",
    "PostgresGeometryAdapter",
    "geometry_distance",
    "geometry_contains",
    "geometry_contained_by",
    "geometry_overlaps",
    "geometry_strictly_left",
    "geometry_strictly_right",
    "geometry_not_extend_right",
    "geometry_not_extend_left",
    "geometry_area",
    "geometry_center",
    "geometry_length",
    "geometry_width",
    "geometry_height",
    "geometry_npoints",
    # Bit String Types
    "PostgresBitString",
    "PostgresBitStringAdapter",
    "bit_concat",
    "bit_and",
    "bit_or",
    "bit_xor",
    "bit_not",
    "bit_shift_left",
    "bit_shift_right",
    "bit_length",
    "bit_length_func",
    "bit_octet_length",
    "bit_get_bit",
    "bit_set_bit",
    "bit_count",
    # Enum Types
    "EnumTypeManager",
    "enum_range",
    "enum_first",
    "enum_last",
    "enum_lt",
    "enum_le",
    "enum_gt",
    "enum_ge",
    # Money Type
    "PostgresMoney",
    "PostgresMoneyAdapter",
    # XML Type
    "PostgresXML",
    "PostgresXMLAdapter",
    # MACADDR Types
    "PostgresMacaddr",
    "PostgresMacaddrAdapter",
    "PostgresMacaddr8",
    "PostgresMacaddr8Adapter",
    # Text Search Types
    "PostgresTsVector",
    "PostgresTsVectorAdapter",
    "PostgresTsQuery",
    "PostgresTsQueryAdapter",
    "to_tsvector",
    "to_tsquery",
    "plainto_tsquery",
    "phraseto_tsquery",
    "websearch_to_tsquery",
    "ts_matches",
    "ts_matches_expr",
    "ts_rank",
    "ts_rank_cd",
    "ts_headline",
    "tsvector_concat",
    "tsvector_strip",
    "tsvector_setweight",
    "tsvector_length",
    # LSN Type
    "PostgresLsn",
    "PostgresLsnAdapter",
    # OID Types
    "OID",
    "RegClass",
    "RegType",
    "RegProc",
    "RegProcedure",
    "RegOper",
    "RegOperator",
    "RegConfig",
    "RegDictionary",
    "RegNamespace",
    "RegRole",
    "RegCollation",
    "XID",
    "XID8",
    "CID",
    "TID",
    "PostgresOidAdapter",
    "PostgresXidAdapter",
    "PostgresTidAdapter",
    # JSON Path Type
    "PostgresJsonPath",
    "PostgresJsonPathAdapter",
    "json_path_root",
    "json_path_key",
    "json_path_index",
    "json_path_wildcard",
    "json_path_filter",
    "jsonb_path_query",
    "jsonb_path_query_first",
    "jsonb_path_exists",
    "jsonb_path_match",
    # UUID Type
    "PostgresUUIDAdapter",
    "gen_random_uuid",
    "uuid_default_generator",
    # pgvector Types
    "PostgresVector",
    "PostgresVectorAdapter",
    "vector_l2_distance",
    "vector_cosine_distance",
    "vector_inner_product",
    "vector_cosine_similarity",
    "vector_literal",
    # PostGIS Types
    "PostgresGeometry",
    "PostgresPostGISAdapter",
    # hstore Types
    "PostgresHstore",
    "PostgresHstoreAdapter",
    "st_make_point",
    "st_geom_from_text",
    "st_geog_from_text",
    "st_set_srid",
    "st_transform",
    "st_contains",
    "st_intersects",
    "st_within",
    "st_dwithin",
    "st_crosses",
    "st_touches",
    "st_overlaps",
    "st_distance",
    "st_area",
    "st_length",
    "st_as_geojson",
    "st_as_text",
    "st_buffer",
    "st_envelope",
    "st_centroid",
    # hstore functions - Constructors
    "hstore_from_record",
    "hstore_from_key_value",
    # hstore functions - Key/Value Extraction
    "hstore_akeys",
    "hstore_skeys",
    "hstore_avals",
    "hstore_svals",
    "hstore_each",
    # hstore functions - Conversion
    "hstore_to_array",
    "hstore_to_matrix",
    "hstore_to_json",
    "hstore_to_jsonb",
    "hstore_to_json_loose",
    "hstore_to_jsonb_loose",
    # hstore functions - Subset
    "hstore_slice",
    # hstore functions - Existence
    "hstore_exist",
    "hstore_defined",
    # hstore functions - Delete
    "hstore_delete",
    "hstore_delete_keys",
    "hstore_delete_pairs",
    # hstore functions - Record
    "hstore_populate_record",
    # hstore functions - Operators
    "hstore_get_value",
    "hstore_get_value_as_text",
    "hstore_get_values",
    "hstore_concat",
    "hstore_key_exists",
    "hstore_all_keys_exist",
    "hstore_any_key_exists",
    "hstore_contains",
    "hstore_contained_by",
    "hstore_subtract_key",
    "hstore_subtract_keys",
    "hstore_subtract_pairs",
    "hstore_to_array_operator",
    "hstore_to_matrix_operator",
    "hstore_record_update",
    # hstore functions - Subscript
    "hstore_subscript_get",
    "hstore_subscript_set",
]
