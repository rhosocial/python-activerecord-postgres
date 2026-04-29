# src/rhosocial/activerecord/backend/impl/postgres/functions/__init__.py
"""
PostgreSQL type-specific functions and operators.

This module provides SQL expression generators for PostgreSQL-specific
functions and operators for various data types.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions.html

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
- They do not concatenate SQL strings directly

Function modules and their corresponding PostgreSQL documentation:
- range: Range functions and operators - https://www.postgresql.org/docs/current/functions-range.html
- geometric: Geometric functions and operators - https://www.postgresql.org/docs/current/functions-geometry.html
- enum: Enum functions - https://www.postgresql.org/docs/current/functions-enum.html
- bit_string: Bit string functions and operators - https://www.postgresql.org/docs/current/functions-bitstring.html
- json: JSON functions and operators - https://www.postgresql.org/docs/current/functions-json.html
- text_search: Full-text search functions - https://www.postgresql.org/docs/current/functions-textsearch.html
- xml: XML functions - https://www.postgresql.org/docs/current/functions-xml.html
- math_enhanced: Enhanced math functions - https://www.postgresql.org/docs/current/functions-math.html
- pgcrypto: Cryptographic functions - https://www.postgresql.org/docs/current/pgcrypto.html
- ltree: Label tree functions - https://www.postgresql.org/docs/current/ltree.html
- pg_trgm: Trigram similarity functions - https://www.postgresql.org/docs/current/pgtrgm.html
- fuzzystrmatch: Fuzzy string matching functions - https://www.postgresql.org/docs/current/fuzzystrmatch.html
- earthdistance: Earth distance functions - https://www.postgresql.org/docs/current/earthdistance.html
- hypopg: Hypothetical index functions - https://hypopg.readthedocs.io/
- pg_surgery: Heap surgery functions - https://www.postgresql.org/docs/current/pgsurgery.html
- pg_walinspect: WAL inspection functions - https://www.postgresql.org/docs/current/pgwalinspect.html
- address_standardizer: Address standardization functions - https://postgis.net/docs/Address_Standardizer.html
- pg_partman: Partition management functions - https://github.com/pgpartman/pg_partman
- pg_repack: Table reorganization functions - https://reorg.github.io/pg_repack/
- orafce: Oracle compatibility functions - https://github.com/orafce/orafce
- intarray: Integer array functions and operators - https://www.postgresql.org/docs/current/intarray.html
- cube: Multidimensional cube functions and operators - https://www.postgresql.org/docs/current/cube.html
- citext: Case-insensitive text functions - https://www.postgresql.org/docs/current/citext.html
- pg_stat_statements: Query statistics functions - https://www.postgresql.org/docs/current/pgstatstatements.html
- postgis_raster: PostGIS raster functions - https://postgis.net/docs/RT_reference.html
- pglogical: Logical replication functions - https://github.com/2ndQuadrant/pglogical
- pgrouting: Geospatial routing functions - https://docs.pgrouting.org/latest/en/
- tablefunc: Pivot table and tree traversal functions - https://www.postgresql.org/docs/current/tablefunc.html
- pg_cron: Cron-based job scheduler - https://github.com/citusdata/pg_cron
- pgaudit: Audit logging configuration - https://github.com/pgaudit/pgaudit
"""

# Range functions
from .range import (
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
    multirange_contains,
    multirange_is_contained_by,
    multirange_overlaps,
    multirange_union,
    multirange_intersection,
    multirange_difference,
    range_merge,
    multirange_literal,
    multirange_constructor,
    int4range,
    int8range,
    numrange,
    tsrange,
    tstzrange,
    daterange,
)

# Geometric functions
from .geometric import (
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
)

# Enum functions
from .enum import (
    enum_range,
    enum_first,
    enum_last,
    enum_lt,
    enum_le,
    enum_gt,
    enum_ge,
)

# Bit string functions
from .bit_string import (
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
)

# JSON functions
from .json import (
    json_path_root,
    json_path_key,
    json_path_index,
    json_path_wildcard,
    json_path_filter,
    jsonb_path_query,
    jsonb_path_query_first,
    jsonb_path_exists,
    jsonb_path_match,
)

# Text search functions
from .text_search import (
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

# XML functions
from .xml import (
    xmlparse,
    xpath_query,
    xpath_exists,
    xml_is_well_formed,
)

# Math enhanced functions
from .math_enhanced import (
    round_,
    pow,
    power,
    sqrt,
    mod,
    ceil,
    floor,
    trunc,
    max_,
    min_,
    avg,
)

# Array functions
from .array import (
    array_agg,
    array_append,
    array_cat,
    array_dims,
    array_fill,
    array_length,
    array_lower,
    array_ndims,
    array_position,
    array_positions,
    array_prepend,
    array_remove,
    array_replace,
    array_to_string,
    array_upper,
    unnest,
    array_agg_distinct,
    string_to_array,
)

# Network address functions
from .network import (
    inet_client_addr,
    inet_client_port,
    inet_server_addr,
    inet_server_port,
    inet_merge,
    inet_and,
    inet_or,
    inetnot,
    inet_set_mask,
    inet_masklen,
    inet_netmask,
    inet_network,
    inet_recv,
    inet_show,
    cidr_netmask,
    macaddr8_set7bit,
)

# UUID functions
from .uuid import (
    uuid_generate_v1,
    uuid_generate_v1mc,
    uuid_generate_v3,
    uuid_generate_v4,
    uuid_generate_v5,
    uuid_ns_dns,
    uuid_ns_url,
    uuid_ns_oid,
    uuid_ns_x500,
    uuid_nil,
    uuid_max,
    gen_random_uuid,
    uuid_default_generator,
)

# hstore functions
from .hstore import (
    hstore_from_record,
    hstore_from_key_value,
    hstore_akeys,
    hstore_skeys,
    hstore_avals,
    hstore_svals,
    hstore_each,
    hstore_to_array,
    hstore_to_matrix,
    hstore_to_json,
    hstore_to_jsonb,
    hstore_to_json_loose,
    hstore_to_jsonb_loose,
    hstore_slice,
    hstore_exist,
    hstore_defined,
    hstore_delete,
    hstore_delete_keys,
    hstore_delete_pairs,
    hstore_populate_record,
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
    hstore_subscript_get,
    hstore_subscript_set,
)

# pgvector functions
from .pgvector import (
    vector_l2_distance,
    vector_cosine_distance,
    vector_inner_product,
    vector_cosine_similarity,
    vector_literal,
)

# PostGIS functions
from .postgis import (
    st_make_point,
    st_geom_from_text,
    st_geog_from_text,
    st_set_srid,
    st_transform,
    st_contains,
    st_intersects,
    st_within,
    st_dwithin,
    st_crosses,
    st_touches,
    st_overlaps,
    st_distance,
    st_area,
    st_length,
    st_as_geojson,
    st_as_text,
    st_buffer,
    st_envelope,
    st_centroid,
)

# pgcrypto functions
from .pgcrypto import (
    gen_salt,
    crypt,
    encrypt,
    decrypt,
    gen_random_bytes,
    hmac,
    digest,
    pgp_sym_encrypt,
    pgp_sym_decrypt,
)

# ltree functions
from .ltree import (
    ltree_literal,
    lquery_literal,
    ltxtquery_literal,
    ltree_ancestor,
    ltree_descendant,
    ltree_matches,
    ltree_text_search,
    ltree_concat,
    ltree_nlevel,
    ltree_subpath,
    ltree_lca,
)

# pg_trgm functions
from .pg_trgm import (
    similarity,
    word_similarity,
    show_trgm,
    similarity_operator,
)

# fuzzystrmatch functions
from .fuzzystrmatch import (
    levenshtein,
    levenshtein_less_equal,
    soundex,
    difference,
    metaphone,
    dmetaphone,
    dmetaphone_alt,
)

# earthdistance functions
from .earthdistance import (
    ll_to_earth,
    earth_distance,
    earth_box,
    earthdistance_operator,
    point_inside_circle,
)

# intarray functions
from .intarray import (
    intarray_contains,
    intarray_contained_by,
    intarray_overlaps,
    intarray_idx,
    intarray_subarray,
    intarray_uniq,
    intarray_sort,
    intarray_operator,
)

# cube functions
from .cube import (
    cube_literal,
    cube_dimension,
    cube_size,
    cube_union,
    cube_inter,
    cube_contains,
    cube_distance,
)

# citext functions
from .citext import (
    citext_literal,
)

# pg_stat_statements functions
from .pg_stat_statements import (
    pg_stat_statements_reset,
)

# postgis_raster functions
from .postgis_raster import (
    st_rast_from_hexwkb,
    st_value,
    st_summary,
)

# pglogical functions
from .pglogical import (
    pglogical_create_node,
    pglogical_create_publication,
    pglogical_create_subscription,
    pglogical_show_subscription_status,
    pglogical_alter_subscription_synchronize,
)

# pgrouting functions
from .pgrouting import (
    pgr_dijkstra,
    pgr_astar,
)

# tablefunc functions
from .tablefunc import (
    crosstab,
    connectby,
    normal_rand,
)

# pg_cron functions
from .pg_cron import (
    cron_schedule,
    cron_unschedule,
    cron_run,
)

# pgaudit functions
from .pgaudit import (
    pgaudit_set_role,
    pgaudit_log_level,
    pgaudit_include_catalog,
)

# hypopg functions
from .hypopg import (
    hypopg_create_index,
    hypopg_reset,
    hypopg_show_indexes,
    hypopg_estimate_size,
)

# pg_surgery functions
from .pg_surgery import (
    pg_surgery_heap_freeze,
    pg_surgery_heap_page_header,
)

# pg_walinspect functions
from .pg_walinspect import (
    pg_get_wal_records_info,
    pg_get_wal_blocks_info,
    pg_logical_emit_message,
)

# address_standardizer functions
from .address_standardizer import (
    standardize_address,
    parse_address,
)

# pg_partman functions
from .pg_partman import (
    create_time_partition,
    create_id_partition,
    run_maintenance,
)

# pg_repack functions
from .pg_repack import (
    repack_table,
    repack_index,
    move_tablespace,
)

# orafce functions
from .orafce import (
    add_months,
    last_day,
    months_between,
    next_day,
    nvl,
    nvl2,
    decode,
    trunc,
    round,
    instr,
    substr,
)

# data_type functions
from .data_type import (
    xid8_literal,
    array_literal,
)

__all__ = [
    # Range functions
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
    # Multirange operators
    "multirange_contains",
    "multirange_is_contained_by",
    "multirange_overlaps",
    "multirange_union",
    "multirange_intersection",
    "multirange_difference",
    # Multirange functions
    "range_merge",
    "multirange_literal",
    "multirange_constructor",
    # Geometric functions
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
    # Enum functions
    "enum_range",
    "enum_first",
    "enum_last",
    "enum_lt",
    "enum_le",
    "enum_gt",
    "enum_ge",
    # Bit string functions
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
    # JSON functions
    "json_path_root",
    "json_path_key",
    "json_path_index",
    "json_path_wildcard",
    "json_path_filter",
    "jsonb_path_query",
    "jsonb_path_query_first",
    "jsonb_path_exists",
    "jsonb_path_match",
    # Text search functions
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
    # XML functions
    "xmlparse",
    "xpath_query",
    "xpath_exists",
    "xml_is_well_formed",
    # Math enhanced functions
    "round_",
    "pow",
    "power",
    "sqrt",
    "mod",
    "ceil",
    "floor",
    "trunc",
    "max_",
    "min_",
    "avg",
    # Array functions
    "array_agg",
    "array_append",
    "array_cat",
    "array_dims",
    "array_fill",
    "array_length",
    "array_lower",
    "array_ndims",
    "array_position",
    "array_positions",
    "array_prepend",
    "array_remove",
    "array_replace",
    "array_to_string",
    "array_upper",
    "unnest",
    "array_agg_distinct",
    "string_to_array",
    # Network address functions
    "inet_client_addr",
    "inet_client_port",
    "inet_server_addr",
    "inet_server_port",
    "inet_merge",
    "inet_and",
    "inet_or",
    "inetnot",
    "inet_set_mask",
    "inet_masklen",
    "inet_netmask",
    "inet_network",
    "inet_recv",
    "inet_show",
    "cidr_netmask",
    "macaddr8_set7bit",
    # UUID functions
    "uuid_generate_v1",
    "uuid_generate_v1mc",
    "uuid_generate_v3",
    "uuid_generate_v4",
    "uuid_generate_v5",
    "uuid_ns_dns",
    "uuid_ns_url",
    "uuid_ns_oid",
    "uuid_ns_x500",
    "uuid_nil",
    "uuid_max",
    "gen_random_uuid",
    "uuid_default_generator",
    # hstore functions
    "hstore_from_record",
    "hstore_from_key_value",
    "hstore_akeys",
    "hstore_skeys",
    "hstore_avals",
    "hstore_svals",
    "hstore_each",
    "hstore_to_array",
    "hstore_to_matrix",
    "hstore_to_json",
    "hstore_to_jsonb",
    "hstore_to_json_loose",
    "hstore_to_jsonb_loose",
    "hstore_slice",
    "hstore_exist",
    "hstore_defined",
    "hstore_delete",
    "hstore_delete_keys",
    "hstore_delete_pairs",
    "hstore_populate_record",
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
    "hstore_subscript_get",
    "hstore_subscript_set",
    # Range constructors
    "int4range",
    "int8range",
    "numrange",
    "tsrange",
    "tstzrange",
    "daterange",
    # pgvector functions
    "vector_l2_distance",
    "vector_cosine_distance",
    "vector_inner_product",
    "vector_cosine_similarity",
    "vector_literal",
    # PostGIS functions - Construction
    "st_make_point",
    "st_geom_from_text",
    "st_geog_from_text",
    "st_set_srid",
    "st_transform",
    # PostGIS functions - Predicates
    "st_contains",
    "st_intersects",
    "st_within",
    "st_dwithin",
    "st_crosses",
    "st_touches",
    "st_overlaps",
    # PostGIS functions - Measurements
    "st_distance",
    "st_area",
    "st_length",
    # PostGIS functions - Output
    "st_as_geojson",
    "st_as_text",
    # PostGIS functions - Operations
    "st_buffer",
    "st_envelope",
    "st_centroid",
    # pgcrypto functions
    "gen_salt",
    "crypt",
    "encrypt",
    "decrypt",
    "gen_random_bytes",
    "hmac",
    "digest",
    "pgp_sym_encrypt",
    "pgp_sym_decrypt",
    # ltree functions
    "ltree_literal",
    "lquery_literal",
    "ltxtquery_literal",
    "ltree_ancestor",
    "ltree_descendant",
    "ltree_matches",
    "ltree_text_search",
    "ltree_concat",
    "ltree_nlevel",
    "ltree_subpath",
    "ltree_lca",
    # pg_trgm functions
    "similarity",
    "word_similarity",
    "show_trgm",
    "similarity_operator",
    # fuzzystrmatch functions
    "levenshtein",
    "levenshtein_less_equal",
    "soundex",
    "difference",
    "metaphone",
    "dmetaphone",
    "dmetaphone_alt",
    # earthdistance functions
    "ll_to_earth",
    "earth_distance",
    "earth_box",
    "earthdistance_operator",
    "point_inside_circle",
    # intarray functions
    "intarray_contains",
    "intarray_contained_by",
    "intarray_overlaps",
    "intarray_idx",
    "intarray_subarray",
    "intarray_uniq",
    "intarray_sort",
    "intarray_operator",
    # cube functions
    "cube_literal",
    "cube_dimension",
    "cube_size",
    "cube_union",
    "cube_inter",
    "cube_contains",
    "cube_distance",
    # citext functions
    "citext_literal",
    # pg_stat_statements functions
    "pg_stat_statements_reset",
    # postgis_raster functions
    "st_rast_from_hexwkb",
    "st_value",
    "st_summary",
    # pglogical functions
    "pglogical_create_node",
    "pglogical_create_publication",
    "pglogical_create_subscription",
    "pglogical_show_subscription_status",
    "pglogical_alter_subscription_synchronize",
    # pgrouting functions
    "pgr_dijkstra",
    "pgr_astar",
    # tablefunc functions
    "crosstab",
    "connectby",
    "normal_rand",
    # pg_cron functions
    "cron_schedule",
    "cron_unschedule",
    "cron_run",
    # pgaudit functions
    "pgaudit_set_role",
    "pgaudit_log_level",
    "pgaudit_include_catalog",
    # hypopg functions
    "hypopg_create_index",
    "hypopg_reset",
    "hypopg_show_indexes",
    "hypopg_estimate_size",
    # pg_surgery functions
    "pg_surgery_heap_freeze",
    "pg_surgery_heap_page_header",
    # pg_walinspect functions
    "pg_get_wal_records_info",
    "pg_get_wal_blocks_info",
    "pg_logical_emit_message",
    # address_standardizer functions
    "standardize_address",
    "parse_address",
    # pg_partman functions
    "create_time_partition",
    "create_id_partition",
    "run_maintenance",
    # pg_repack functions
    "repack_table",
    "repack_index",
    "move_tablespace",
    # orafce functions
    "add_months",
    "last_day",
    "months_between",
    "next_day",
    "nvl",
    "nvl2",
    "decode",
    "trunc",
    "round",
    "instr",
    "substr",
    # data_type functions
    "xid8_literal",
    "array_literal",
]
