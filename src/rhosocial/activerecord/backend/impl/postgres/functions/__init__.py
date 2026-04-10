# src/rhosocial/activerecord/backend/impl/postgres/functions/__init__.py
"""
PostgreSQL type-specific functions and operators.

This module provides SQL expression generators for PostgreSQL-specific
functions and operators for various data types.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions.html

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return expression objects or SQL expression strings
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
    uuid_nil,
    uuid_max,
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
    "uuid_nil",
    "uuid_max",
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
]
