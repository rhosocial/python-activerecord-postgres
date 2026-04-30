# src/rhosocial/activerecord/backend/impl/postgres/function_versions.py
"""PostgreSQL function version requirements, organized by category.

This module defines version requirements for PostgreSQL functions,
categorized by topic. The categories are assembled into
POSTGRES_FUNCTION_VERSIONS for use by PostgresDialect.

Function sources:
- Core built-in functions: require only PostgreSQL server version
- Extension functions: require extension installed + version check
"""

from dataclasses import dataclass
from typing import Optional, Tuple, Dict


@dataclass(frozen=True)
class FunctionSupportInfo:
    """Detailed information about whether a function is supported.

    Attributes:
        supported: Whether the function is available
        reason: If not supported, the reason why. Possible values:
            - None: function is supported (supported=True)
            - "pg_version_too_low": PG server version < min_pg_version
            - "pg_version_too_high": PG server version > max_pg_version
            - "extension_not_probed": introspect_and_adapt() not called
            - "extension_not_installed": extension not installed
            - "extension_version_insufficient": extension installed but version too low
    """

    supported: bool
    reason: Optional[str] = None


@dataclass(frozen=True)
class FunctionVersionRequirement:
    """Describe version requirements for a PostgreSQL function.

    Functions come from two sources:
    1. Core built-in: only PG server version matters
    2. Extension-provided: requires extension installed + version check

    When extension is None, the function is built-in.
    When extension is set, the function requires that extension to be
    installed (as detected by introspect_and_adapt()).

    Attributes:
        min_pg_version: Minimum PostgreSQL server version, e.g. (12, 0, 0)
        max_pg_version: Maximum PostgreSQL server version (exclusive)
        extension: Extension name, e.g. "hstore", "vector"
        min_ext_version: Minimum extension version string, e.g. "1.0"
        ext_feature: Feature name in KNOWN_EXTENSIONS, for feature-level checks
    """

    min_pg_version: Optional[Tuple[int, ...]] = None
    max_pg_version: Optional[Tuple[int, ...]] = None
    extension: Optional[str] = None
    min_ext_version: Optional[str] = None
    ext_feature: Optional[str] = None

    @property
    def is_extension_function(self) -> bool:
        """Whether this function requires an extension."""
        return self.extension is not None


# ── Core PG built-in functions ────────────────────────────────

JSON_PATH_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "jsonb_path_query": FunctionVersionRequirement(min_pg_version=(12, 0, 0)),
    "jsonb_path_query_first": FunctionVersionRequirement(min_pg_version=(12, 0, 0)),
    "jsonb_path_exists": FunctionVersionRequirement(min_pg_version=(12, 0, 0)),
    "jsonb_path_match": FunctionVersionRequirement(min_pg_version=(12, 0, 0)),
    "json_path_root": FunctionVersionRequirement(min_pg_version=(12, 0, 0)),
    "json_path_key": FunctionVersionRequirement(min_pg_version=(12, 0, 0)),
    "json_path_index": FunctionVersionRequirement(min_pg_version=(12, 0, 0)),
    "json_path_wildcard": FunctionVersionRequirement(min_pg_version=(12, 0, 0)),
    "json_path_filter": FunctionVersionRequirement(min_pg_version=(12, 0, 0)),
}

RANGE_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "range_contains": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_contained_by": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_contains_range": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_overlaps": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_adjacent": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_strictly_left_of": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_strictly_right_of": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_not_extend_right": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_not_extend_left": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_union": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_intersection": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_difference": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_lower": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_upper": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_is_empty": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_lower_inc": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_upper_inc": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_lower_inf": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "range_upper_inf": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
}

GEOMETRIC_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "geometry_distance": FunctionVersionRequirement(),
    "geometry_contains": FunctionVersionRequirement(),
    "geometry_contained_by": FunctionVersionRequirement(),
    "geometry_overlaps": FunctionVersionRequirement(),
    "geometry_strictly_left": FunctionVersionRequirement(),
    "geometry_strictly_right": FunctionVersionRequirement(),
    "geometry_not_extend_right": FunctionVersionRequirement(),
    "geometry_not_extend_left": FunctionVersionRequirement(),
    "geometry_area": FunctionVersionRequirement(),
    "geometry_center": FunctionVersionRequirement(),
    "geometry_length": FunctionVersionRequirement(),
    "geometry_width": FunctionVersionRequirement(),
    "geometry_height": FunctionVersionRequirement(),
    "geometry_npoints": FunctionVersionRequirement(),
}

ENUM_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "enum_range": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "enum_first": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "enum_last": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "enum_lt": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "enum_le": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "enum_gt": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "enum_ge": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
}

BIT_STRING_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "bit_concat": FunctionVersionRequirement(),
    "bit_and": FunctionVersionRequirement(),
    "bit_or": FunctionVersionRequirement(),
    "bit_xor": FunctionVersionRequirement(),
    "bit_not": FunctionVersionRequirement(),
    "bit_shift_left": FunctionVersionRequirement(),
    "bit_shift_right": FunctionVersionRequirement(),
    "bit_length": FunctionVersionRequirement(),
    "bit_length_func": FunctionVersionRequirement(),
    "bit_octet_length": FunctionVersionRequirement(),
    "bit_get_bit": FunctionVersionRequirement(),
    "bit_set_bit": FunctionVersionRequirement(),
    "bit_count": FunctionVersionRequirement(min_pg_version=(9, 5, 0)),
}

TEXT_SEARCH_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "to_tsvector": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "to_tsquery": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "plainto_tsquery": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "phraseto_tsquery": FunctionVersionRequirement(min_pg_version=(9, 6, 0)),
    "websearch_to_tsquery": FunctionVersionRequirement(min_pg_version=(11, 0, 0)),
    "ts_matches": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "ts_matches_expr": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "ts_rank": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "ts_rank_cd": FunctionVersionRequirement(min_pg_version=(8, 5, 0)),
    "ts_headline": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "tsvector_concat": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "tsvector_strip": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "tsvector_setweight": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "tsvector_length": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
}

XML_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "xmlparse": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "xpath_query": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "xpath_exists": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "xml_is_well_formed": FunctionVersionRequirement(min_pg_version=(9, 1, 0)),
}

MATH_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "round_": FunctionVersionRequirement(),
    "pow": FunctionVersionRequirement(),
    "power": FunctionVersionRequirement(),
    "sqrt": FunctionVersionRequirement(),
    "mod": FunctionVersionRequirement(),
    "ceil": FunctionVersionRequirement(),
    "floor": FunctionVersionRequirement(),
    "trunc": FunctionVersionRequirement(),
    "max_": FunctionVersionRequirement(),
    "min_": FunctionVersionRequirement(),
    "avg": FunctionVersionRequirement(),
}

ARRAY_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "array_agg": FunctionVersionRequirement(),
    "array_append": FunctionVersionRequirement(),
    "array_cat": FunctionVersionRequirement(),
    "array_dims": FunctionVersionRequirement(),
    "array_fill": FunctionVersionRequirement(min_pg_version=(8, 4, 0)),
    "array_length": FunctionVersionRequirement(),
    "array_lower": FunctionVersionRequirement(),
    "array_ndims": FunctionVersionRequirement(min_pg_version=(8, 4, 0)),
    "array_position": FunctionVersionRequirement(min_pg_version=(9, 5, 0)),
    "array_positions": FunctionVersionRequirement(min_pg_version=(9, 5, 0)),
    "array_prepend": FunctionVersionRequirement(),
    "array_remove": FunctionVersionRequirement(),
    "array_replace": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "array_to_string": FunctionVersionRequirement(),
    "array_upper": FunctionVersionRequirement(),
    "unnest": FunctionVersionRequirement(min_pg_version=(8, 4, 0)),
    "array_agg_distinct": FunctionVersionRequirement(),
    "string_to_array": FunctionVersionRequirement(),
}

NETWORK_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "inet_client_addr": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "inet_client_port": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "inet_server_addr": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "inet_server_port": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "inet_merge": FunctionVersionRequirement(min_pg_version=(9, 5, 0)),
    "inet_and": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "inet_or": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "inetnot": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "inet_set_mask": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "inet_masklen": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "inet_netmask": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "inet_network": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "inet_recv": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "inet_show": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "cidr_netmask": FunctionVersionRequirement(min_pg_version=(8, 3, 0)),
    "macaddr8_set7bit": FunctionVersionRequirement(min_pg_version=(10, 0, 0)),
}

RANGE_CONSTRUCTOR_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "int4range": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "int8range": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "numrange": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "tsrange": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "tstzrange": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
    "daterange": FunctionVersionRequirement(min_pg_version=(9, 2, 0)),
}

# ── Extension-provided functions ──────────────────────────────

UUID_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    # Built-in since PG 13
    "gen_random_uuid": FunctionVersionRequirement(min_pg_version=(13, 0, 0)),
    # uuid-ossp extension functions
    "uuid_generate_v1": FunctionVersionRequirement(extension="uuid-ossp", min_ext_version="1.0"),
    "uuid_generate_v1mc": FunctionVersionRequirement(extension="uuid-ossp", min_ext_version="1.0"),
    "uuid_generate_v3": FunctionVersionRequirement(extension="uuid-ossp", min_ext_version="1.0"),
    "uuid_generate_v4": FunctionVersionRequirement(extension="uuid-ossp", min_ext_version="1.0"),
    "uuid_generate_v5": FunctionVersionRequirement(extension="uuid-ossp", min_ext_version="1.0"),
    "uuid_nil": FunctionVersionRequirement(extension="uuid-ossp", min_ext_version="1.0"),
    "uuid_max": FunctionVersionRequirement(extension="uuid-ossp", min_ext_version="1.0"),
}

HSTORE_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "hstore_from_record": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_from_key_value": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_akeys": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_skeys": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_avals": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_svals": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_each": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_to_array": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_to_matrix": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    # hstore_to_json requires PG 9.3+ (JSON support)
    "hstore_to_json": FunctionVersionRequirement(
        extension="hstore", min_ext_version="1.0", min_pg_version=(9, 3, 0),
    ),
    # hstore_to_jsonb requires PG 9.4+ (JSONB type)
    "hstore_to_jsonb": FunctionVersionRequirement(
        extension="hstore", min_ext_version="1.0", min_pg_version=(9, 4, 0),
    ),
    "hstore_to_json_loose": FunctionVersionRequirement(
        extension="hstore", min_ext_version="1.0", min_pg_version=(9, 3, 0),
    ),
    "hstore_to_jsonb_loose": FunctionVersionRequirement(
        extension="hstore", min_ext_version="1.0", min_pg_version=(9, 4, 0),
    ),
    "hstore_slice": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_exist": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_defined": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_delete": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_delete_keys": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_delete_pairs": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_populate_record": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_get_value": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_get_value_as_text": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_get_values": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_concat": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_key_exists": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_all_keys_exist": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_any_key_exists": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_contains": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_contained_by": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_subtract_key": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_subtract_keys": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_subtract_pairs": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_to_array_operator": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_to_matrix_operator": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    "hstore_record_update": FunctionVersionRequirement(extension="hstore", min_ext_version="1.0"),
    # Subscript access requires PG 9.0+
    "hstore_subscript_get": FunctionVersionRequirement(
        extension="hstore", min_ext_version="1.0", min_pg_version=(9, 0, 0),
    ),
    "hstore_subscript_set": FunctionVersionRequirement(
        extension="hstore", min_ext_version="1.0", min_pg_version=(9, 0, 0),
    ),
}

PGVECTOR_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "vector_l2_distance": FunctionVersionRequirement(extension="vector", ext_feature="similarity_search"),
    "vector_cosine_distance": FunctionVersionRequirement(extension="vector", ext_feature="similarity_search"),
    "vector_inner_product": FunctionVersionRequirement(extension="vector", ext_feature="similarity_search"),
    "vector_cosine_similarity": FunctionVersionRequirement(extension="vector", ext_feature="similarity_search"),
    "vector_literal": FunctionVersionRequirement(extension="vector", ext_feature="type"),
}

POSTGIS_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {
    "st_make_point": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_geom_from_text": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_geog_from_text": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_set_srid": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_transform": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_contains": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_intersects": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_within": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_dwithin": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_crosses": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_touches": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_overlaps": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_distance": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_area": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_length": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_as_geojson": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_as_text": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_buffer": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_envelope": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
    "st_centroid": FunctionVersionRequirement(extension="postgis", ext_feature="spatial_functions"),
}

# ── Assemble ──────────────────────────────────────────────────

_ALL_CATEGORIES = [
    JSON_PATH_FUNCTION_VERSIONS,
    RANGE_FUNCTION_VERSIONS,
    GEOMETRIC_FUNCTION_VERSIONS,
    ENUM_FUNCTION_VERSIONS,
    BIT_STRING_FUNCTION_VERSIONS,
    TEXT_SEARCH_FUNCTION_VERSIONS,
    XML_FUNCTION_VERSIONS,
    MATH_FUNCTION_VERSIONS,
    ARRAY_FUNCTION_VERSIONS,
    NETWORK_FUNCTION_VERSIONS,
    RANGE_CONSTRUCTOR_FUNCTION_VERSIONS,
    UUID_FUNCTION_VERSIONS,
    HSTORE_FUNCTION_VERSIONS,
    PGVECTOR_FUNCTION_VERSIONS,
    POSTGIS_FUNCTION_VERSIONS,
]

POSTGRES_FUNCTION_VERSIONS: Dict[str, FunctionVersionRequirement] = {}
for _cat in _ALL_CATEGORIES:
    POSTGRES_FUNCTION_VERSIONS.update(_cat)
