# tests/rhosocial/activerecord_postgres_test/feature/backend/test_dialect_function_support.py
"""
Test SQLFunctionSupport protocol implementation for PostgreSQL dialect.

This module tests the supports_functions() method and version-dependent
function availability detection in PostgresDialect.
"""
import pytest
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.function_versions import FunctionSupportInfo


class TestPostgreSQLFunctionSupportBasic:
    """Basic tests for PostgreSQL function support detection."""

    def test_supports_functions_returns_dict(self):
        """Test that supports_functions returns a dictionary."""
        dialect = PostgresDialect()
        result = dialect.supports_functions()
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_supports_functions_all_values_are_support_info(self):
        """Test that all values in the returned dict are FunctionSupportInfo."""
        dialect = PostgresDialect()
        result = dialect.supports_functions()
        for func_name, info in result.items():
            assert isinstance(info, FunctionSupportInfo), f"Value for {func_name} is not FunctionSupportInfo"

    def test_core_functions_always_supported(self):
        """Test that core functions are marked as supported."""
        dialect = PostgresDialect()
        result = dialect.supports_functions()
        core_functions = ["count", "sum_", "avg", "min_", "max_", "coalesce", "nullif"]
        for func in core_functions:
            assert func in result, f"Core function {func} not in result"
            assert result[func].supported, f"Core function {func} should be supported"


class TestPostgreSQLFunctionSupportVersionDependent:
    """Tests for version-dependent function support."""

    def test_json_path_functions_require_pg_12(self):
        """Test that JSONPath functions require PostgreSQL 12+."""
        json_path_functions = ["jsonb_path_query", "jsonb_path_query_first",
                              "jsonb_path_exists", "jsonb_path_match",
                              "json_path_root", "json_path_key", "json_path_index",
                              "json_path_wildcard", "json_path_filter"]

        dialect_old = PostgresDialect(version=(11, 99, 99))
        result_old = dialect_old.supports_functions()
        for func in json_path_functions:
            assert not result_old.get(func).supported

        dialect_new = PostgresDialect(version=(12, 0, 0))
        result_new = dialect_new.supports_functions()
        for func in json_path_functions:
            assert result_new.get(func).supported

    def test_range_functions_require_pg_9_2(self):
        """Test that range functions require PostgreSQL 9.2+."""
        range_functions = ["range_contains", "range_contained_by", "range_overlaps",
                          "range_adjacent", "range_union", "range_intersection",
                          "range_lower", "range_upper", "range_is_empty"]

        dialect_old = PostgresDialect(version=(9, 1, 99))
        result_old = dialect_old.supports_functions()
        for func in range_functions:
            assert not result_old.get(func).supported

        dialect_new = PostgresDialect(version=(9, 2, 0))
        result_new = dialect_new.supports_functions()
        for func in range_functions:
            assert result_new.get(func).supported

    def test_range_constructors_require_pg_9_2(self):
        """Test that range constructors require PostgreSQL 9.2+."""
        range_constructors = ["int4range", "int8range", "numrange",
                             "tsrange", "tstzrange", "daterange"]

        dialect_old = PostgresDialect(version=(9, 1, 99))
        result_old = dialect_old.supports_functions()
        for func in range_constructors:
            assert not result_old.get(func).supported

        dialect_new = PostgresDialect(version=(9, 2, 0))
        result_new = dialect_new.supports_functions()
        for func in range_constructors:
            assert result_new.get(func).supported

    def test_enum_functions_require_pg_8_3(self):
        """Test that enum functions require PostgreSQL 8.3+."""
        enum_functions = ["enum_range", "enum_first", "enum_last",
                         "enum_lt", "enum_le", "enum_gt", "enum_ge"]

        dialect_old = PostgresDialect(version=(8, 2, 99))
        result_old = dialect_old.supports_functions()
        for func in enum_functions:
            assert not result_old.get(func).supported

        dialect_new = PostgresDialect(version=(8, 3, 0))
        result_new = dialect_new.supports_functions()
        for func in enum_functions:
            assert result_new.get(func).supported

    def test_text_search_functions_require_pg_8_3(self):
        """Test that text search functions require PostgreSQL 8.3+."""
        ts_functions = ["to_tsvector", "to_tsquery", "plainto_tsquery",
                       "ts_matches", "ts_headline", "tsvector_concat"]

        dialect_old = PostgresDialect(version=(8, 2, 99))
        result_old = dialect_old.supports_functions()
        for func in ts_functions:
            assert not result_old.get(func).supported

        dialect_new = PostgresDialect(version=(8, 3, 0))
        result_new = dialect_new.supports_functions()
        for func in ts_functions:
            assert result_new.get(func).supported

    def test_phraseto_tsquery_requires_pg_9_6(self):
        """Test that phraseto_tsquery requires PostgreSQL 9.6+."""
        dialect_old = PostgresDialect(version=(9, 5, 99))
        result_old = dialect_old.supports_functions()
        assert not result_old.get("phraseto_tsquery").supported

        dialect_new = PostgresDialect(version=(9, 6, 0))
        result_new = dialect_new.supports_functions()
        assert result_new.get("phraseto_tsquery").supported

    def test_websearch_to_tsquery_requires_pg_11(self):
        """Test that websearch_to_tsquery requires PostgreSQL 11+."""
        dialect_old = PostgresDialect(version=(10, 99, 99))
        result_old = dialect_old.supports_functions()
        assert not result_old.get("websearch_to_tsquery").supported

        dialect_new = PostgresDialect(version=(11, 0, 0))
        result_new = dialect_new.supports_functions()
        assert result_new.get("websearch_to_tsquery").supported

    def test_array_functions_require_various_versions(self):
        """Test that array functions have various version requirements."""
        dialect_83 = PostgresDialect(version=(8, 3, 0))
        dialect_84 = PostgresDialect(version=(8, 4, 0))
        dialect_95 = PostgresDialect(version=(9, 5, 0))

        result_83 = dialect_83.supports_functions()
        result_84 = dialect_84.supports_functions()
        result_95 = dialect_95.supports_functions()

        assert not result_83.get("array_fill").supported
        assert result_84.get("array_fill").supported

        assert not result_83.get("array_ndims").supported
        assert result_84.get("array_ndims").supported

        assert not result_84.get("array_position").supported
        assert result_95.get("array_position").supported

    def test_bit_count_requires_pg_9_5(self):
        """Test that bit_count requires PostgreSQL 9.5+."""
        dialect_old = PostgresDialect(version=(9, 4, 99))
        result_old = dialect_old.supports_functions()
        assert not result_old.get("bit_count").supported

        dialect_new = PostgresDialect(version=(9, 5, 0))
        result_new = dialect_new.supports_functions()
        assert result_new.get("bit_count").supported

    def test_xml_functions_require_pg_8_3(self):
        """Test that XML functions require PostgreSQL 8.3+."""
        xml_functions = ["xmlparse", "xpath_query", "xpath_exists"]

        dialect_old = PostgresDialect(version=(8, 2, 99))
        result_old = dialect_old.supports_functions()
        for func in xml_functions:
            assert not result_old.get(func).supported

        dialect_new = PostgresDialect(version=(8, 3, 0))
        result_new = dialect_new.supports_functions()
        for func in xml_functions:
            assert result_new.get(func).supported

    def test_xml_is_well_formed_requires_pg_9_1(self):
        """Test that xml_is_well_formed requires PostgreSQL 9.1+."""
        dialect_old = PostgresDialect(version=(9, 0, 99))
        result_old = dialect_old.supports_functions()
        assert not result_old.get("xml_is_well_formed").supported

        dialect_new = PostgresDialect(version=(9, 1, 0))
        result_new = dialect_new.supports_functions()
        assert result_new.get("xml_is_well_formed").supported

    def test_hstore_to_json_functions_require_pg_9_3(self):
        """Test that hstore_to_json functions require PostgreSQL 9.3+ and hstore extension."""
        dialect_old = PostgresDialect(version=(9, 2, 99))
        result_old = dialect_old.supports_functions()
        # Below PG version threshold → pg_version_too_low
        assert not result_old.get("hstore_to_json").supported
        assert result_old.get("hstore_to_json").reason == "pg_version_too_low"

        dialect_new = PostgresDialect(version=(9, 3, 0))
        result_new = dialect_new.supports_functions()
        # PG version sufficient but extension not probed → extension_not_probed
        assert not result_new.get("hstore_to_json").supported
        assert result_new.get("hstore_to_json").reason == "extension_not_probed"

    def test_hstore_to_jsonb_requires_pg_9_4(self):
        """Test that hstore_to_jsonb requires PostgreSQL 9.4+ and hstore extension."""
        dialect_old = PostgresDialect(version=(9, 3, 99))
        result_old = dialect_old.supports_functions()
        # Below PG version threshold → pg_version_too_low
        assert not result_old.get("hstore_to_jsonb").supported
        assert result_old.get("hstore_to_jsonb").reason == "pg_version_too_low"

        dialect_new = PostgresDialect(version=(9, 4, 0))
        result_new = dialect_new.supports_functions()
        # PG version sufficient but extension not probed → extension_not_probed
        assert not result_new.get("hstore_to_jsonb").supported
        assert result_new.get("hstore_to_jsonb").reason == "extension_not_probed"

    def test_macaddr8_set7bit_requires_pg_10(self):
        """Test that macaddr8_set7bit requires PostgreSQL 10+."""
        dialect_old = PostgresDialect(version=(9, 99, 99))
        result_old = dialect_old.supports_functions()
        assert not result_old.get("macaddr8_set7bit").supported

        dialect_new = PostgresDialect(version=(10, 0, 0))
        result_new = dialect_new.supports_functions()
        assert result_new.get("macaddr8_set7bit").supported

    def test_always_available_functions(self):
        """Test functions that are available in all PostgreSQL versions."""
        dialect = PostgresDialect()
        result = dialect.supports_functions()

        always_available = [
            "geometry_distance", "geometry_contains", "geometry_area",
            "bit_concat", "bit_and", "bit_or", "bit_xor", "bit_shift_left",
            "round_", "pow", "power", "sqrt", "mod", "ceil", "floor",
            "array_agg", "array_append", "array_cat", "array_length",
        ]
        for func in always_available:
            assert result.get(func).supported, f"{func} should be always available"


class TestPostgreSQLFunctionSupportPrivateMethod:
    """Tests for the private _is_postgres_function_supported method."""

    def test_unknown_function_returns_true(self):
        """Test that unknown functions return True (no restriction)."""
        dialect = PostgresDialect()
        result = dialect._is_postgres_function_supported("unknown_function_xyz")
        assert result is True

    def test_version_restricted_function_below_minimum(self):
        """Test that version-restricted function returns False below minimum."""
        dialect = PostgresDialect(version=(11, 0, 0))
        result = dialect._is_postgres_function_supported("jsonb_path_query")
        assert result is False

    def test_version_restricted_function_at_minimum(self):
        """Test that version-restricted function returns True at minimum."""
        dialect = PostgresDialect(version=(12, 0, 0))
        result = dialect._is_postgres_function_supported("jsonb_path_query")
        assert result is True

    def test_version_restricted_function_above_minimum(self):
        """Test that version-restricted function returns True above minimum."""
        dialect = PostgresDialect(version=(15, 0, 0))
        result = dialect._is_postgres_function_supported("jsonb_path_query")
        assert result is True


class TestPostgreSQLFunctionSupportIntegration:
    """Integration tests for function support detection."""

    def test_function_dict_contains_both_core_and_backend_functions(self):
        """Test that the result contains both core and PostgreSQL-specific functions."""
        dialect = PostgresDialect()
        result = dialect.supports_functions()

        assert any(func in result for func in ["count", "sum", "avg"])
        assert any(func in result for func in ["range_contains", "geometry_distance", "enum_range"])

    def test_function_support_changes_with_version(self):
        """Test that function support changes across different versions."""
        old_dialect = PostgresDialect(version=(8, 2, 0))
        new_dialect = PostgresDialect(version=(15, 0, 0))

        old_result = old_dialect.supports_functions()
        new_result = new_dialect.supports_functions()

        assert not old_result.get("enum_range").supported
        assert new_result.get("enum_range").supported

        assert not old_result.get("jsonb_path_query").supported
        assert new_result.get("jsonb_path_query").supported
