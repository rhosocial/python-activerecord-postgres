# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_postgres_hstore_functions.py
"""
Tests for PostgreSQL hstore function factories.

Functions: hstore_from_record, hstore_from_key_value, hstore_akeys,
           hstore_skeys, hstore_avals, hstore_svals, hstore_each,
           hstore_to_array, hstore_to_matrix, hstore_to_json,
           hstore_to_jsonb, hstore_to_json_loose, hstore_to_jsonb_loose,
           hstore_slice, hstore_exist, hstore_defined,
           hstore_delete, hstore_delete_keys, hstore_delete_pairs,
           hstore_populate_record,
           hstore_get_value, hstore_get_value_as_text, hstore_get_values,
           hstore_concat, hstore_key_exists, hstore_all_keys_exist,
           hstore_any_key_exists, hstore_contains, hstore_contained_by,
           hstore_subtract_key, hstore_subtract_keys, hstore_subtract_pairs,
           hstore_to_array_operator, hstore_to_matrix_operator,
           hstore_record_update,
           hstore_subscript_get, hstore_subscript_set
"""

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.hstore import (
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
from rhosocial.activerecord.backend.impl.postgres.types.hstore import PostgresHstore
from rhosocial.activerecord.backend.expression import bases
from rhosocial.activerecord.backend.expression.core import FunctionCall
from rhosocial.activerecord.backend.expression.operators import BinaryExpression


class TestHstoreConstructors:
    """Tests for hstore constructor functions."""

    def test_hstore_from_record(self, postgres_dialect: PostgresDialect):
        """Test hstore_from_record() returns FunctionCall."""
        result = hstore_from_record(postgres_dialect, "my_table")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "HSTORE" in sql

    def test_hstore_from_key_value(self, postgres_dialect: PostgresDialect):
        """Test hstore_from_key_value() returns FunctionCall."""
        result = hstore_from_key_value(postgres_dialect, "name", "value")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "HSTORE" in sql


class TestHstoreKeyValueExtraction:
    """Tests for hstore key/value extraction functions."""

    def test_hstore_akeys(self, postgres_dialect: PostgresDialect):
        """Test hstore_akeys() returns FunctionCall."""
        result = hstore_akeys(postgres_dialect, "data")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "AKEYS" in sql

    def test_hstore_skeys(self, postgres_dialect: PostgresDialect):
        """Test hstore_skeys() returns FunctionCall."""
        result = hstore_skeys(postgres_dialect, "data")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "SKEYS" in sql

    def test_hstore_avals(self, postgres_dialect: PostgresDialect):
        """Test hstore_avals() returns FunctionCall."""
        result = hstore_avals(postgres_dialect, "data")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "AVALS" in sql

    def test_hstore_svals(self, postgres_dialect: PostgresDialect):
        """Test hstore_svals() returns FunctionCall."""
        result = hstore_svals(postgres_dialect, "data")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "SVALS" in sql

    def test_hstore_each(self, postgres_dialect: PostgresDialect):
        """Test hstore_each() returns FunctionCall."""
        result = hstore_each(postgres_dialect, "data")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "EACH" in sql


class TestHstoreConversion:
    """Tests for hstore conversion functions."""

    def test_hstore_to_array(self, postgres_dialect: PostgresDialect):
        """Test hstore_to_array() returns FunctionCall."""
        result = hstore_to_array(postgres_dialect, "data")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "HSTORE_TO_ARRAY" in sql

    def test_hstore_to_matrix(self, postgres_dialect: PostgresDialect):
        """Test hstore_to_matrix() returns FunctionCall."""
        result = hstore_to_matrix(postgres_dialect, "data")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "HSTORE_TO_MATRIX" in sql

    def test_hstore_to_json(self, postgres_dialect: PostgresDialect):
        """Test hstore_to_json() returns FunctionCall."""
        result = hstore_to_json(postgres_dialect, "data")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "HSTORE_TO_JSON" in sql

    def test_hstore_to_jsonb(self, postgres_dialect: PostgresDialect):
        """Test hstore_to_jsonb() returns FunctionCall."""
        result = hstore_to_jsonb(postgres_dialect, "data")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "HSTORE_TO_JSONB" in sql

    def test_hstore_to_json_loose(self, postgres_dialect: PostgresDialect):
        """Test hstore_to_json_loose() returns FunctionCall."""
        result = hstore_to_json_loose(postgres_dialect, "data")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "HSTORE_TO_JSON_LOOSE" in sql

    def test_hstore_to_jsonb_loose(self, postgres_dialect: PostgresDialect):
        """Test hstore_to_jsonb_loose() returns FunctionCall."""
        result = hstore_to_jsonb_loose(postgres_dialect, "data")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "HSTORE_TO_JSONB_LOOSE" in sql


class TestHstoreSubset:
    """Tests for hstore subset functions."""

    def test_hstore_slice(self, postgres_dialect: PostgresDialect):
        """Test hstore_slice() returns FunctionCall."""
        result = hstore_slice(postgres_dialect, "data", "ARRAY['a', 'b']")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "SLICE" in sql


class TestHstoreExistence:
    """Tests for hstore existence functions."""

    def test_hstore_exist(self, postgres_dialect: PostgresDialect):
        """Test hstore_exist() returns FunctionCall."""
        result = hstore_exist(postgres_dialect, "data", "name")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "EXIST" in sql

    def test_hstore_defined(self, postgres_dialect: PostgresDialect):
        """Test hstore_defined() returns FunctionCall."""
        result = hstore_defined(postgres_dialect, "data", "name")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "DEFINED" in sql


class TestHstoreDelete:
    """Tests for hstore delete functions."""

    def test_hstore_delete(self, postgres_dialect: PostgresDialect):
        """Test hstore_delete() returns FunctionCall."""
        result = hstore_delete(postgres_dialect, "data", "name")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "DELETE" in sql

    def test_hstore_delete_keys(self, postgres_dialect: PostgresDialect):
        """Test hstore_delete_keys() returns FunctionCall."""
        result = hstore_delete_keys(postgres_dialect, "data", "ARRAY['a', 'b']")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "DELETE" in sql

    def test_hstore_delete_pairs(self, postgres_dialect: PostgresDialect):
        """Test hstore_delete_pairs() returns FunctionCall."""
        result = hstore_delete_pairs(postgres_dialect, "data", "other")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "DELETE" in sql


class TestHstoreRecord:
    """Tests for hstore record functions."""

    def test_hstore_populate_record(self, postgres_dialect: PostgresDialect):
        """Test hstore_populate_record() returns FunctionCall."""
        result = hstore_populate_record(postgres_dialect, "my_record", "data")
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "POPULATE_RECORD" in sql


class TestHstoreOperators:
    """Tests for hstore operator functions."""

    def test_hstore_get_value(self, postgres_dialect: PostgresDialect):
        """Test hstore_get_value() returns BinaryExpression with -> operator."""
        result = hstore_get_value(postgres_dialect, "data", "name")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "->" in sql

    def test_hstore_get_value_as_text(self, postgres_dialect: PostgresDialect):
        """Test hstore_get_value_as_text() returns BinaryExpression with ->> operator."""
        result = hstore_get_value_as_text(postgres_dialect, "data", "name")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "->>" in sql

    def test_hstore_get_values(self, postgres_dialect: PostgresDialect):
        """Test hstore_get_values() returns BinaryExpression with -> operator."""
        result = hstore_get_values(postgres_dialect, "data", "ARRAY['a', 'b']")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "->" in sql

    def test_hstore_concat(self, postgres_dialect: PostgresDialect):
        """Test hstore_concat() returns BinaryExpression with || operator."""
        result = hstore_concat(postgres_dialect, "data", "other")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "||" in sql

    def test_hstore_key_exists(self, postgres_dialect: PostgresDialect):
        """Test hstore_key_exists() returns BinaryExpression with ? operator."""
        result = hstore_key_exists(postgres_dialect, "data", "name")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "?" in sql

    def test_hstore_all_keys_exist(self, postgres_dialect: PostgresDialect):
        """Test hstore_all_keys_exist() returns BinaryExpression with ?& operator."""
        result = hstore_all_keys_exist(postgres_dialect, "data", "ARRAY['a', 'b']")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "?&" in sql

    def test_hstore_any_key_exists(self, postgres_dialect: PostgresDialect):
        """Test hstore_any_key_exists() returns BinaryExpression with ?| operator."""
        result = hstore_any_key_exists(postgres_dialect, "data", "ARRAY['a', 'b']")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "?|" in sql

    def test_hstore_contains(self, postgres_dialect: PostgresDialect):
        """Test hstore_contains() returns BinaryExpression with @> operator."""
        result = hstore_contains(postgres_dialect, "data", "other")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "@>" in sql

    def test_hstore_contained_by(self, postgres_dialect: PostgresDialect):
        """Test hstore_contained_by() returns BinaryExpression with <@ operator."""
        result = hstore_contained_by(postgres_dialect, "data", "other")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "<@" in sql


class TestHstoreSubtraction:
    """Tests for hstore subtraction operator functions."""

    def test_hstore_subtract_key(self, postgres_dialect: PostgresDialect):
        """Test hstore_subtract_key() returns BinaryExpression with - operator."""
        result = hstore_subtract_key(postgres_dialect, "data", "name")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "-" in sql

    def test_hstore_subtract_keys(self, postgres_dialect: PostgresDialect):
        """Test hstore_subtract_keys() returns BinaryExpression with - operator."""
        result = hstore_subtract_keys(postgres_dialect, "data", "ARRAY['a', 'b']")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "-" in sql

    def test_hstore_subtract_pairs(self, postgres_dialect: PostgresDialect):
        """Test hstore_subtract_pairs() returns BinaryExpression with - operator."""
        result = hstore_subtract_pairs(postgres_dialect, "data", "other")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "-" in sql


class TestHstoreSpecialOperators:
    """Tests for hstore special operator functions."""

    def test_hstore_to_array_operator(self, postgres_dialect: PostgresDialect):
        """Test hstore_to_array_operator() returns BinaryExpression."""
        result = hstore_to_array_operator(postgres_dialect, "data")
        assert isinstance(result, BinaryExpression)

    def test_hstore_to_matrix_operator(self, postgres_dialect: PostgresDialect):
        """Test hstore_to_matrix_operator() returns BinaryExpression."""
        result = hstore_to_matrix_operator(postgres_dialect, "data")
        assert isinstance(result, BinaryExpression)

    def test_hstore_record_update(self, postgres_dialect: PostgresDialect):
        """Test hstore_record_update() returns BinaryExpression with #= operator."""
        result = hstore_record_update(postgres_dialect, "my_record", "data")
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "#=" in sql


class TestHstoreSubscript:
    """Tests for hstore subscript access functions."""

    def test_hstore_subscript_get(self, postgres_dialect: PostgresDialect):
        """Test hstore_subscript_get() returns expression."""
        result = hstore_subscript_get(postgres_dialect, "data", "name")
        assert isinstance(result, (BinaryExpression, FunctionCall))

    def test_hstore_subscript_set(self, postgres_dialect: PostgresDialect):
        """Test hstore_subscript_set() returns expression."""
        result = hstore_subscript_set(postgres_dialect, "data", "name", "value")
        assert isinstance(result, (BinaryExpression, FunctionCall))


class TestHstoreWithPostgresHstoreInput:
    """Tests for hstore functions with PostgresHstore input objects."""

    def test_akeys_with_hstore(self, postgres_dialect: PostgresDialect):
        """Test hstore_akeys() with PostgresHstore input."""
        h = PostgresHstore(data={"a": "1", "b": "2"})
        result = hstore_akeys(postgres_dialect, h)
        assert isinstance(result, FunctionCall)
        sql, params = result.to_sql()
        assert "AKEYS" in sql

    def test_contains_with_dict(self, postgres_dialect: PostgresDialect):
        """Test hstore_contains() with dict input."""
        result = hstore_contains(postgres_dialect, "data", {"color": "red"})
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "@>" in sql

    def test_concat_with_hstore(self, postgres_dialect: PostgresDialect):
        """Test hstore_concat() with PostgresHstore inputs."""
        left = PostgresHstore(data={"a": "1"})
        right = PostgresHstore(data={"b": "2"})
        result = hstore_concat(postgres_dialect, left, right)
        assert isinstance(result, BinaryExpression)
        sql, params = result.to_sql()
        assert "||" in sql


class TestHstoreAllFunctionsReturnExpressionObjects:
    """Test that all hstore functions return Expression objects (not strings)."""

    def test_constructors_return_expressions(self, postgres_dialect: PostgresDialect):
        """Test constructors return BaseExpression."""
        assert isinstance(hstore_from_record(postgres_dialect, "t"), bases.BaseExpression)
        assert isinstance(hstore_from_key_value(postgres_dialect, "k", "v"), bases.BaseExpression)

    def test_extraction_functions_return_expressions(self, postgres_dialect: PostgresDialect):
        """Test extraction functions return BaseExpression."""
        assert isinstance(hstore_akeys(postgres_dialect, "d"), bases.BaseExpression)
        assert isinstance(hstore_skeys(postgres_dialect, "d"), bases.BaseExpression)
        assert isinstance(hstore_avals(postgres_dialect, "d"), bases.BaseExpression)
        assert isinstance(hstore_svals(postgres_dialect, "d"), bases.BaseExpression)
        assert isinstance(hstore_each(postgres_dialect, "d"), bases.BaseExpression)

    def test_conversion_functions_return_expressions(self, postgres_dialect: PostgresDialect):
        """Test conversion functions return BaseExpression."""
        assert isinstance(hstore_to_array(postgres_dialect, "d"), bases.BaseExpression)
        assert isinstance(hstore_to_matrix(postgres_dialect, "d"), bases.BaseExpression)
        assert isinstance(hstore_to_json(postgres_dialect, "d"), bases.BaseExpression)
        assert isinstance(hstore_to_jsonb(postgres_dialect, "d"), bases.BaseExpression)
        assert isinstance(hstore_to_json_loose(postgres_dialect, "d"), bases.BaseExpression)
        assert isinstance(hstore_to_jsonb_loose(postgres_dialect, "d"), bases.BaseExpression)

    def test_operator_functions_return_expressions(self, postgres_dialect: PostgresDialect):
        """Test operator functions return BaseExpression."""
        assert isinstance(hstore_get_value(postgres_dialect, "d", "k"), bases.BaseExpression)
        assert isinstance(hstore_get_value_as_text(postgres_dialect, "d", "k"), bases.BaseExpression)
        assert isinstance(hstore_get_values(postgres_dialect, "d", "k"), bases.BaseExpression)
        assert isinstance(hstore_concat(postgres_dialect, "d", "o"), bases.BaseExpression)
        assert isinstance(hstore_key_exists(postgres_dialect, "d", "k"), bases.BaseExpression)
        assert isinstance(hstore_all_keys_exist(postgres_dialect, "d", "k"), bases.BaseExpression)
        assert isinstance(hstore_any_key_exists(postgres_dialect, "d", "k"), bases.BaseExpression)
        assert isinstance(hstore_contains(postgres_dialect, "d", "o"), bases.BaseExpression)
        assert isinstance(hstore_contained_by(postgres_dialect, "d", "o"), bases.BaseExpression)
        assert isinstance(hstore_subtract_key(postgres_dialect, "d", "k"), bases.BaseExpression)
        assert isinstance(hstore_subtract_keys(postgres_dialect, "d", "k"), bases.BaseExpression)
        assert isinstance(hstore_subtract_pairs(postgres_dialect, "d", "o"), bases.BaseExpression)
        assert isinstance(hstore_to_array_operator(postgres_dialect, "d"), bases.BaseExpression)
        assert isinstance(hstore_to_matrix_operator(postgres_dialect, "d"), bases.BaseExpression)
        assert isinstance(hstore_record_update(postgres_dialect, "r", "d"), bases.BaseExpression)
