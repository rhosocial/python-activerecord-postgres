# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_postgres_hstore_functions.py
"""
Tests for PostgreSQL hstore functions.

Functions: hstore_from_record, hstore_from_key_value, hstore_akeys,
           hstore_skeys, hstore_avals, hstore_svals, hstore_each,
           hstore_to_array, hstore_to_matrix, hstore_to_json,
           hstore_to_jsonb, hstore_to_json_loose, hstore_to_jsonb_loose,
           hstore_slice, hstore_exist, hstore_defined,
           hstore_delete, hstore_delete_keys, hstore_delete_pairs,
           hstore_populate_record,
           hstore_get_value, hstore_get_values, hstore_concat,
           hstore_key_exists, hstore_all_keys_exist, hstore_any_key_exists,
           hstore_contains, hstore_contained_by,
           hstore_subtract_key, hstore_subtract_keys, hstore_subtract_pairs,
           hstore_to_array_operator, hstore_to_matrix_operator,
           hstore_record_update,
           hstore_subscript_get, hstore_subscript_set
"""

from unittest.mock import MagicMock

from rhosocial.activerecord.backend.impl.postgres.functions.hstore import (
    _to_sql,
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


class TestToSql:
    """Tests for the internal _to_sql helper function."""

    def test_to_sql_with_to_sql_method(self):
        """Test _to_sql with an object that has a to_sql method."""
        mock_expr = MagicMock()
        mock_expr.to_sql.return_value = ("mocked_sql",)
        result = _to_sql(mock_expr)
        assert result == "mocked_sql"
        mock_expr.to_sql.assert_called_once()

    def test_to_sql_with_plain_string(self):
        """Test _to_sql with a plain string value."""
        result = _to_sql("plain_value")
        assert result == "plain_value"

    def test_to_sql_with_number(self):
        """Test _to_sql with a number value."""
        result = _to_sql(42)
        assert result == "42"


class TestHstoreConstructors:
    """Tests for hstore constructor functions."""

    def test_hstore_from_record(self):
        """Test hstore_from_record() function."""
        result = hstore_from_record("ROW(1,2)")
        assert result == "hstore(ROW(1,2))"

    def test_hstore_from_key_value(self):
        """Test hstore_from_key_value() function."""
        result = hstore_from_key_value("'name'", "'value'")
        assert result == "hstore('name', 'value')"


class TestHstoreKeyValueExtraction:
    """Tests for hstore key/value extraction functions."""

    def test_hstore_akeys(self):
        """Test hstore_akeys() function."""
        result = hstore_akeys("data")
        assert result == "akeys(data)"

    def test_hstore_skeys(self):
        """Test hstore_skeys() function."""
        result = hstore_skeys("data")
        assert result == "skeys(data)"

    def test_hstore_avals(self):
        """Test hstore_avals() function."""
        result = hstore_avals("data")
        assert result == "avals(data)"

    def test_hstore_svals(self):
        """Test hstore_svals() function."""
        result = hstore_svals("data")
        assert result == "svals(data)"

    def test_hstore_each(self):
        """Test hstore_each() function."""
        result = hstore_each("data")
        assert result == "each(data)"


class TestHstoreConversion:
    """Tests for hstore conversion functions."""

    def test_hstore_to_array(self):
        """Test hstore_to_array() function."""
        result = hstore_to_array("data")
        assert result == "hstore_to_array(data)"

    def test_hstore_to_matrix(self):
        """Test hstore_to_matrix() function."""
        result = hstore_to_matrix("data")
        assert result == "hstore_to_matrix(data)"

    def test_hstore_to_json(self):
        """Test hstore_to_json() function."""
        result = hstore_to_json("data")
        assert result == "hstore_to_json(data)"

    def test_hstore_to_jsonb(self):
        """Test hstore_to_jsonb() function."""
        result = hstore_to_jsonb("data")
        assert result == "hstore_to_jsonb(data)"

    def test_hstore_to_json_loose(self):
        """Test hstore_to_json_loose() function."""
        result = hstore_to_json_loose("data")
        assert result == "hstore_to_json_loose(data)"

    def test_hstore_to_jsonb_loose(self):
        """Test hstore_to_jsonb_loose() function."""
        result = hstore_to_jsonb_loose("data")
        assert result == "hstore_to_jsonb_loose(data)"


class TestHstoreSubset:
    """Tests for hstore subset functions."""

    def test_hstore_slice(self):
        """Test hstore_slice() function."""
        result = hstore_slice("data", "ARRAY['a', 'b']")
        assert result == "slice(data, ARRAY['a', 'b'])"


class TestHstoreExistence:
    """Tests for hstore existence functions."""

    def test_hstore_exist(self):
        """Test hstore_exist() function."""
        result = hstore_exist("data", "'name'")
        assert result == "exist(data, 'name')"

    def test_hstore_defined(self):
        """Test hstore_defined() function."""
        result = hstore_defined("data", "'name'")
        assert result == "defined(data, 'name')"


class TestHstoreDelete:
    """Tests for hstore delete functions."""

    def test_hstore_delete(self):
        """Test hstore_delete() function."""
        result = hstore_delete("data", "'name'")
        assert result == "delete(data, 'name')"

    def test_hstore_delete_keys(self):
        """Test hstore_delete_keys() function."""
        result = hstore_delete_keys("data", "ARRAY['name', 'age']")
        assert result == "delete(data, ARRAY['name', 'age'])"

    def test_hstore_delete_pairs(self):
        """Test hstore_delete_pairs() function."""
        result = hstore_delete_pairs("data", "'a=>1'::hstore")
        assert result == "delete(data, 'a=>1'::hstore)"


class TestHstoreRecord:
    """Tests for hstore record functions."""

    def test_hstore_populate_record(self):
        """Test hstore_populate_record() function."""
        result = hstore_populate_record("ROW(1,2)", "'f1=>42'::hstore")
        assert result == "populate_record(ROW(1,2), 'f1=>42'::hstore)"


class TestHstoreOperators:
    """Tests for hstore operator functions."""

    def test_hstore_get_value(self):
        """Test hstore_get_value() function (operator ->)."""
        result = hstore_get_value("data", "'name'")
        assert result == "data -> 'name'"

    def test_hstore_get_values(self):
        """Test hstore_get_values() function (operator -> text[])."""
        result = hstore_get_values("data", "ARRAY['name', 'age']")
        assert result == "data -> ARRAY['name', 'age']"

    def test_hstore_concat(self):
        """Test hstore_concat() function (operator ||)."""
        result = hstore_concat("'a=>1'::hstore", "'b=>2'::hstore")
        assert result == "'a=>1'::hstore || 'b=>2'::hstore"

    def test_hstore_key_exists(self):
        """Test hstore_key_exists() function (operator ?)."""
        result = hstore_key_exists("data", "'name'")
        assert result == "data ? 'name'"

    def test_hstore_all_keys_exist(self):
        """Test hstore_all_keys_exist() function (operator ?&)."""
        result = hstore_all_keys_exist("data", "ARRAY['a', 'b']")
        assert result == "data ?& ARRAY['a', 'b']"

    def test_hstore_any_key_exists(self):
        """Test hstore_any_key_exists() function (operator ?|)."""
        result = hstore_any_key_exists("data", "ARRAY['a', 'b']")
        assert result == "data ?| ARRAY['a', 'b']"

    def test_hstore_contains(self):
        """Test hstore_contains() function (operator @>)."""
        result = hstore_contains("data", "'a=>1'")
        assert result == "data @> 'a=>1'"

    def test_hstore_contained_by(self):
        """Test hstore_contained_by() function (operator <@)."""
        result = hstore_contained_by("'a=>1'", "data")
        assert result == "'a=>1' <@ data"


class TestHstoreSubtraction:
    """Tests for hstore subtraction operator functions."""

    def test_hstore_subtract_key(self):
        """Test hstore_subtract_key() function (operator - text)."""
        result = hstore_subtract_key("data", "'name'")
        assert result == "data - 'name'"

    def test_hstore_subtract_keys(self):
        """Test hstore_subtract_keys() function (operator - text[])."""
        result = hstore_subtract_keys("data", "ARRAY['a', 'b']")
        assert result == "data - ARRAY['a', 'b']"

    def test_hstore_subtract_pairs(self):
        """Test hstore_subtract_pairs() function (operator - hstore)."""
        result = hstore_subtract_pairs("data", "'a=>1'::hstore")
        assert result == "data - 'a=>1'::hstore"


class TestHstoreSpecialOperators:
    """Tests for hstore special operator functions."""

    def test_hstore_to_array_operator(self):
        """Test hstore_to_array_operator() function (operator %%)."""
        result = hstore_to_array_operator("data")
        assert result == "%%data"

    def test_hstore_to_matrix_operator(self):
        """Test hstore_to_matrix_operator() function (operator %#)."""
        result = hstore_to_matrix_operator("data")
        assert result == "%#data"

    def test_hstore_record_update(self):
        """Test hstore_record_update() function (operator #=)."""
        result = hstore_record_update("ROW(1,2)", "'f1=>42'::hstore")
        assert result == "ROW(1,2) #= 'f1=>42'::hstore"


class TestHstoreSubscript:
    """Tests for hstore subscript access functions."""

    def test_hstore_subscript_get(self):
        """Test hstore_subscript_get() function."""
        result = hstore_subscript_get("data", "'name'")
        assert result == "data['name']"

    def test_hstore_subscript_set(self):
        """Test hstore_subscript_set() function."""
        result = hstore_subscript_set("data", "'name'", "'value'")
        assert result == "data['name'] = 'value'"
