# tests/rhosocial/activerecord_postgres_test/feature/backend/test_type_compatibility.py
"""PostgreSQL type compatibility tests.

This module tests the extended type compatibility checking
functionality including warnings and suggestions.
"""
import pytest
import warnings

from rhosocial.activerecord.backend.impl.postgres.type_compatibility import (
    WARNED_CASTS,
    INTERMEDIATE_SUGGESTIONS,
    TYPE_CATEGORIES,
    check_cast_compatibility,
    get_type_category,
    get_intermediate_type,
)


class TestTimezoneConversionWarnings:
    """Test timezone conversion warnings."""

    def test_timestamptz_to_time_warning(self):
        """Test timestamptz -> time produces warning (date lost)."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('timestamptz', 'time')
            assert result is True
            assert len(w) == 1

    def test_timestamp_to_time_warning(self):
        """Test timestamp -> time produces warning (date lost)."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('timestamp', 'time')
            assert result is True
            assert len(w) == 1

    def test_timestamptz_to_timestamp_no_warning(self):
        """Test timestamptz -> timestamp is direct compatible (no warning)."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('timestamptz', 'timestamp')
            assert result is True
            assert len(w) == 0  # No warning - direct compatible


class TestNumericPrecisionWarnings:
    """Test numeric precision loss warnings."""

    def test_double_precision_to_real_warning(self):
        """Test double precision -> real produces warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('double precision', 'real')
            assert result is True
            assert len(w) == 1
            assert 'precision' in str(w[0].message).lower()

    def test_bigint_to_integer_warning(self):
        """Test bigint -> integer produces warning with suggestion."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('bigint', 'integer')
            assert result is True
            assert len(w) == 1
            assert 'numeric' in str(w[0].message)

    def test_integer_to_smallint_warning(self):
        """Test integer -> smallint produces warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('integer', 'smallint')
            assert result is True
            assert len(w) == 1


class TestStringTruncationWarnings:
    """Test string truncation warnings.

    Note: Most string conversions (text->varchar, text->char, varchar->char)
    are in DIRECT_COMPATIBLE_CASTS, so they don't produce warnings.
    """

    def test_text_to_varchar_no_warning(self):
        """Test text -> varchar is direct compatible (no warning)."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('text', 'varchar')
            assert result is True
            assert len(w) == 0  # No warning - direct compatible

    def test_text_to_char_no_warning(self):
        """Test text -> char is direct compatible (no warning)."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('text', 'char')
            assert result is True
            assert len(w) == 0  # No warning - direct compatible

    def test_varchar_to_char_no_warning(self):
        """Test varchar -> char is direct compatible (no warning)."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('varchar', 'char')
            assert result is True
            assert len(w) == 0  # No warning - direct compatible


class TestJSONConversionWarnings:
    """Test JSON conversion warnings."""

    def test_jsonb_to_varchar_warning(self):
        """Test jsonb -> varchar produces warning with suggestion."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('jsonb', 'varchar')
            assert result is True
            assert len(w) == 1
            assert 'text' in str(w[0].message)

    def test_json_to_varchar_warning(self):
        """Test json -> varchar produces warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('json', 'varchar')
            assert result is True
            assert len(w) == 1


class TestUUIDConversionWarnings:
    """Test UUID conversion warnings."""

    def test_uuid_to_varchar_warning(self):
        """Test uuid -> varchar produces warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('uuid', 'varchar')
            assert result is True
            assert len(w) == 1

    def test_uuid_to_char_warning(self):
        """Test uuid -> char produces warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('uuid', 'char')
            assert result is True
            assert len(w) == 1


class TestIntermediateSuggestions:
    """Test intermediate type suggestions."""

    def test_bigint_to_integer_suggestion(self):
        """Test bigint -> integer has numeric suggestion."""
        suggestion = get_intermediate_type('bigint', 'integer')
        assert suggestion == 'numeric'

    def test_jsonb_to_varchar_suggestion(self):
        """Test jsonb -> varchar has text suggestion."""
        suggestion = get_intermediate_type('jsonb', 'varchar')
        assert suggestion == 'text'

    def test_money_to_float_suggestion(self):
        """Test money -> float has numeric suggestion."""
        suggestion = get_intermediate_type('money', 'double precision')
        assert suggestion == 'numeric'


class TestTypeCategories:
    """Test type category classification."""

    def test_multirange_types(self):
        """Test multirange type categories."""
        multirange_types = [
            'int4multirange', 'int8multirange', 'nummultirange',
            'tsmultirange', 'tstzmultirange', 'datemultirange'
        ]
        for typ in multirange_types:
            assert get_type_category(typ) == 'R', f"{typ} should be category R"

    def test_pseudo_types(self):
        """Test pseudo type categories."""
        pseudo_types = [
            'anyelement', 'anyarray', 'anynonarray', 'anyenum',
            'anyrange', 'anymultirange', 'void', 'trigger', 'event_trigger'
        ]
        for typ in pseudo_types:
            assert get_type_category(typ) == 'P', f"{typ} should be category P"

    def test_json_types(self):
        """Test JSON type categories."""
        json_types = ['json', 'jsonb', 'jsonpath', 'uuid']
        for typ in json_types:
            assert get_type_category(typ) == 'U', f"{typ} should be category U"

    def test_text_search_types(self):
        """Test text search type categories."""
        assert get_type_category('tsvector') == 'U'
        assert get_type_category('tsquery') == 'U'

    def test_object_identifier_types(self):
        """Test object identifier type categories."""
        oid_types = ['oid', 'regclass', 'regtype', 'regproc', 'xid', 'cid', 'tid']
        for typ in oid_types:
            assert get_type_category(typ) == 'N', f"{typ} should be category N"


class TestWarnedCastsSet:
    """Test WARNED_CASTS set contents."""

    def test_time_casts_present(self):
        """Test time conversion casts are in WARNED_CASTS."""
        assert ('timestamptz', 'time') in WARNED_CASTS
        assert ('timestamp', 'time') in WARNED_CASTS

    def test_numeric_precision_casts_present(self):
        """Test numeric precision casts are in WARNED_CASTS."""
        assert ('double precision', 'real') in WARNED_CASTS
        assert ('bigint', 'integer') in WARNED_CASTS
        assert ('integer', 'smallint') in WARNED_CASTS

    def test_json_casts_present(self):
        """Test JSON conversion casts are in WARNED_CASTS."""
        assert ('jsonb', 'varchar') in WARNED_CASTS
        assert ('json', 'varchar') in WARNED_CASTS

    def test_uuid_casts_present(self):
        """Test UUID conversion casts are in WARNED_CASTS."""
        assert ('uuid', 'varchar') in WARNED_CASTS
        assert ('uuid', 'char') in WARNED_CASTS

    def test_timezone_casts_not_in_warned(self):
        """Test timezone casts are NOT in WARNED_CASTS (they're direct compatible)."""
        # These are in DIRECT_COMPATIBLE_CASTS, not WARNED_CASTS
        assert ('timestamptz', 'timestamp') not in WARNED_CASTS
        assert ('timestamptz', 'date') not in WARNED_CASTS

    def test_string_casts_not_in_warned(self):
        """Test string casts are NOT in WARNED_CASTS (they're direct compatible)."""
        # These are in DIRECT_COMPATIBLE_CASTS, not WARNED_CASTS
        assert ('text', 'varchar') not in WARNED_CASTS
        assert ('text', 'char') not in WARNED_CASTS
        assert ('varchar', 'char') not in WARNED_CASTS


class TestIntermediateSuggestionsDict:
    """Test INTERMEDIATE_SUGGESTIONS dictionary contents."""

    def test_numeric_suggestions_present(self):
        """Test numeric conversion suggestions are present."""
        assert ('bigint', 'integer') in INTERMEDIATE_SUGGESTIONS
        assert ('bigint', 'smallint') in INTERMEDIATE_SUGGESTIONS
        assert ('integer', 'smallint') in INTERMEDIATE_SUGGESTIONS

    def test_json_suggestions_present(self):
        """Test JSON conversion suggestions are present."""
        assert ('jsonb', 'varchar') in INTERMEDIATE_SUGGESTIONS
        assert ('json', 'varchar') in INTERMEDIATE_SUGGESTIONS

    def test_time_suggestions_not_present(self):
        """Test time conversion suggestions are NOT present (direct compatible)."""
        # These are direct compatible, no intermediate needed
        assert ('timestamptz', 'timestamp') not in INTERMEDIATE_SUGGESTIONS
        assert ('timestamptz', 'date') not in INTERMEDIATE_SUGGESTIONS


class TestNoWarningScenarios:
    """Test scenarios that should NOT produce warnings."""

    def test_same_type_no_warning(self):
        """Test same type conversion produces no warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('integer', 'integer')
            assert result is True
            assert len(w) == 0

    def test_numeric_widening_no_warning(self):
        """Test numeric widening produces no warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('integer', 'bigint')
            assert result is True
            assert len(w) == 0

    def test_jsonb_to_json_no_warning(self):
        """Test jsonb -> json produces no warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('jsonb', 'json')
            assert result is True
            assert len(w) == 0

    def test_varchar_to_text_no_warning(self):
        """Test varchar -> text produces no warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = check_cast_compatibility('varchar', 'text')
            assert result is True
            assert len(w) == 0
