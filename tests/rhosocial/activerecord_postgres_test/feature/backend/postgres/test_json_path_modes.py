# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_json_path_modes.py
"""JSON path rendering modes on PostgreSQL.

``format_json_expression`` overrode the core dispatch entry point and ignored
``expr.mode``: ARROW produced the jsonpath form and FUNCTION was
indistinguishable from AUTO, while ``supports_json_arrow_operators()``
advertised arrows that the implementation never emitted. A probe and its
implementation have to agree, so the mode now decides.

No database is needed: every assertion is on rendered SQL.
"""

# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_json_path_modes.py
import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import Column, JSONExpression
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


def _dialect(version=(16, 2, 1)):
    dialect = PostgresDialect()
    dialect._version = version
    return dialect


def _expr(dialect, mode=None, operation="->>", path="$.a"):
    return JSONExpression(dialect, Column(dialect, "data", table="t"), path, operation, mode=mode)


# ---------------------------------------------------------------------------
# The mode decides
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", [None, "auto", "arrow"])
def test_arrow_modes_use_the_operators(mode):
    """AUTO follows the core contract: arrows when the server has them."""
    sql, _ = _expr(_dialect(), mode).to_sql()
    assert sql == """"t"."data"->>'$.a'"""


def test_function_mode_uses_the_path_language():
    sql, params = _expr(_dialect(), "function").to_sql()
    assert "jsonb_path_query_first" in sql
    assert params == ("$.a",)


def test_the_two_modes_really_differ():
    """Before the fix all four mode values produced the same SQL."""
    arrow, _ = _expr(_dialect(), "arrow").to_sql()
    function, _ = _expr(_dialect(), "function").to_sql()
    assert arrow != function


def test_json_operation_keeps_the_jsonb_result():
    dialect = _dialect()
    sql, _ = JSONExpression(
        dialect, Column(dialect, "data", table="t"), "$.a", "->", mode="function"
    ).to_sql()
    assert " #>> " not in sql
    assert "jsonb_path_query_first" in sql


def test_text_operation_unwraps_the_scalar():
    dialect = _dialect()
    sql, _ = JSONExpression(
        dialect, Column(dialect, "data", table="t"), "$.a", "->>", mode="function"
    ).to_sql()
    assert "#>> '{}'" in sql


def test_path_is_bound_not_inlined():
    """A document-driven path must not be able to change the statement shape."""
    dialect = _dialect()
    sql, params = JSONExpression(
        dialect, Column(dialect, "data", table="t"), "$.a'); DROP TABLE t; --", "->>", mode="function"
    ).to_sql()
    assert "DROP TABLE" not in sql
    assert params == ("$.a'); DROP TABLE t; --",)


def test_alias_is_appended():
    dialect = _dialect()
    expr = JSONExpression(
        dialect, Column(dialect, "data", table="t"), "$.a", "->>", alias="v", mode="arrow"
    )
    assert expr.to_sql()[0] == """"t"."data"->>'$.a' AS "v\""""


# ---------------------------------------------------------------------------
# Version gating
# ---------------------------------------------------------------------------


def test_function_mode_needs_twelve():
    with pytest.raises(UnsupportedFeatureError) as excinfo:
        _expr(_dialect((11, 5, 0)), "function").to_sql()
    assert "12.0" in str(excinfo.value)


def test_arrow_mode_works_before_twelve():
    """Arrows are older than the path language, so PG 11 still has them."""
    sql, _ = _expr(_dialect((11, 5, 0)), "arrow").to_sql()
    assert "->>" in sql


# ---------------------------------------------------------------------------
# Probes that used to lie
# ---------------------------------------------------------------------------


def test_json_table_is_not_claimed():
    """PostgreSQL has no JSON_TABLE; the probe used to claim 12.0+."""
    assert _dialect((16, 2, 1)).supports_json_table() is False


def test_json_table_error_names_the_real_construct():
    dialect = _dialect()
    with pytest.raises(UnsupportedFeatureError) as excinfo:
        dialect.format_json_table_expression(None)
    assert "jsonb_to_recordset" in str(excinfo.value)


def test_jsonb_subscript_is_not_claimed():
    """There is no ``jsonb['key']`` in PostgreSQL."""
    assert _dialect((16, 2, 1)).supports_jsonb_subscript() is False


def test_each_probe_is_defined_exactly_once_in_the_mixin_hierarchy():
    """Two definitions with two version gates used to race through the MRO."""
    from rhosocial.activerecord.backend.impl.postgres import mixins as pg_mixins

    for probe in ("supports_jsonb_subscript", "supports_infinity_numeric_infinity_jsonb"):
        owners = [
            name
            for name in dir(pg_mixins)
            if isinstance(getattr(pg_mixins, name, None), type)
            and probe in vars(getattr(pg_mixins, name))
        ]
        assert owners == ["PostgresJSONBEnhancedMixin"], f"{probe} defined by {owners}"


def test_arrow_probe_matches_what_is_actually_emitted():
    dialect = _dialect()
    assert dialect.supports_json_arrow_operators() is True
    assert "->>" in _expr(dialect, "auto").to_sql()[0]
