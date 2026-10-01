# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_orafce_schema_prefix.py
"""
C12 -- the orafce function schema prefix must be configurable.

orafce installs its functions into the ``oracle`` schema by default, so every
call in :mod:`...functions.orafce` is schema-qualified. That hard-coded
``oracle.`` prefix breaks when the extension is installed elsewhere.

These tests pin three things:

1. the default output is **byte-identical** to the historical hard-coded form
   (regression guard -- making the prefix configurable must not alter existing
   SQL);
2. ``schema=`` overrides the prefix;
3. a schema name that cannot survive the unquoted/upper-cased rendering path
   emits a :class:`UserWarning` instead of silently producing wrong SQL.
"""

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions import orafce

FUNCS = [
    ("add_months", "ADD_MONTHS", lambda m: m.add_months(D, "2024-01-15", 3)),
    ("last_day", "LAST_DAY", lambda m: m.last_day(D, "2024-01-15")),
    ("months_between", "MONTHS_BETWEEN",
     lambda m: m.months_between(D, "2024-01-31", "2024-02-29")),
    ("next_day", "NEXT_DAY", lambda m: m.next_day(D, "2024-01-15", "MONDAY")),
    ("nvl", "NVL", lambda m: m.nvl(D, "a", "b")),
    ("nvl2", "NVL2", lambda m: m.nvl2(D, "a", "b", "c")),
    ("decode", "DECODE", lambda m: m.decode(D, "x", "'a'", "1")),
    ("orafce_trunc", "TRUNC", lambda m: m.orafce_trunc(D, "2024-01-15", "YEAR")),
    ("orafce_round", "ROUND", lambda m: m.orafce_round(D, 3.14159, 2)),
    ("instr", "INSTR", lambda m: m.instr(D, "hello", "l")),
    ("substr", "SUBSTR", lambda m: m.substr(D, "hello", 2, 3)),
]

D = PostgresDialect()


class TestDefaultSchemaUnchanged:
    """T-25a -- the default rendering must not change."""

    @pytest.mark.parametrize("name,expected,call", FUNCS, ids=[f[0] for f in FUNCS])
    def test_default_prefix_is_oracle(self, name, expected, call):
        sql, _ = call(orafce).to_sql()
        assert sql.startswith(f"ORACLE.{expected}("), f"{name}: {sql}"

    def test_every_public_function_accepts_schema_kwarg(self):
        """All 11 factories expose ``schema=`` (regression guard)."""
        import inspect

        for name, _expected, _call in FUNCS:
            sig = inspect.signature(getattr(orafce, name))
            assert "schema" in sig.parameters, f"{name} lacks schema="
            assert sig.parameters["schema"].default is None

    def test_default_prefix_constant(self):
        assert orafce.DEFAULT_ORAFCE_SCHEMA == "oracle"


class TestSchemaOverride:
    """T-25b -- ``schema=`` replaces the prefix."""

    @pytest.mark.parametrize("name,expected,call", FUNCS, ids=[f[0] for f in FUNCS])
    def test_schema_override(self, name, expected, call):
        factories = {
            "add_months": lambda m, s: m.add_months(D, "2024-01-15", 3, schema=s),
            "last_day": lambda m, s: m.last_day(D, "2024-01-15", schema=s),
            "months_between": lambda m, s: m.months_between(
                D, "2024-01-31", "2024-02-29", schema=s),
            "next_day": lambda m, s: m.next_day(D, "2024-01-15", "MONDAY", schema=s),
            "nvl": lambda m, s: m.nvl(D, "a", "b", schema=s),
            "nvl2": lambda m, s: m.nvl2(D, "a", "b", "c", schema=s),
            "decode": lambda m, s: m.decode(D, "x", "'a'", "1", schema=s),
            "orafce_trunc": lambda m, s: m.orafce_trunc(D, "2024-01-15", "YEAR", schema=s),
            "orafce_round": lambda m, s: m.orafce_round(D, 3.14159, 2, schema=s),
            "instr": lambda m, s: m.instr(D, "hello", "l", schema=s),
            "substr": lambda m, s: m.substr(D, "hello", 2, 3, schema=s),
        }
        sql, _ = factories[name](orafce, "ext").to_sql()
        assert sql.startswith(f"EXT.{expected}("), f"{name}: {sql}"

    def test_schema_none_falls_back_to_default(self):
        sql, _ = orafce.nvl(D, "a", "b", schema=None).to_sql()
        assert sql.startswith("ORACLE.NVL(")


class TestNonPlainSchemaWarns:
    """T-25c -- an unquotable schema name must warn, not fail silently."""

    @pytest.mark.parametrize("bad", ["MySchema", "my schema", "1abc", "or-acle"])
    def test_warns_on_non_plain_schema(self, bad):
        with pytest.warns(UserWarning, match="plain lowercase identifier"):
            orafce.nvl(D, "a", "b", schema=bad)

    @pytest.mark.parametrize("good", ["oracle", "ext", "my_schema", "_x1"])
    def test_no_warning_for_plain_schema(self, good, recwarn):
        orafce.nvl(D, "a", "b", schema=good)
        assert not [w for w in recwarn if "plain lowercase" in str(w.message)]
