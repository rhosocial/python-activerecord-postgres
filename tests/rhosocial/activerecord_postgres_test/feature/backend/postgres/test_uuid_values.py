# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_uuid_values.py
"""UUID value expressions on PostgreSQL.

PostgreSQL has two routes to a generated UUID — the built-in
``gen_random_uuid()`` on 13.0+ and ``uuid_generate_v4()`` from uuid-ossp —
so version handling is the substance here. A probe that answers True for SQL
the server will reject is worse than one that says no.

No database is needed: every assertion is on the SQL a dialect renders.
"""

# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_uuid_values.py
import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import (
    Literal,
    UUIDCastExpression,
    UUIDConstantExpression,
    UUIDGenerationExpression,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


def _dialect(version=(16, 2, 1)):
    dialect = PostgresDialect()
    dialect._version = version
    return dialect


# ---------------------------------------------------------------------------
# Version-aware generation
# ---------------------------------------------------------------------------


def test_modern_server_uses_the_builtin_function():
    assert UUIDGenerationExpression(_dialect((16, 2, 1))).to_sql() == ("gen_random_uuid()", ())


def test_generation_carries_its_alias():
    assert UUIDGenerationExpression(_dialect(), alias="u").to_sql() == (
        'gen_random_uuid() AS "u"',
        (),
    )


def test_builtin_arrives_in_thirteen():
    assert _dialect((13, 0, 0)).supports_uuid_generation() is True
    assert UUIDGenerationExpression(_dialect((13, 0, 0))).to_sql() == ("gen_random_uuid()", ())


def test_twelve_without_the_extension_reports_no_generation():
    """``uuid_generate_v4()`` needs uuid-ossp; without it the answer is no."""
    dialect = _dialect((12, 4, 0))
    assert dialect.supports_uuid_generation() is False


def test_twelve_without_the_extension_refuses_with_a_route_forward():
    dialect = _dialect((12, 4, 0))
    with pytest.raises(UnsupportedFeatureError) as excinfo:
        UUIDGenerationExpression(dialect).to_sql()
    message = str(excinfo.value)
    assert "uuid-ossp" in message
    assert "UUIDMixin" in message


def test_old_server_with_the_extension_falls_back_to_ossp(monkeypatch):
    dialect = _dialect((12, 4, 0))
    monkeypatch.setattr(
        type(dialect), "check_extension_feature", lambda *a, **k: True, raising=True
    )
    assert dialect.supports_uuid_generation() is True
    assert UUIDGenerationExpression(dialect).to_sql() == ("uuid_generate_v4()", ())


def test_generation_consults_the_extension_probe(monkeypatch):
    """The merged probe must go through the extension's own narrower probe.

    The two used to share the name ``supports_uuid_generation`` while asking
    different questions, so the MRO picked an answer by class order. The
    extension question now has its own name, and this asserts the merge still
    consults it — otherwise a 12.4 server with uuid-ossp would report no
    generation.
    """
    from rhosocial.activerecord.backend.impl.postgres.mixins import (
        PostgresUUIDMixin,
        PostgresUuidOssMixin,
    )

    dialect = _dialect((12, 4, 0))
    # Patch where the method is defined, not on the dialect's own class: the
    # previous version patched PostgresDialect, so the direct mixin call below
    # still reached the real implementation and read the extension state.
    monkeypatch.setattr(
        PostgresUuidOssMixin,
        "supports_uuid_ossp_extension",
        lambda self: True,
        raising=True,
    )
    assert PostgresUUIDMixin.supports_uuid_generation(dialect) is True
    assert dialect.supports_uuid_ossp_extension() is True


def test_the_dialect_itself_resolves_one_answer(monkeypatch):
    dialect = _dialect((12, 4, 0))
    monkeypatch.setattr(
        type(dialect), "check_extension_feature", lambda *a, **k: True, raising=True
    )
    assert dialect.supports_uuid_generation() is True


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def test_capability_probes_reflect_the_table():
    dialect = _dialect()
    assert dialect.supports_uuid_constant() is True
    assert dialect.supports_uuid_cast() is True


@pytest.mark.parametrize(
    "which, expected",
    [
        ("nil", "'00000000-0000-0000-0000-000000000000'::uuid"),
        ("max", "'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid"),
    ],
)
def test_constants_are_casted_literals(which, expected):
    assert UUIDConstantExpression(_dialect(), which).to_sql() == (expected, ())


def test_constant_alias_lands_after_the_cast():
    assert UUIDConstantExpression(_dialect(), "nil", alias="n").to_sql() == (
        "'00000000-0000-0000-0000-000000000000'::uuid AS \"n\"",
        (),
    )


# ---------------------------------------------------------------------------
# Cast
# ---------------------------------------------------------------------------


def test_cast_uses_postgres_shorthand():
    """PostgreSQL has ``::uuid``, so it needs no CAST() wrapper."""
    dialect = _dialect()
    expr = UUIDCastExpression(dialect, Literal(dialect, "not-a-uuid"))
    assert expr.to_sql() == ("%s::uuid", ("not-a-uuid",))


def test_cast_preserves_nested_parameters():
    dialect = _dialect()
    inner = UUIDCastExpression(dialect, UUIDConstantExpression(dialect, "nil"))
    assert inner.to_sql() == ("'00000000-0000-0000-0000-000000000000'::uuid::uuid", ())
