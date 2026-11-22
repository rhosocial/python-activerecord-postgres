# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/types/test_column_operations.py
"""The server-specific operations on PostgreSQL's own column classes.

Each operation is a thin façade over a function/operator node this backend
already has. These tests assert the SQL it renders: adding an operation is a
deliberate edit here, not a name that quietly promises more than the server
can execute. Live round-trips are done against the configured scenarios; the
render assertions are the part that needs no server.
"""

import pytest

from rhosocial.activerecord.backend.impl.postgres.column_types import (
    PostgresAreaGeometryColumn,
    PostgresGeometryColumn,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


@pytest.fixture
def dialect():
    return PostgresDialect(version=(16, 0, 0))


@pytest.fixture
def geometry(dialect):
    return PostgresGeometryColumn(dialect, "g")


# ---------------------------------------------------------------------------
# Geometry: PostgreSQL's built-in geometric operators (no extension needed)
# ---------------------------------------------------------------------------


def test_distance_is_available_on_every_geometry(geometry):
    """``<->`` answers on point, line, lseg, box, path, polygon and circle."""
    expr = geometry.distance_to("(1,1)")
    assert expr.to_sql() == ('"g" <-> %s', ("(1,1)",))


@pytest.mark.parametrize(
    "method, operator",
    [
        ("contains", "@>"),
        ("contained_by", "<@"),
        ("overlaps", "&&"),
    ],
)
def test_area_geometry_operators_against_a_literal(dialect, method, operator):
    column = PostgresAreaGeometryColumn(dialect, "g")
    expr = getattr(column, method)("(1,1)")
    assert expr.to_sql() == (f'"g" {operator} %s', ("(1,1)",))


@pytest.mark.parametrize(
    "method, operator",
    [
        ("distance_to", "<->"),
        ("contains", "@>"),
        ("contained_by", "<@"),
        ("overlaps", "&&"),
    ],
)
def test_geometry_operators_against_a_column(geometry, dialect, method, operator):
    if method == "distance_to":
        source, other = geometry, PostgresGeometryColumn(dialect, "h")
    else:
        source = PostgresAreaGeometryColumn(dialect, "g")
        other = PostgresAreaGeometryColumn(dialect, "h")
    expr = getattr(source, method)(other)
    assert expr.to_sql() == (f'"g" {operator} "h"', ())


def test_the_area_operators_are_not_offered_on_a_point(dialect):
    """A point has no ``@>``; resolving Point must not hand out a false one."""
    from rhosocial.activerecord.backend.impl.postgres.type_values import Box, Point

    assert dialect.suggest_extra_column_types().table[Point] is PostgresGeometryColumn
    assert dialect.suggest_extra_column_types().table[Box] is PostgresAreaGeometryColumn
    assert not hasattr(PostgresGeometryColumn(dialect, "p"), "contains")
    assert hasattr(PostgresAreaGeometryColumn(dialect, "b"), "contains")


# ---------------------------------------------------------------------------
# Bit strings: built-in bitwise operators and length/count functions
# ---------------------------------------------------------------------------


@pytest.fixture
def bits(dialect):
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgresBitStringColumn,
    )

    return PostgresBitStringColumn(dialect, "b")


@pytest.mark.parametrize(
    "method, operator",
    [("bit_and", "&"), ("bit_or", "|"), ("bit_xor", "#")],
)
def test_bitwise_binary_operators(bits, dialect, method, operator):
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgresBitStringColumn,
    )

    expr = getattr(bits, method)(PostgresBitStringColumn(dialect, "h"))
    assert expr.to_sql() == (f'"b" {operator} "h"', ())


def test_bitwise_not_and_length(bits):
    assert bits.bit_not().to_sql() == ('~ "b"', ())
    assert bits.bit_length().to_sql() == ('BIT_LENGTH("b")', ())
    assert bits.bit_count().to_sql() == ('BIT_COUNT("b")', ())


@pytest.mark.parametrize("method, operator", [("shift_left", "<<"), ("shift_right", ">>")])
def test_bitwise_shifts(bits, method, operator):
    assert getattr(bits, method)(2).to_sql() == (f'"b" {operator} %s', (2,))


# ---------------------------------------------------------------------------
# Ranges: native operators and functions, gated on the range capability
# ---------------------------------------------------------------------------


@pytest.fixture
def ranges(dialect):
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgresRangeColumn,
    )

    return PostgresRangeColumn(dialect, "r")


@pytest.mark.parametrize(
    "method, operator",
    [("contains", "@>"), ("contained_by", "<@"), ("overlaps", "&&")],
)
def test_range_operators(ranges, method, operator):
    assert getattr(ranges, method)(5).to_sql() == (f'"r" {operator} %s', (5,))


@pytest.mark.parametrize(
    "method, sql",
    [("is_empty", 'ISEMPTY("r")'), ("lower", 'LOWER("r")'), ("upper", 'UPPER("r")')],
)
def test_range_functions(ranges, method, sql):
    assert getattr(ranges, method)().to_sql() == (sql, ())


@pytest.mark.parametrize("method", ["contains", "contained_by", "overlaps"])
def test_range_operators_fail_on_a_server_without_them(method):
    from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgresRangeColumn,
    )

    old = PostgresDialect(version=(9, 1, 0))
    column = PostgresRangeColumn(old, "r")
    with pytest.raises(UnsupportedFeatureError, match="supports_range_operators"):
        getattr(column, method)(5)


@pytest.mark.parametrize("method", ["is_empty", "lower", "upper"])
def test_range_functions_fail_on_a_server_without_them(method):
    from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgresRangeColumn,
    )

    old = PostgresDialect(version=(9, 1, 0))
    column = PostgresRangeColumn(old, "r")
    with pytest.raises(UnsupportedFeatureError, match="supports_range_functions"):
        getattr(column, method)()


# ---------------------------------------------------------------------------
# Multiranges: the same operators on a native multirange column
# ---------------------------------------------------------------------------


@pytest.fixture
def multiranges(dialect):
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgresMultirangeColumn,
    )

    return PostgresMultirangeColumn(dialect, "m")


@pytest.mark.parametrize(
    "method, operator",
    [("contains", "@>"), ("contained_by", "<@"), ("overlaps", "&&")],
)
def test_multirange_operators(multiranges, method, operator):
    assert getattr(multiranges, method)(5).to_sql() == (f'"m" {operator} %s', (5,))


@pytest.mark.parametrize("method", ["contains", "contained_by", "overlaps"])
def test_multirange_operators_fail_without_the_capability(method):
    from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgresMultirangeColumn,
    )

    old = PostgresDialect(version=(9, 1, 0))
    column = PostgresMultirangeColumn(old, "m")
    with pytest.raises(UnsupportedFeatureError, match="supports_multirange"):
        getattr(column, method)(5)


# ---------------------------------------------------------------------------
# Text search: the match operator and ranking function
# ---------------------------------------------------------------------------


@pytest.fixture
def tsvector(dialect):
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgresTextSearchColumn,
    )

    return PostgresTextSearchColumn(dialect, "tv")


def test_text_search_matches(tsvector):
    assert tsvector.matches("foo & bar").to_sql() == ('"tv" @@ %s', ("foo & bar",))


def test_text_search_rank(tsvector):
    assert tsvector.rank("foo").to_sql() == ('TS_RANK("tv", %s)', ("foo",))


# ---------------------------------------------------------------------------
# pgvector: distance operators, gated on the extension's capability
# ---------------------------------------------------------------------------


@pytest.fixture
def vectors(dialect, monkeypatch):
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgresVectorColumn,
    )

    monkeypatch.setattr(dialect, "supports_pgvector_similarity_search", lambda: True)
    return PostgresVectorColumn(dialect, "v")


@pytest.mark.parametrize(
    "method, operator",
    [
        ("l2_distance", "<->"),
        ("cosine_distance", "<=>"),
        ("inner_product", "<#>"),
    ],
)
def test_vector_distance_operators(vectors, method, operator):
    sql, params = getattr(vectors, method)([1.0, 2.0]).to_sql()
    assert sql == f'"v" {operator} %s::VECTOR(2)'
    assert params == ("[1.0, 2.0]",)


def test_vector_cosine_similarity(vectors):
    sql, params = vectors.cosine_similarity([1.0, 2.0]).to_sql()
    assert sql == '%s - ("v" <=> %s::VECTOR(2))'
    assert params == (1, "[1.0, 2.0]")


@pytest.mark.parametrize(
    "method", ["l2_distance", "cosine_distance", "inner_product", "cosine_similarity"]
)
def test_vector_ops_fail_when_the_extension_is_not_available(dialect, method):
    from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgresVectorColumn,
    )

    column = PostgresVectorColumn(dialect, "v")
    with pytest.raises(UnsupportedFeatureError, match="supports_pgvector_similarity_search"):
        getattr(column, method)([1.0, 2.0])


# ---------------------------------------------------------------------------
# hstore: key existence, gated on the extension's operators
# ---------------------------------------------------------------------------


@pytest.fixture
def hstore(dialect, monkeypatch):
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgresHstoreColumn,
    )

    monkeypatch.setattr(dialect, "supports_hstore_operators", lambda: True)
    return PostgresHstoreColumn(dialect, "h")


@pytest.mark.parametrize("method, name", [("exist", "EXIST"), ("defined", "DEFINED")])
def test_hstore_key_predicates(hstore, method, name):
    sql, params = getattr(hstore, method)("k").to_sql()
    assert sql == f'{name}("h", %s)'
    assert params == ("k",)


@pytest.mark.parametrize("method", ["exist", "defined"])
def test_hstore_ops_fail_when_the_extension_is_not_available(dialect, method):
    from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgresHstoreColumn,
    )

    column = PostgresHstoreColumn(dialect, "h")
    with pytest.raises(UnsupportedFeatureError, match="supports_hstore_operators"):
        getattr(column, method)("k")


# ---------------------------------------------------------------------------
# PostGIS: ST_* on the wrapper's column class, gated on the extension
# ---------------------------------------------------------------------------


@pytest.fixture
def postgis(dialect, monkeypatch):
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgisGeometryColumn,
    )

    monkeypatch.setattr(dialect, "supports_postgis_spatial_functions", lambda: True)
    return PostgisGeometryColumn(dialect, "g")


@pytest.mark.parametrize(
    "method, name",
    [
        ("st_contains", "ST_CONTAINS"),
        ("st_within", "ST_WITHIN"),
        ("st_intersects", "ST_INTERSECTS"),
        ("st_distance", "ST_DISTANCE"),
    ],
)
def test_postgis_predicates_and_distance(postgis, dialect, method, name):
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgisGeometryColumn,
    )

    other = PostgisGeometryColumn(dialect, "h")
    sql, params = getattr(postgis, method)(other).to_sql()
    assert sql == f'{name}("g", "h")'
    assert params == ()


@pytest.mark.parametrize("method, name", [("st_area", "ST_AREA"), ("st_length", "ST_LENGTH")])
def test_postgis_measurements(postgis, method, name):
    assert getattr(postgis, method)().to_sql() == (f'{name}("g")', ())


@pytest.mark.parametrize(
    "method",
    ["st_distance", "st_contains", "st_within", "st_intersects", "st_area", "st_length"],
)
def test_postgis_ops_fail_without_the_extension(dialect, method):
    from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgisGeometryColumn,
    )

    column = PostgisGeometryColumn(dialect, "g")
    args = () if method in ("st_area", "st_length") else (column,)
    with pytest.raises(UnsupportedFeatureError, match="supports_postgis_spatial_functions"):
        getattr(column, method)(*args)


def test_the_postgis_wrapper_resolves_to_the_extension_class(dialect):
    from rhosocial.activerecord.backend.impl.postgres.column_types import (
        PostgisGeometryColumn,
    )
    from rhosocial.activerecord.backend.impl.postgres.type_values import (
        Point,
        PostgresGeometry,
    )

    assert dialect.suggest_extra_column_types().table[PostgresGeometry] is PostgisGeometryColumn
    assert dialect.suggest_extra_column_types().table[Point] is not PostgisGeometryColumn
