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

    assert dialect.suggested_extra_column_types()[Point] is PostgresGeometryColumn
    assert dialect.suggested_extra_column_types()[Box] is PostgresAreaGeometryColumn
    assert not hasattr(PostgresGeometryColumn(dialect, "p"), "contains")
    assert hasattr(PostgresAreaGeometryColumn(dialect, "b"), "contains")
