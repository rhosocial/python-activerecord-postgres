# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/test_native_geometric.py
"""Native tests for PostgreSQL's built-in geometric types.

The seven wrapper types are checked together: each column is read through
``col::text`` in one SELECT, which is the server's canonical geometric text
(and which drops trailing ``.0`` components).  Decoding happens with the
wrapper's own target class; re-writing server text must not change it.
"""

import pytest

from rhosocial.activerecord.backend.impl.postgres.adapters.geometric import (
    PostgresGeometryAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.type_values import (
    Box,
    Circle,
    Line,
    LineSegment,
    Path,
    Point,
    Polygon,
)
from rhosocial.activerecord_postgres_test.feature.backend.adapters.native.helpers import (
    insert_row,
    insert_sql,
    read_raw,
    update_cell,
)

TABLE = "native_geometric"

WRAPPER = PostgresGeometryAdapter()

_RAW_COLUMNS = (
    "point_value",
    "line_value",
    "lseg_value",
    "box_value",
    "path_value",
    "polygon_value",
    "circle_value",
)


class TestGeometric:
    TABLES = (TABLE,)

    def test_seven_shapes_round_trip_and_raw_text(self, native_backend):
        """S1: all seven shapes decode, and their text has the documented form."""
        backend = native_backend
        values = {
            "point_value": WRAPPER.to_database(Point(1.5, 2.5), str),
            "line_value": WRAPPER.to_database(Line(1.0, -1.0, 0.0), str),
            "lseg_value": WRAPPER.to_database(
                LineSegment(Point(0.0, 0.0), Point(1.5, 1.5)), str
            ),
            "box_value": WRAPPER.to_database(
                Box(Point(1.5, 1.5), Point(0.0, 0.0)), str
            ),
            "path_value": WRAPPER.to_database(
                Path([Point(0.5, 0.0), Point(1.5, 1.0), Point(2.5, 0.0)]), str
            ),
            "polygon_value": WRAPPER.to_database(
                Polygon(
                    [
                        Point(0.5, 0.5),
                        Point(10.5, 0.5),
                        Point(10.5, 10.5),
                        Point(0.5, 0.5),
                    ]
                ),
                str,
            ),
            "circle_value": WRAPPER.to_database(Circle(Point(1.5, 2.5), 3.5), str),
        }
        casts = {name: name[: -len("_value")] for name in values}
        row_id = insert_row(backend, TABLE, values, casts)

        select = ", ".join(
            "{}::text AS {}".format(name, name) for name in _RAW_COLUMNS
        )
        row = backend.fetch_one(
            "SELECT {} FROM {} WHERE id = %s".format(select, TABLE), (row_id,)
        )
        assert row["point_value"] == "(1.5,2.5)"
        assert row["line_value"] == "{1,-1,0}"
        assert row["lseg_value"] == "[(0,0),(1.5,1.5)]"
        assert row["box_value"] == "(1.5,1.5),(0,0)"
        assert row["path_value"] == "[(0.5,0),(1.5,1),(2.5,0)]"
        assert row["polygon_value"] == (
            "((0.5,0.5),(10.5,0.5),(10.5,10.5),(0.5,0.5))"
        )
        assert row["circle_value"] == "<(1.5,2.5),3.5>"

        assert WRAPPER.from_database(row["point_value"], Point) == Point(1.5, 2.5)
        assert WRAPPER.from_database(row["line_value"], Line) == Line(1.0, -1.0, 0.0)
        assert WRAPPER.from_database(row["lseg_value"], LineSegment) == LineSegment(
            Point(0.0, 0.0), Point(1.5, 1.5)
        )
        assert WRAPPER.from_database(row["box_value"], Box) == Box(
            Point(1.5, 1.5), Point(0.0, 0.0)
        )
        assert WRAPPER.from_database(row["path_value"], Path) == Path(
            [Point(0.5, 0.0), Point(1.5, 1.0), Point(2.5, 0.0)]
        )
        assert WRAPPER.from_database(row["polygon_value"], Polygon) == Polygon(
            [
                Point(0.5, 0.5),
                Point(10.5, 0.5),
                Point(10.5, 10.5),
                Point(0.5, 0.5),
            ]
        )
        assert WRAPPER.from_database(row["circle_value"], Circle) == Circle(
            Point(1.5, 2.5), 3.5
        )

    def test_open_and_closed_paths_keep_their_brackets(self, native_backend):
        """The path's bracket form is data, not formatting."""
        backend = native_backend
        open_value = Path([Point(0.5, 0.0), Point(1.5, 1.0)], is_closed=False)
        closed_value = Path([Point(0.5, 0.0), Point(1.5, 1.0)], is_closed=True)

        open_row = insert_row(
            backend,
            TABLE,
            {"path_value": WRAPPER.to_database(open_value, str)},
            {"path_value": "path"},
        )
        closed_row = insert_row(
            backend,
            TABLE,
            {"path_value": WRAPPER.to_database(closed_value, str)},
            {"path_value": "path"},
        )
        assert read_raw(backend, TABLE, "path_value", open_row) == "[(0.5,0),(1.5,1)]"
        assert read_raw(backend, TABLE, "path_value", closed_row) == "((0.5,0),(1.5,1))"
        assert WRAPPER.from_database(
            read_raw(backend, TABLE, "path_value", closed_row), Path
        ).is_closed

    def test_rewriting_server_text_does_not_drift(self, native_backend):
        """S2: the server canonical text is stable across a write/read/write."""
        backend = native_backend
        value = Box(Point(1.5, 1.5), Point(0.0, 0.0))
        row_id = insert_row(
            backend,
            TABLE,
            {"box_value": WRAPPER.to_database(value, str)},
            {"box_value": "box"},
        )
        raw = read_raw(backend, TABLE, "box_value", row_id)
        decoded = WRAPPER.from_database(raw, Box)
        assert decoded == value

        update_cell(
            backend, TABLE, "box_value", row_id,
            WRAPPER.to_database(decoded, str), "box",
        )
        assert read_raw(backend, TABLE, "box_value", row_id) == raw

    def test_external_geometric_literals_are_parsed(self, native_backend):
        """S3: native SQL literals read back through the wrapper adapter."""
        backend = native_backend
        row_id = insert_sql(
            backend,
            "INSERT INTO {} (point_value, circle_value, polygon_value) VALUES "
            "('(1,2)'::point, '<(1,2),3>'::circle, "
            "'((0,0),(1,0),(1,1),(0,0))'::polygon) RETURNING id".format(TABLE),
        )
        assert read_raw(backend, TABLE, "point_value", row_id) == "(1,2)"
        assert read_raw(backend, TABLE, "circle_value", row_id) == "<(1,2),3>"
        assert read_raw(backend, TABLE, "polygon_value", row_id) == (
            "((0,0),(1,0),(1,1),(0,0))"
        )
        assert WRAPPER.from_database("(1,2)", Point) == Point(1.0, 2.0)
        assert WRAPPER.from_database("<(1,2),3>", Circle) == Circle(Point(1.0, 2.0), 3.0)

    def test_malformed_geometric_text_raises(self):
        """The legacy wrapper adapter surfaces malformed text as ValueError."""
        with pytest.raises(ValueError):
            WRAPPER.from_database("(1)", Point)
