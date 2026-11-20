# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/test_native_extensions.py
"""Native tests for extension-provided types.

The extension existence is established before any table is created, so a
server without the extension reports a skip naming exactly that extension.
On the shared test server only ``hstore`` is available; ``postgis`` and
``pgvector`` are listed in ``pg_available_extensions`` as absent and their
classes skip accordingly.
"""

import pytest

from rhosocial.activerecord.backend.adapters import Geometry, Vector
from rhosocial.activerecord.backend.errors import DatabaseError, TypeConversionError
from rhosocial.activerecord.backend.impl.postgres.adapters.hstore import (
    PostgresHstoreAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.neutral import (
    PostgresNeutralGeometryAdapter,
    PostgresNeutralVectorAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.type_values import (
    PostgresHstore,
)
from rhosocial.activerecord_postgres_test.feature.backend.adapters.native.helpers import (
    insert_row,
    insert_sql,
    read_raw,
    update_cell,
)

POSTGIS_TABLE = "native_ext_postgis"
VECTOR_TABLE = "native_ext_pgvector"
HSTORE_TABLE = "native_ext_hstore"

GEOMETRY = PostgresNeutralGeometryAdapter()
VECTOR = PostgresNeutralVectorAdapter()
HSTORE = PostgresHstoreAdapter()


class TestPostgis:
    """PostGIS geometry/geography; skipped when the extension is absent."""

    def test_geometry_ewkt_round_trip_and_srid(self, postgis_backend):
        backend = postgis_backend
        value = Geometry.from_wkt("POINT (1 2)", srid=4326)
        bound = GEOMETRY.to_database(value, str)
        assert bound == "SRID=4326;POINT (1.0 2.0)"

        row_id = insert_row(
            backend, POSTGIS_TABLE, {"geom": bound}, {"geom": "geometry"}
        )
        ewkt = read_raw(backend, POSTGIS_TABLE, "geom", row_id, "ST_AsEWKT(geom)")
        assert ewkt.startswith("SRID=4326;POINT")
        assert read_raw(backend, POSTGIS_TABLE, "geom", row_id, "ST_SRID(geom)") == 4326

        decoded = GEOMETRY.from_database(ewkt, Geometry)
        assert decoded.srid == 4326
        assert decoded.to_wkt().startswith("POINT")

        update_cell(backend, POSTGIS_TABLE, "geom", row_id, ewkt, "geometry")
        assert read_raw(backend, POSTGIS_TABLE, "geom", row_id, "ST_AsEWKT(geom)") == ewkt

    def test_geography_keeps_its_default_srid(self, postgis_backend):
        backend = postgis_backend
        value = Geometry.from_wkt("POINT (1 2)", srid=4326)
        bound = GEOMETRY.to_database(value, str)
        row_id = insert_row(
            backend, POSTGIS_TABLE, {"geog": bound}, {"geog": "geography"}
        )
        assert read_raw(backend, POSTGIS_TABLE, "geog", row_id, "ST_SRID(geog)") == 4326
        ewkt = read_raw(backend, POSTGIS_TABLE, "geog", row_id, "ST_AsEWKT(geog)")
        assert ewkt.startswith("SRID=4326;POINT")
        assert GEOMETRY.from_database(ewkt, Geometry).srid == 4326

    def test_default_geometry_text_is_hex_ewkb_and_rejected(self, postgis_backend):
        """PostGIS's default output is EWKB hex; the adapter refuses to guess."""
        backend = postgis_backend
        row_id = insert_row(
            backend,
            POSTGIS_TABLE,
            {"geom": GEOMETRY.to_database(Geometry.from_wkt("POINT (1 2)", srid=4326), str)},
            {"geom": "geometry"},
        )
        hex_ewkb = read_raw(backend, POSTGIS_TABLE, "geom", row_id, "geom::text")
        assert hex_ewkb[:2].lower() == "01"
        with pytest.raises(TypeConversionError) as excinfo:
            GEOMETRY.from_database(hex_ewkb, Geometry)
        assert "EWKB" in str(excinfo.value)

    def test_external_ewkt_literal_reads_back(self, postgis_backend):
        backend = postgis_backend
        row_id = insert_sql(
            backend,
            "INSERT INTO {} (geom) VALUES "
            "(ST_GeomFromEWKT('SRID=4326;POINT(1 2)')) RETURNING id".format(
                POSTGIS_TABLE
            ),
        )
        ewkt = read_raw(backend, POSTGIS_TABLE, "geom", row_id, "ST_AsEWKT(geom)")
        assert ewkt == "SRID=4326;POINT(1 2)"
        assert GEOMETRY.from_database(ewkt, Geometry).srid == 4326


class TestPgVector:
    """pgvector ``vector``; skipped when the extension is absent."""

    def test_vector_text_round_trip(self, vector_backend):
        backend = vector_backend
        value = Vector((1.0, 2.0, 3.0))
        bound = VECTOR.to_database(value, str)
        assert bound == "[1.0,2.0,3.0]"

        row_id = insert_row(
            backend, VECTOR_TABLE, {"embedding": bound}, {"embedding": "vector(3)"}
        )
        raw = read_raw(backend, VECTOR_TABLE, "embedding", row_id)
        assert raw == "[1,2,3]"
        assert read_raw(
            backend, VECTOR_TABLE, "embedding", row_id, "vector_dims(embedding)"
        ) == 3
        assert VECTOR.from_database(raw, Vector) == value

        update_cell(
            backend, VECTOR_TABLE, "embedding", row_id,
            VECTOR.to_database(VECTOR.from_database(raw, Vector), str), "vector(3)",
        )
        assert read_raw(backend, VECTOR_TABLE, "embedding", row_id) == raw

    def test_vector_dimension_mismatch_is_rejected(self, vector_backend):
        backend = vector_backend
        with pytest.raises(DatabaseError):
            backend.execute(
                "SELECT %s::vector(3)", (VECTOR.to_database(Vector((1.0, 2.0)), str),)
            )


class TestHstore:
    """hstore key/value pairs; installed from contrib when available."""

    def test_hstore_dict_round_trip_and_raw_text(self, hstore_backend):
        backend = hstore_backend
        value = {"a": "1", "b": "2"}
        bound = HSTORE.to_database(value, str)
        assert bound == '"a"=>"1", "b"=>"2"'

        row_id = insert_row(
            backend, HSTORE_TABLE, {"attrs": bound}, {"attrs": "hstore"}
        )
        raw = read_raw(backend, HSTORE_TABLE, "attrs", row_id)
        assert raw == '"a"=>"1", "b"=>"2"'
        decoded = HSTORE.from_database(raw, PostgresHstore)
        assert decoded.data == value

        update_cell(backend, HSTORE_TABLE, "attrs", row_id, raw, "hstore")
        assert read_raw(backend, HSTORE_TABLE, "attrs", row_id) == raw

    def test_external_hstore_literal_including_null(self, hstore_backend):
        backend = hstore_backend
        row_id = insert_sql(
            backend,
            "INSERT INTO {} (attrs) VALUES ('a=>1, b=>NULL'::hstore) RETURNING id".format(
                HSTORE_TABLE
            ),
        )
        raw = read_raw(backend, HSTORE_TABLE, "attrs", row_id)
        assert raw == '"a"=>"1", "b"=>NULL'
        decoded = HSTORE.from_database(raw, PostgresHstore)
        assert decoded.data == {"a": "1", "b": None}
