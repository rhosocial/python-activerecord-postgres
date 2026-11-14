-- tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/schema/extension_types.sql
-- Need per database the grouped statement; create extension before running this.
CREATE TABLE IF NOT EXISTS native_ext_postgis (
    id serial PRIMARY KEY,
    geom geometry(Point, 4326),
    geog geography(Point, 4326)
);

CREATE TABLE IF NOT EXISTS native_ext_pgvector (
    id serial PRIMARY KEY,
    embedding vector(3)
);

CREATE TABLE IF NOT EXISTS native_ext_hstore (
    id serial PRIMARY KEY,
    attrs hstore
);
