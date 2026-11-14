-- tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/schema/native_types.sql
-- Built-in PostgreSQL types only: every statement is safe on a stock server.
-- Tables are grouped by family; a module enables only the tables it names.

CREATE TABLE IF NOT EXISTS native_wrapped_scalars (
    id serial PRIMARY KEY,
    amount money,
    oid_value oid,
    regclass_value regclass,
    regtype_value regtype,
    regproc_value regproc,
    regconfig_value regconfig,
    regnamespace_value regnamespace,
    xid_value xid,
    xid8_value xid8,
    cid_value cid,
    tid_value tid,
    lsn_value pg_lsn
);

CREATE TABLE IF NOT EXISTS native_network (
    id serial PRIMARY KEY,
    inet_value inet,
    cidr_value cidr,
    mac_value macaddr,
    mac8_value macaddr8
);

CREATE TABLE IF NOT EXISTS native_bits (
    id serial PRIMARY KEY,
    fixed_bits bit(8),
    var_bits varbit,
    five_bits bit(5)
);

CREATE TABLE IF NOT EXISTS native_full_text (
    id serial PRIMARY KEY,
    vector_value tsvector,
    query_value tsquery
);

CREATE TABLE IF NOT EXISTS native_geometric (
    id serial PRIMARY KEY,
    point_value point,
    line_value line,
    lseg_value lseg,
    box_value box,
    path_value path,
    polygon_value polygon,
    circle_value circle
);

CREATE TABLE IF NOT EXISTS native_arrays (
    id serial PRIMARY KEY,
    ints int[],
    texts text[]
);

CREATE TABLE IF NOT EXISTS native_ranges (
    id serial PRIMARY KEY,
    i4 int4range,
    i8 int8range,
    num numrange,
    ts tsrange,
    tstz tstzrange,
    dt daterange
);

CREATE TABLE IF NOT EXISTS native_multiranges (
    id serial PRIMARY KEY,
    i4 int4multirange,
    i8 int8multirange,
    num nummultirange,
    ts tsmultirange,
    tstz tstzmultirange,
    dt datemultirange
);

CREATE TABLE IF NOT EXISTS native_json_xml (
    id serial PRIMARY KEY,
    payload_json json,
    payload_jsonb jsonb,
    path_value jsonpath,
    doc xml
);

CREATE TABLE IF NOT EXISTS native_remaining (
    id serial PRIMARY KEY,
    payload bytea,
    span interval
);
