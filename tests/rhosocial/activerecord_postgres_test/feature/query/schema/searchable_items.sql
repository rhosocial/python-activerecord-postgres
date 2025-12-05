-- tests/rhosocial/activerecord_postgres_test/feature/query/schema/searchable_items.sql

CREATE TABLE "searchable_items" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(255),
    "tags" TEXT,
    "created_at" TIMESTAMPTZ,
    "updated_at" TIMESTAMPTZ
);
