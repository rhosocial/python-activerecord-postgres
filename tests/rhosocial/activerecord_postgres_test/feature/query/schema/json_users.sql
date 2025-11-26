-- tests/rhosocial/activerecord_postgres_test/feature/query/schema/json_users.sql
-- Schema for JsonUser model
CREATE TABLE json_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    age INTEGER,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    -- JSONB fields for JSON testing
    settings JSONB,
    tags JSONB,
    profile JSONB,
    roles JSONB,
    scores JSONB,
    subscription JSONB,
    preferences JSONB
);
