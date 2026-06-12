-- tests/rhosocial/activerecord_postgres_test/feature/query/schema/profiles.sql
-- Schema for Profile model
CREATE TABLE profiles (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    bio TEXT,
    avatar_url VARCHAR(512),
    created_at TIMESTAMP(6),
    updated_at TIMESTAMP(6)
);
