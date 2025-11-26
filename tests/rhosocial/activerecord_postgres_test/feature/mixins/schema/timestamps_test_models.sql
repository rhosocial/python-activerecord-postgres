-- tests/rhosocial/activerecord_postgres_test/feature/mixins/schema/timestamps_test_models.sql
-- Schema for TimestampsTestModel (mapped from TimestampedPost)
CREATE TABLE timestamps_test_models (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);