-- tests/rhosocial/activerecord_postgres_test/feature/mixins/schema/timestamped_posts.sql
-- Schema for TimestampedPost
CREATE TABLE timestamped_posts (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);