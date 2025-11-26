-- tests/rhosocial/activerecord_postgres_test/feature/mixins/schema/comments.sql
-- Schema for Comment (used with Article)
CREATE TABLE comments (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    content TEXT,
    article_id INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    version INTEGER DEFAULT 1,
    deleted_at TIMESTAMP WITH TIME ZONE
);