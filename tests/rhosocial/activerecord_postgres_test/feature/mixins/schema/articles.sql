-- tests/rhosocial/activerecord_postgres_test/feature/mixins/schema/articles.sql
-- Schema for Article (mapped from CombinedArticle)
CREATE TABLE articles (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    status VARCHAR(50) DEFAULT 'draft',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    version INTEGER DEFAULT 1,
    deleted_at TIMESTAMP WITH TIME ZONE
);