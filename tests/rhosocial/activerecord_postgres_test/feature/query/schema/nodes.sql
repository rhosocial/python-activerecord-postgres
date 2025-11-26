-- tests/rhosocial/activerecord_postgres_test/feature/query/schema/nodes.sql
-- Schema for Nodes (for tree/CTE tests)
CREATE TABLE nodes (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    parent_id INTEGER,
    value DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
);
