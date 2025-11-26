-- tests/rhosocial/activerecord_postgres_test/feature/mixins/schema/tasks.sql
-- Schema for Task
CREATE TABLE tasks (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    is_completed BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP WITH TIME ZONE
);