-- tests/rhosocial/activerecord_postgres_test/feature/events/schema/event_tests.sql
-- Schema for EventTestModel
CREATE TABLE event_tests (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'draft',
    revision INTEGER DEFAULT 1,
    content TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);