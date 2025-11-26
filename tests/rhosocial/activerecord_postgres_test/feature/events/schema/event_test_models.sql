-- tests/rhosocial/activerecord_postgres_test/feature/events/schema/event_test_models.sql
-- Schema for EventTestModel
CREATE TABLE event_test_models (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'draft',
    revision INTEGER DEFAULT 1,
    content TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Schema for EventTrackingModel
CREATE TABLE event_tracking_models (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    view_count INTEGER DEFAULT 0,
    last_viewed_at TIMESTAMP WITH TIME ZONE
);