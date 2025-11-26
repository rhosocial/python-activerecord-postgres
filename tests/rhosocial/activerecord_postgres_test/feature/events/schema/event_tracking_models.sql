-- tests/rhosocial/activerecord_postgres_test/feature/events/schema/event_tracking_models.sql
-- Schema for EventTrackingModel
CREATE TABLE event_tracking_models (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    view_count INTEGER DEFAULT 0,
    last_viewed_at TIMESTAMP WITH TIME ZONE
);