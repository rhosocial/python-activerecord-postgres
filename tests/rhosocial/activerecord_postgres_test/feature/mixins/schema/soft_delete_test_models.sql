-- tests/rhosocial/activerecord_postgres_test/feature/mixins/schema/soft_delete_test_models.sql
-- Schema for SoftDeleteTestModel (mapped from Task)
CREATE TABLE soft_delete_test_models (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    is_completed BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP WITH TIME ZONE
);