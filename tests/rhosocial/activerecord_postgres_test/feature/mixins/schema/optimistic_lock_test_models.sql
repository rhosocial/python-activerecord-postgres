-- tests/rhosocial/activerecord_postgres_test/feature/mixins/schema/optimistic_lock_test_models.sql
-- Schema for OptimisticLockTestModel (mapped from VersionedProduct)
CREATE TABLE optimistic_lock_test_models (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) DEFAULT 0.00,
    version INTEGER DEFAULT 1
);