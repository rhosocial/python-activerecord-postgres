-- tests/rhosocial/activerecord_postgres_test/feature/mixins/schema/versioned_products.sql
-- Schema for VersionedProduct
CREATE TABLE versioned_products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) DEFAULT 0.00,
    version INTEGER DEFAULT 1
);