-- tests/rhosocial/activerecord_postgres_test/feature/query/schema/extended_order_items.sql
-- Schema for ExtendedOrderItem
CREATE TABLE extended_order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(255),
    region VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
);
