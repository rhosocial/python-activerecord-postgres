-- tests/rhosocial/activerecord_postgres_test/feature/basic/schema/validated_field_users.sql
-- First create the custom enum type
DO $$
BEGIN
    -- Try creating a type
    BEGIN
        CREATE TYPE user_status AS ENUM (
            'active', 'inactive', 'banned', 'pending', 'suspended'
        );
    EXCEPTION
        -- Catch type already exists error (error code 42710)
        WHEN duplicate_object THEN
            NULL;  -- Ignore the error and continue
    END;
END
$$;

CREATE TABLE validated_field_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    age INTEGER,
    balance DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    credit_score INTEGER NOT NULL DEFAULT 300,
    status user_status NOT NULL DEFAULT 'active',
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);