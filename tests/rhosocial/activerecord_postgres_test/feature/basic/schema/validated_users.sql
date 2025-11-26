-- tests/rhosocial/activerecord_postgres_test/feature/basic/schema/validated_users.sql
CREATE TABLE validated_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL,
    age INTEGER
);