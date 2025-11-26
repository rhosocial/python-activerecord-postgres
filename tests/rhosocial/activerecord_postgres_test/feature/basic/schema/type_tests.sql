-- tests/rhosocial/activerecord_postgres_test/feature/basic/schema/type_tests.sql
CREATE TABLE type_tests (
    id UUID NOT NULL, -- Only when defined as UUID, the driver allows passing UUID type
    string_field VARCHAR(255) NOT NULL,
    int_field INTEGER NOT NULL,
    float_field REAL NOT NULL,
    decimal_field DECIMAL(10,2) NOT NULL,
    bool_field BOOLEAN NOT NULL,
    datetime_field TIMESTAMP WITH TIME ZONE NOT NULL,
    json_field JSONB,         -- postgres supports native JSON types
    nullable_field VARCHAR(255),
    PRIMARY KEY (id)
);