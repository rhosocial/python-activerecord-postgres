-- tests/rhosocial/activerecord_postgres_test/feature/basic/schema/type_cases.sql
CREATE TABLE type_cases (
    id UUID NOT NULL, -- Only when defined as UUID, the driver allows passing UUID type
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    tiny_int SMALLINT,
    small_int SMALLINT,
    big_int BIGINT,
    float_val REAL,
    double_val DOUBLE PRECISION,
    decimal_val DECIMAL(10,4),
    char_val VARCHAR(255),
    varchar_val VARCHAR(255),
    text_val TEXT,
    date_val DATE,
    time_val TIME WITH TIME ZONE,
    timestamp_val TIMESTAMP WITH TIME ZONE,
    blob_val BYTEA,
    json_val JSONB,          -- postgres supports native JSON types
    array_val INTEGER[],        -- postgres supports native arrays
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (id)
);