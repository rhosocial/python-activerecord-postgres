"""
Create a table with primary key, SERIAL, and constraints - PostgreSQL.

This example demonstrates:
1. CREATE TABLE with SERIAL auto-increment primary key
2. Various column types (VARCHAR, INTEGER, TIMESTAMP, BOOLEAN)
3. UNIQUE and NOT NULL constraints
4. DEFAULT values including CURRENT_TIMESTAMP
5. Verify table structure using introspector
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    DropTableExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.expression.core import FunctionCall
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    IndexDefinition,
)

config = PostgresConnectionConfig(
    host=os.getenv('PG_HOST', 'localhost'),
    port=int(os.getenv('PG_PORT', '5432')),
    database=os.getenv('PG_DATABASE', 'test'),
    username=os.getenv('PG_USERNAME', 'postgres'),
    password=os.getenv('PG_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
dialect = backend.dialect

# Drop if exists for clean setup
drop = DropTableExpression(dialect=dialect, table_name='products', if_exists=True)
sql, params = drop.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
columns = [
    ColumnDefinition(
        name='id',
        data_type='SERIAL',
        constraints=[
            ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
        ],
    ),
    ColumnDefinition(
        name='name',
        data_type='VARCHAR(200)',
        constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ],
    ),
    ColumnDefinition(
        name='price',
        data_type='NUMERIC(10,2)',
        constraints=[
            ColumnConstraint(ColumnConstraintType.NOT_NULL),
        ],
    ),
    ColumnDefinition(
        name='category',
        data_type='VARCHAR(100)',
    ),
    ColumnDefinition(
        name='is_active',
        data_type='BOOLEAN',
        constraints=[
            ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=True),
        ],
    ),
    ColumnDefinition(
        name='created_at',
        data_type='TIMESTAMP',
        constraints=[
            ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=FunctionCall(dialect, 'now')),
        ],
    ),
]

indexes = [
    IndexDefinition(
        name='idx_products_category',
        columns=['category'],
    ),
]

create_expr = CreateTableExpression(
    dialect=dialect,
    table_name='products',
    columns=columns,
    indexes=indexes,
    if_not_exists=True,
)

sql, params = create_expr.to_sql()
print(f"SQL: {sql}")
print(f"Params: {params}")

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
result = backend.execute(sql, params)
print("Table created: products")

# Verify table structure using introspector
columns_info = backend.introspector.list_columns('products')
print("Columns in 'products':")
for col in columns_info:
    print(f"  {col}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
drop_table = DropTableExpression(dialect=dialect, table_name='products', if_exists=True)
sql, params = drop_table.to_sql()
backend.execute(sql, params)
backend.disconnect()

# ============================================================
# SECTION: Summary
# ============================================================
# Key points:
# 1. PostgreSQL uses SERIAL for auto-increment (not AUTO_INCREMENT)
# 2. NUMERIC(p,s) for precise decimal columns
# 3. BOOLEAN type for true/false values
# 4. DEFAULT values use PostgreSQL syntax (TRUE, CURRENT_TIMESTAMP)
# 5. Use introspector.list_columns() to verify table structure
