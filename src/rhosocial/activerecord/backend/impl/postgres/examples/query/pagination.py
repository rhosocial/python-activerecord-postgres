"""
Pagination with LIMIT/OFFSET: basic paging, total count, and page iteration.
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    InsertExpression,
    ValuesSource,
    DropTableExpression,
    QueryExpression,
    TableExpression,
    Column,
    OrderByClause,
    LimitOffsetClause,
)
from rhosocial.activerecord.backend.expression.core import (
    Literal,
    WildcardExpression,
    FunctionCall,
)
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

config = PostgresConnectionConfig(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_PORT', '5432')),
    database=os.getenv('POSTGRES_DATABASE', 'test'),
    username=os.getenv('POSTGRES_USER', 'postgres'),
    password=os.getenv('POSTGRES_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
dialect = backend.dialect

drop_table = DropTableExpression(
    dialect=dialect,
    table_name='articles',
    if_exists=True,
    cascade=True,
)
sql, params = drop_table.to_sql()
backend.execute(sql, params)

create_table = CreateTableExpression(
    dialect=dialect,
    table_name='articles',
    columns=[
        ColumnDefinition(
            'id',
            'SERIAL',
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
                ColumnConstraint(ColumnConstraintType.NOT_NULL),
            ],
        ),
        ColumnDefinition(
            'title',
            'VARCHAR(200)',
            constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)],
        ),
        ColumnDefinition('author', 'VARCHAR(100)'),
        ColumnDefinition('published_at', 'DATE'),
    ],
    if_not_exists=True,
)
sql, params = create_table.to_sql()
backend.execute(sql, params)

insert = InsertExpression(
    dialect=dialect,
    into='articles',
    columns=['title', 'author', 'published_at'],
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 'Intro to SQL'), Literal(dialect, 'Alice'), Literal(dialect, '2025-01-10')],
            [Literal(dialect, 'Advanced Joins'), Literal(dialect, 'Bob'), Literal(dialect, '2025-01-15')],
            [Literal(dialect, 'Indexing Deep Dive'), Literal(dialect, 'Alice'), Literal(dialect, '2025-02-01')],
            [Literal(dialect, 'CTE Explained'), Literal(dialect, 'Charlie'), Literal(dialect, '2025-02-10')],
            [Literal(dialect, 'Window Functions 101'), Literal(dialect, 'Bob'), Literal(dialect, '2025-02-20')],
            [Literal(dialect, 'Recursive Queries'), Literal(dialect, 'Alice'), Literal(dialect, '2025-03-01')],
            [Literal(dialect, 'JSONB in Postgres'), Literal(dialect, 'Charlie'), Literal(dialect, '2025-03-10')],
            [Literal(dialect, 'Full-Text Search'), Literal(dialect, 'Bob'), Literal(dialect, '2025-03-15')],
            [Literal(dialect, 'Array Operations'), Literal(dialect, 'Alice'), Literal(dialect, '2025-03-20')],
            [Literal(dialect, 'Partitioning Tables'), Literal(dialect, 'Charlie'), Literal(dialect, '2025-04-01')],
            [Literal(dialect, 'PL/pgSQL Basics'), Literal(dialect, 'Bob'), Literal(dialect, '2025-04-10')],
            [Literal(dialect, 'Connection Pooling'), Literal(dialect, 'Alice'), Literal(dialect, '2025-04-15')],
        ],
    ),
)
sql, params = insert.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================

page_size = 5
options = ExecutionOptions(stmt_type=StatementType.DQL)

# --- 1. Basic LIMIT/OFFSET pagination (page 1) ---
page1_query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'title'),
        Column(dialect, 'author'),
        Column(dialect, 'published_at'),
    ],
    from_=TableExpression(dialect, 'articles'),
    order_by=OrderByClause(
        dialect,
        expressions=[(Column(dialect, 'id'), 'ASC')],
    ),
    limit_offset=LimitOffsetClause(dialect, limit=page_size, offset=0),
)

sql, params = page1_query.to_sql()
print("=== Page 1 (LIMIT/OFFSET) ===")
print(f"SQL: {sql}")
print(f"Params: {params}")

# --- 2. Calculate total pages using COUNT ---
count_query = QueryExpression(
    dialect=dialect,
    select=[FunctionCall(dialect, 'COUNT', WildcardExpression(dialect)).as_('total')],
    from_=TableExpression(dialect, 'articles'),
)

count_sql, count_params = count_query.to_sql()
print("\n=== COUNT Query ===")
print(f"SQL: {count_sql}")
print(f"Params: {count_params}")

# --- 3. Page through all results ---
# (execution shown in the Execution section below)

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================

# Execute page 1
result = backend.execute(sql, params, options=options)
print(f"\nPage 1 rows: {len(result.data) if result.data else 0}")
for row in result.data or []:
    print(f"  {row}")

# Execute count
count_result = backend.execute(count_sql, count_params, options=options)
total_count = count_result.data[0]['total'] if count_result.data else 0
total_pages = (total_count + page_size - 1) // page_size
print(f"\nTotal articles: {total_count}")
print(f"Total pages (page_size={page_size}): {total_pages}")

# Iterate remaining pages
for page_num in range(2, total_pages + 1):
    offset = (page_num - 1) * page_size
    page_query = QueryExpression(
        dialect=dialect,
        select=[
            Column(dialect, 'id'),
            Column(dialect, 'title'),
            Column(dialect, 'author'),
            Column(dialect, 'published_at'),
        ],
        from_=TableExpression(dialect, 'articles'),
        order_by=OrderByClause(
            dialect,
            expressions=[(Column(dialect, 'id'), 'ASC')],
        ),
        limit_offset=LimitOffsetClause(dialect, limit=page_size, offset=offset),
    )
    p_sql, p_params = page_query.to_sql()
    p_result = backend.execute(p_sql, p_params, options=options)
    print(f"\nPage {page_num} rows: {len(p_result.data) if p_result.data else 0}")
    for row in p_result.data or []:
        print(f"  {row}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
