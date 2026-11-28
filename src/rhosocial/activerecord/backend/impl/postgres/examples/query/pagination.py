"""
Pagination with LIMIT/OFFSET: basic paging, total count, and page iteration.
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.expression import (
    CreateTableStatement,
    InsertStatement,
    ValuesSource,
    DropTableStatement,
    QueryStatement,
    Column,
    OrderByClause,
    LimitOffsetClause,
    ColumnNotNullConstraint,
    ColumnPrimaryKeyConstraint,
)
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.value_types.parameter import Parameter
from rhosocial.activerecord.backend.expression.value_types.wildcards import RowCountWildcard
from rhosocial.activerecord.backend.expression.functions import CountFunction
from rhosocial.activerecord.backend.expression.functions.function_call import FunctionCall
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

if __name__ == "__main__":
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

    drop_table = DropTableStatement(
        dialect=dialect,
        table=Table(dialect, 'articles'),
        if_exists=True,
        cascade=True,
    )
    sql, params = drop_table.to_sql()
    backend.execute(sql, params)

    create_table = CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, 'articles'),
        columns=[
            ColumnDefinition(
                'id',
                'SERIAL',
                constraints=[
                    ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
                    ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect)),
                ],
            ),
            ColumnDefinition(
                'title',
                'VARCHAR(200)',
                constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))],
            ),
            ColumnDefinition('author', 'VARCHAR(100)'),
            ColumnDefinition('published_at', 'DATE'),
        ],
        if_not_exists=True,
    )
    sql, params = create_table.to_sql()
    backend.execute(sql, params)

    insert = InsertStatement(
        dialect=dialect,
        into=Table(dialect, 'articles'),
        columns=['title', 'author', 'published_at'],
        source=ValuesSource(
            dialect,
            [
                [Parameter(dialect, 'Intro to SQL'), Parameter(dialect, 'Alice'), Parameter(dialect, '2025-01-10')],
                [Parameter(dialect, 'Advanced Joins'), Parameter(dialect, 'Bob'), Parameter(dialect, '2025-01-15')],
                [Parameter(dialect, 'Indexing Deep Dive'), Parameter(dialect, 'Alice'), Parameter(dialect, '2025-02-01')],
                [Parameter(dialect, 'CTE Explained'), Parameter(dialect, 'Charlie'), Parameter(dialect, '2025-02-10')],
                [Parameter(dialect, 'Window Functions 101'), Parameter(dialect, 'Bob'), Parameter(dialect, '2025-02-20')],
                [Parameter(dialect, 'Recursive Queries'), Parameter(dialect, 'Alice'), Parameter(dialect, '2025-03-01')],
                [Parameter(dialect, 'JSONB in Postgres'), Parameter(dialect, 'Charlie'), Parameter(dialect, '2025-03-10')],
                [Parameter(dialect, 'Full-Text Search'), Parameter(dialect, 'Bob'), Parameter(dialect, '2025-03-15')],
                [Parameter(dialect, 'Array Operations'), Parameter(dialect, 'Alice'), Parameter(dialect, '2025-03-20')],
                [Parameter(dialect, 'Partitioning Tables'), Parameter(dialect, 'Charlie'), Parameter(dialect, '2025-04-01')],
                [Parameter(dialect, 'PL/pgSQL Basics'), Parameter(dialect, 'Bob'), Parameter(dialect, '2025-04-10')],
                [Parameter(dialect, 'Connection Pooling'), Parameter(dialect, 'Alice'), Parameter(dialect, '2025-04-15')],
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
    page1_query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, 'id'),
            Column(dialect, 'title'),
            Column(dialect, 'author'),
            Column(dialect, 'published_at'),
        ],
        from_=Table(dialect, 'articles'),
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
    count_query = QueryStatement(
        dialect=dialect,
        select=[CountFunction(dialect, RowCountWildcard(dialect)).as_('total')],
        from_=Table(dialect, 'articles'),
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
        page_query = QueryStatement(
            dialect=dialect,
            select=[
                Column(dialect, 'id'),
                Column(dialect, 'title'),
                Column(dialect, 'author'),
                Column(dialect, 'published_at'),
            ],
            from_=Table(dialect, 'articles'),
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
