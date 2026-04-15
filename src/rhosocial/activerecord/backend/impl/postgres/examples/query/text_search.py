"""
PostgreSQL-specific full-text search using tsvector and tsquery.
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
)
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

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
        ColumnDefinition('title', 'VARCHAR(200)'),
        ColumnDefinition('content', 'TEXT'),
    ],
)
backend.execute(*create_table.to_sql())

insert_expr = InsertExpression(
    dialect=dialect,
    into='articles',
    columns=['title', 'content'],
    source=ValuesSource(
        dialect,
        [
            [Literal(dialect, 'PostgreSQL Tutorial'), Literal(dialect, 'This tutorial covers PostgreSQL database basics and advanced features.')],
            [Literal(dialect, 'MySQL Guide'), Literal(dialect, 'Learn MySQL from beginner to advanced level.')],
            [Literal(dialect, 'Database Design'), Literal(dialect, 'Best practices for designing relational databases.')],
        ],
    ),
)
backend.execute(*insert_expr.to_sql())

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.expression import (
    QueryExpression,
    TableExpression,
    Column,
    WhereClause,
)
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.operators import RawSQLExpression
from rhosocial.activerecord.backend.impl.postgres.functions.text_search import (
    to_tsvector,
    plainto_tsquery,
)
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate

search_term = 'PostgreSQL'
tsvector_expr = to_tsvector('content', 'english')
tsquery_expr = plainto_tsquery(search_term, 'english')
match_expr = RawSQLExpression(
    dialect,
    f"{tsvector_expr} @@ {tsquery_expr}",
)

query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'title'),
    ],
    from_=TableExpression(dialect, 'articles'),
    where=WhereClause(
        dialect,
        condition=ComparisonPredicate(
            dialect,
            '=',
            Literal(dialect, True),
            match_expr,
        ),
    ),
)

sql, params = query.to_sql()
print(f"SQL: {sql}")
print(f"Params: {params}")

# ============================================================
# SECTION: Execution (run the expression)
# ============================================================
options = ExecutionOptions(stmt_type=StatementType.DQL)
result = backend.execute(sql, params, options=options)
print(f"Rows returned: {len(result.data) if result.data else 0}")
for row in result.data or []:
    print(f" {row}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
