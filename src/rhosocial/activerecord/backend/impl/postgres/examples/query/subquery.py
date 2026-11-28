"""
Subquery expressions - WHERE and FROM subqueries.
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
    ColumnNotNullConstraint,
    ColumnPrimaryKeyConstraint,
)
from rhosocial.activerecord.backend.expression.core import Parameter
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

    for table in ['employees', 'departments']:
        drop_table = DropTableStatement(
            dialect=dialect,
            table=Table(dialect, table),
            if_exists=True,
            cascade=True,
        )
        sql, params = drop_table.to_sql()
        backend.execute(sql, params)

    create_departments = CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, 'departments'),
        columns=[
            ColumnDefinition(
                'id',
                'SERIAL',
                constraints=[
                    ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
                    ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect)),
                ],
            ),
            ColumnDefinition('name', 'VARCHAR(100)'),
            ColumnDefinition('budget', 'DECIMAL(15,2)'),
        ],
    )
    backend.execute(*create_departments.to_sql())

    create_employees = CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, 'employees'),
        columns=[
            ColumnDefinition(
                'id',
                'SERIAL',
                constraints=[
                    ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
                    ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect)),
                ],
            ),
            ColumnDefinition('name', 'VARCHAR(100)'),
            ColumnDefinition('salary', 'DECIMAL(10,2)'),
            ColumnDefinition('department_id', 'INTEGER'),
        ],
    )
    backend.execute(*create_employees.to_sql())

    insert_departments = InsertStatement(
        dialect=dialect,
        into=Table(dialect, 'departments'),
        columns=['name', 'budget'],
        source=ValuesSource(
            dialect,
            [
                [Parameter(dialect, 'Engineering'), Parameter(dialect, 1000000)],
                [Parameter(dialect, 'Sales'), Parameter(dialect, 500000)],
            ],
        ),
    )
    backend.execute(*insert_departments.to_sql())

    insert_employees = InsertStatement(
        dialect=dialect,
        into=Table(dialect, 'employees'),
        columns=['name', 'salary', 'department_id'],
        source=ValuesSource(
            dialect,
            [
                [Parameter(dialect, 'Alice'), Parameter(dialect, 80000), Parameter(dialect, 1)],
                [Parameter(dialect, 'Bob'), Parameter(dialect, 90000), Parameter(dialect, 1)],
                [Parameter(dialect, 'Charlie'), Parameter(dialect, 60000), Parameter(dialect, 2)],
                [Parameter(dialect, 'David'), Parameter(dialect, 70000), Parameter(dialect, 2)],
            ],
        ),
    )
    backend.execute(*insert_employees.to_sql())

    # ============================================================
    # SECTION: Business Logic (the pattern to learn)
    # ============================================================
    from rhosocial.activerecord.backend.expression import (
        QueryStatement, Column, WhereClause, Subquery,
    )
    from rhosocial.activerecord.backend.expression.objects import Table
    from rhosocial.activerecord.backend.expression import functions as F  # noqa: E402
    from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate

    subquery_query = QueryStatement(
        dialect=dialect,
        select=[F.avg(dialect, Column(dialect, 'salary'))],
        from_=Table(dialect, 'employees'),
    )
    sql, params = subquery_query.to_sql()
    subquery = Subquery(dialect, sql, params)

    query = QueryStatement(
        dialect=dialect,
        select=[
            Column(dialect, 'name'),
            Column(dialect, 'salary'),
        ],
        from_=Table(dialect, 'employees'),
        where=WhereClause(
            dialect,
            condition=ComparisonPredicate(
                dialect,
                '>',
                Column(dialect, 'salary'),
                subquery,
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
