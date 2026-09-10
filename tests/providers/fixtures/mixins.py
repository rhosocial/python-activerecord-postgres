# tests/providers/fixtures/mixins.py
"""DDL expressions for the ``feature/mixins`` table group (PostgreSQL).

Reference: ``tests/rhosocial/activerecord_postgres_test/feature/mixins/schema/``.
"""

from typing import Callable, Dict

from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
)
from rhosocial.activerecord.backend.expression.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    TextType,
    TimestampTzType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresSerialType,
)


# ---------------------------------------------------------------------------
# mixins/combined_articles.sql
# ---------------------------------------------------------------------------

def create_combined_articles_table(dialect, table_name: str = "combined_articles") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "title", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "content", TextType(dialect=dialect)),
            ColumnDefinition(dialect, "status", VarCharType(50, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value="draft")]),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "version", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=1)]),
            ColumnDefinition(dialect, "deleted_at", TimestampTzType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NULL)]),
        ],
    )


# ---------------------------------------------------------------------------
# mixins/tasks.sql
# ---------------------------------------------------------------------------

def create_tasks_table(dialect, table_name: str = "tasks") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "title", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "is_completed", BooleanType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=False)]),
            ColumnDefinition(dialect, "deleted_at", TimestampTzType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NULL)]),
        ],
    )


# ---------------------------------------------------------------------------
# mixins/timestamped_posts.sql
# ---------------------------------------------------------------------------

def create_timestamped_posts_table(dialect, table_name: str = "timestamped_posts") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "title", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "content", TextType(dialect=dialect)),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
        ],
    )


# ---------------------------------------------------------------------------
# mixins/versioned_products.sql
# ---------------------------------------------------------------------------

def create_versioned_products_table(dialect, table_name: str = "versioned_products") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "name", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "price", DecimalType(precision=10, scale=2, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=0.00)]),
            ColumnDefinition(dialect, "version", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=1)]),
        ],
    )


TABLE_EXPRESSIONS: Dict[str, Callable] = {
    "combined_articles": create_combined_articles_table,
    "tasks": create_tasks_table,
    "timestamped_posts": create_timestamped_posts_table,
    "versioned_products": create_versioned_products_table,
}
