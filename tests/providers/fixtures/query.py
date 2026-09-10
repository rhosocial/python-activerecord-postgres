# tests/providers/fixtures/query.py
"""DDL expressions for the ``feature/query`` table group (PostgreSQL).

Reference: ``tests/rhosocial/activerecord_postgres_test/feature/query/schema/``.
"""

from typing import Callable, Dict

from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
)
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    ForeignKeyConstraint,
    ReferentialAction,
)
from rhosocial.activerecord.backend.expression.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    JsonBType,
    TextType,
    TimestampTzType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresSerialType,
)

_CASCADE = ReferentialAction.CASCADE


# ---------------------------------------------------------------------------
# query/users.sql
# ---------------------------------------------------------------------------

def create_users_table(dialect, table_name: str = "users") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "username", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "email", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "age", IntegerType(dialect=dialect)),
            ColumnDefinition(dialect, "balance", DecimalType(precision=10, scale=2, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=0.00)]),
            ColumnDefinition(dialect, "is_active", BooleanType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=True)]),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
        ],
    )


# ---------------------------------------------------------------------------
# query/posts.sql
# ---------------------------------------------------------------------------

def create_posts_table(dialect, table_name: str = "posts") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "user_id", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "title", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "content", TextType(dialect=dialect)),
            ColumnDefinition(dialect, "status", VarCharType(50, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value="published")]),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
        ],
        table_constraints=[
            ForeignKeyConstraint(dialect, columns=["user_id"], foreign_key_table="users", foreign_key_columns=["id"],
                on_delete=_CASCADE),
        ],
    )


# ---------------------------------------------------------------------------
# query/comments.sql
# ---------------------------------------------------------------------------

def create_comments_table(dialect, table_name: str = "comments") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "user_id", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "post_id", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "content", TextType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "is_hidden", BooleanType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=False)]),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
        ],
        table_constraints=[
            ForeignKeyConstraint(dialect, columns=["user_id"], foreign_key_table="users", foreign_key_columns=["id"],
                on_delete=_CASCADE),
            ForeignKeyConstraint(dialect, columns=["post_id"], foreign_key_table="posts", foreign_key_columns=["id"],
                on_delete=_CASCADE),
        ],
    )


# ---------------------------------------------------------------------------
# query/orders.sql
# ---------------------------------------------------------------------------

def create_orders_table(dialect, table_name: str = "orders") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "user_id", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "order_number", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "total_amount", DecimalType(precision=10, scale=2, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=0.00)]),
            ColumnDefinition(dialect, "status", VarCharType(50, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value="pending")]),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
        ],
        table_constraints=[
            ForeignKeyConstraint(dialect, columns=["user_id"], foreign_key_table="users", foreign_key_columns=["id"]),
        ],
    )


# ---------------------------------------------------------------------------
# query/order_items.sql
# ---------------------------------------------------------------------------

def create_order_items_table(dialect, table_name: str = "order_items") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "order_id", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "product_name", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "quantity", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=1)]),
            ColumnDefinition(dialect, "unit_price", DecimalType(precision=10, scale=2, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "subtotal", DecimalType(precision=10, scale=2, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=0.00)]),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
        ],
        table_constraints=[
            ForeignKeyConstraint(dialect, columns=["order_id"], foreign_key_table="orders", foreign_key_columns=["id"],
                on_delete=_CASCADE),
        ],
    )


# ---------------------------------------------------------------------------
# query/profiles.sql
# ---------------------------------------------------------------------------

def create_profiles_table(dialect, table_name: str = "profiles") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "user_id", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "bio", TextType(dialect=dialect)),
            ColumnDefinition(dialect, "avatar_url", VarCharType(512, dialect=dialect)),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
        ],
        table_constraints=[
            ForeignKeyConstraint(dialect, columns=["user_id"], foreign_key_table="users", foreign_key_columns=["id"]),
        ],
    )


# ---------------------------------------------------------------------------
# query/json_users.sql
# ---------------------------------------------------------------------------

def create_json_users_table(dialect, table_name: str = "json_users") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "username", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "email", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "age", IntegerType(dialect=dialect)),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "settings", JsonBType(dialect=dialect)),
            ColumnDefinition(dialect, "tags", JsonBType(dialect=dialect)),
            ColumnDefinition(dialect, "profile", JsonBType(dialect=dialect)),
            ColumnDefinition(dialect, "roles", JsonBType(dialect=dialect)),
            ColumnDefinition(dialect, "scores", JsonBType(dialect=dialect)),
            ColumnDefinition(dialect, "subscription", JsonBType(dialect=dialect)),
            ColumnDefinition(dialect, "preferences", JsonBType(dialect=dialect)),
        ],
    )


# ---------------------------------------------------------------------------
# query/nodes.sql (self-referential FK)
# ---------------------------------------------------------------------------

def create_nodes_table(dialect, table_name: str = "nodes") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "name", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "parent_id", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NULL)]),
            ColumnDefinition(dialect, "value", DecimalType(precision=10, scale=2, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=0.00)]),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
        ],
        table_constraints=[
            ForeignKeyConstraint(dialect, columns=["parent_id"], foreign_key_table="nodes", foreign_key_columns=["id"],
                on_delete=_CASCADE),
        ],
    )


# ---------------------------------------------------------------------------
# query/searchable_items.sql
# ---------------------------------------------------------------------------

def create_searchable_items_table(dialect, table_name: str = "searchable_items") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "name", VarCharType(255, dialect=dialect)),
            ColumnDefinition(dialect, "tags", TextType(dialect=dialect)),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
        ],
    )


# ---------------------------------------------------------------------------
# query/extended_orders.sql
# ---------------------------------------------------------------------------

def create_extended_orders_table(dialect, table_name: str = "extended_orders") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "user_id", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "order_number", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "total_amount", DecimalType(precision=10, scale=2, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=0.00)]),
            ColumnDefinition(dialect, "status", VarCharType(50, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value="pending")]),
            ColumnDefinition(dialect, "priority", VarCharType(50, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value="medium")]),
            ColumnDefinition(dialect, "region", VarCharType(50, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value="default")]),
            ColumnDefinition(dialect, "category", VarCharType(255, dialect=dialect)),
            ColumnDefinition(dialect, "product", VarCharType(255, dialect=dialect)),
            ColumnDefinition(dialect, "department", VarCharType(255, dialect=dialect)),
            ColumnDefinition(dialect, "year", VarCharType(10, dialect=dialect)),
            ColumnDefinition(dialect, "quarter", VarCharType(10, dialect=dialect)),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
        ],
        table_constraints=[
            ForeignKeyConstraint(dialect, columns=["user_id"], foreign_key_table="users", foreign_key_columns=["id"],
                on_delete=_CASCADE),
        ],
    )


# ---------------------------------------------------------------------------
# query/extended_order_items.sql
# ---------------------------------------------------------------------------

def create_extended_order_items_table(dialect, table_name: str = "extended_order_items") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "order_id", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "product_name", VarCharType(255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "quantity", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=1)]),
            ColumnDefinition(dialect, "price", DecimalType(precision=10, scale=2, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "category", VarCharType(255, dialect=dialect)),
            ColumnDefinition(dialect, "region", VarCharType(50, dialect=dialect)),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
        ],
        table_constraints=[
            ForeignKeyConstraint(dialect, columns=["order_id"], foreign_key_table="extended_orders", foreign_key_columns=["id"],
                on_delete=_CASCADE),
        ],
    )


TABLE_EXPRESSIONS: Dict[str, Callable] = {
    "users": create_users_table,
    "posts": create_posts_table,
    "comments": create_comments_table,
    "orders": create_orders_table,
    "order_items": create_order_items_table,
    "profiles": create_profiles_table,
    "json_users": create_json_users_table,
    "nodes": create_nodes_table,
    "searchable_items": create_searchable_items_table,
    "extended_orders": create_extended_orders_table,
    "extended_order_items": create_extended_order_items_table,
}
