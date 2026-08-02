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
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("username", VarCharType(255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("email", VarCharType(255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("age", IntegerType()),
            ColumnDefinition("balance", DecimalType(precision=10, scale=2),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=0.00)]),
            ColumnDefinition("is_active", BooleanType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=True)]),
            ColumnDefinition("created_at", TimestampTzType()),
            ColumnDefinition("updated_at", TimestampTzType()),
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
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("user_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("title", VarCharType(255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("content", TextType()),
            ColumnDefinition("status", VarCharType(50),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value="published")]),
            ColumnDefinition("created_at", TimestampTzType()),
            ColumnDefinition("updated_at", TimestampTzType()),
        ],
        table_constraints=[
            ForeignKeyConstraint(columns=["user_id"], foreign_key_table="users", foreign_key_columns=["id"],
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
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("user_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("post_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("content", TextType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("is_hidden", BooleanType(),
                constraints=[ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=False)]),
            ColumnDefinition("created_at", TimestampTzType()),
            ColumnDefinition("updated_at", TimestampTzType()),
        ],
        table_constraints=[
            ForeignKeyConstraint(columns=["user_id"], foreign_key_table="users", foreign_key_columns=["id"],
                on_delete=_CASCADE),
            ForeignKeyConstraint(columns=["post_id"], foreign_key_table="posts", foreign_key_columns=["id"],
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
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("user_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("order_number", VarCharType(255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("total_amount", DecimalType(precision=10, scale=2),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=0.00)]),
            ColumnDefinition("status", VarCharType(50),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value="pending")]),
            ColumnDefinition("created_at", TimestampTzType()),
            ColumnDefinition("updated_at", TimestampTzType()),
        ],
        table_constraints=[
            ForeignKeyConstraint(columns=["user_id"], foreign_key_table="users", foreign_key_columns=["id"]),
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
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("order_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("product_name", VarCharType(255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("quantity", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=1)]),
            ColumnDefinition("unit_price", DecimalType(precision=10, scale=2),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("subtotal", DecimalType(precision=10, scale=2),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=0.00)]),
            ColumnDefinition("created_at", TimestampTzType()),
            ColumnDefinition("updated_at", TimestampTzType()),
        ],
        table_constraints=[
            ForeignKeyConstraint(columns=["order_id"], foreign_key_table="orders", foreign_key_columns=["id"],
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
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("user_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("bio", TextType()),
            ColumnDefinition("avatar_url", VarCharType(512)),
            ColumnDefinition("created_at", TimestampTzType()),
            ColumnDefinition("updated_at", TimestampTzType()),
        ],
        table_constraints=[
            ForeignKeyConstraint(columns=["user_id"], foreign_key_table="users", foreign_key_columns=["id"]),
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
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("username", VarCharType(255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("email", VarCharType(255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("age", IntegerType()),
            ColumnDefinition("created_at", TimestampTzType()),
            ColumnDefinition("updated_at", TimestampTzType()),
            ColumnDefinition("settings", JsonBType()),
            ColumnDefinition("tags", JsonBType()),
            ColumnDefinition("profile", JsonBType()),
            ColumnDefinition("roles", JsonBType()),
            ColumnDefinition("scores", JsonBType()),
            ColumnDefinition("subscription", JsonBType()),
            ColumnDefinition("preferences", JsonBType()),
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
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("name", VarCharType(255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("parent_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NULL)]),
            ColumnDefinition("value", DecimalType(precision=10, scale=2),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=0.00)]),
            ColumnDefinition("created_at", TimestampTzType()),
            ColumnDefinition("updated_at", TimestampTzType()),
        ],
        table_constraints=[
            ForeignKeyConstraint(columns=["parent_id"], foreign_key_table="nodes", foreign_key_columns=["id"],
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
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("name", VarCharType(255)),
            ColumnDefinition("tags", TextType()),
            ColumnDefinition("created_at", TimestampTzType()),
            ColumnDefinition("updated_at", TimestampTzType()),
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
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("user_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("order_number", VarCharType(255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("total_amount", DecimalType(precision=10, scale=2),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=0.00)]),
            ColumnDefinition("status", VarCharType(50),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value="pending")]),
            ColumnDefinition("priority", VarCharType(50),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value="medium")]),
            ColumnDefinition("region", VarCharType(50),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value="default")]),
            ColumnDefinition("category", VarCharType(255)),
            ColumnDefinition("product", VarCharType(255)),
            ColumnDefinition("department", VarCharType(255)),
            ColumnDefinition("year", VarCharType(10)),
            ColumnDefinition("quarter", VarCharType(10)),
            ColumnDefinition("created_at", TimestampTzType()),
            ColumnDefinition("updated_at", TimestampTzType()),
        ],
        table_constraints=[
            ForeignKeyConstraint(columns=["user_id"], foreign_key_table="users", foreign_key_columns=["id"],
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
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("order_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("product_name", VarCharType(255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("quantity", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=1)]),
            ColumnDefinition("price", DecimalType(precision=10, scale=2),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("category", VarCharType(255)),
            ColumnDefinition("region", VarCharType(50)),
            ColumnDefinition("created_at", TimestampTzType()),
            ColumnDefinition("updated_at", TimestampTzType()),
        ],
        table_constraints=[
            ForeignKeyConstraint(columns=["order_id"], foreign_key_table="extended_orders", foreign_key_columns=["id"],
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
