# tests/providers/fixtures/basic.py
"""DDL expressions for the ``feature/basic`` table group (PostgreSQL).

Each factory builds a :class:`CreateTableExpression` whose generated DDL is
semantically equivalent to the reference ``.sql`` schema files under
``tests/rhosocial/activerecord_postgres_test/feature/basic/schema/``.  Those
``.sql`` files are kept as the authoritative reference and are no longer
loaded at runtime.
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
    TableConstraint,
    TableConstraintType,
    ReferentialAction,
)
from rhosocial.activerecord.backend.expression.types import (
    ArrayType,
    BigIntType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    JsonBType,
    SmallIntType,
    TextType,
    TimeTzType,
    TimestampTzType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.postgres import (
    PostgresSerialType,
    PostgresUUIDType,
    PostgresByteaType,
)

_CASCADE = ReferentialAction.CASCADE


# ---------------------------------------------------------------------------
# basic/users.sql
# ---------------------------------------------------------------------------

def create_users_table(dialect, table_name: str = "users") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("username", VarCharType(length=255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("email", VarCharType(length=255),
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
# basic/type_cases.sql
# ---------------------------------------------------------------------------

def create_type_cases_table(dialect, table_name: str = "type_cases") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresUUIDType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("username", VarCharType(length=255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("email", VarCharType(length=255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("tiny_int", SmallIntType()),
            ColumnDefinition("small_int", SmallIntType()),
            ColumnDefinition("big_int", BigIntType()),
            ColumnDefinition("float_val", FloatType()),
            ColumnDefinition("double_val", DoubleType()),
            ColumnDefinition("decimal_val", DecimalType(precision=10, scale=4)),
            ColumnDefinition("char_val", VarCharType(length=255)),
            ColumnDefinition("varchar_val", VarCharType(length=255)),
            ColumnDefinition("text_val", TextType()),
            ColumnDefinition("date_val", DateType()),
            ColumnDefinition("time_val", TimeTzType()),
            ColumnDefinition("timestamp_val", TimestampTzType()),
            ColumnDefinition("blob_val", PostgresByteaType()),
            ColumnDefinition("json_val", JsonBType()),
            ColumnDefinition("array_val", ArrayType(element_type=IntegerType())),
            ColumnDefinition("is_active", BooleanType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=True)]),
        ],
        table_constraints=[
            TableConstraint(constraint_type=TableConstraintType.PRIMARY_KEY,
                columns=["id"]),
        ],
    )


# ---------------------------------------------------------------------------
# basic/type_tests.sql
# ---------------------------------------------------------------------------

def create_type_tests_table(dialect, table_name: str = "type_tests") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresUUIDType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("string_field", VarCharType(length=255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("int_field", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("float_field", FloatType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("decimal_field", DecimalType(precision=10, scale=2),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("bool_field", BooleanType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("datetime_field", TimestampTzType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("json_field", JsonBType()),
            ColumnDefinition("nullable_field", VarCharType(length=255)),
        ],
        table_constraints=[
            TableConstraint(constraint_type=TableConstraintType.PRIMARY_KEY,
                columns=["id"]),
        ],
    )


# ---------------------------------------------------------------------------
# basic/validated_field_users.sql
# ---------------------------------------------------------------------------

def create_validated_field_users_table(dialect, table_name: str = "validated_field_users") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("username", VarCharType(length=255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("email", VarCharType(length=255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("age", IntegerType()),
            ColumnDefinition("balance", DecimalType(precision=10, scale=2),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("credit_score", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=300)]),
            ColumnDefinition("status", VarCharType(length=50),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value="active")]),
            ColumnDefinition("is_active", BooleanType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=True)]),
        ],
    )


# ---------------------------------------------------------------------------
# basic/validated_users.sql
# ---------------------------------------------------------------------------

def create_validated_users_table(dialect, table_name: str = "validated_users") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("username", VarCharType(length=50),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("email", VarCharType(length=255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("age", IntegerType()),
        ],
    )


# ---------------------------------------------------------------------------
# basic/pydantic_validated_models.sql
# ---------------------------------------------------------------------------

def create_pydantic_validated_models_table(dialect, table_name: str = "pydantic_validated_models") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("code", VarCharType(length=32)),
            ColumnDefinition("quantity", IntegerType()),
            ColumnDefinition("step_count", IntegerType()),
            ColumnDefinition("price", DecimalType(precision=10, scale=2)),
            ColumnDefinition("start_at", TimestampTzType()),
            ColumnDefinition("end_at", TimestampTzType()),
            ColumnDefinition("status", VarCharType(length=32)),
            ColumnDefinition("normalized_name", VarCharType(length=50)),
            ColumnDefinition("created_token", VarCharType(length=255)),
        ],
    )


# ---------------------------------------------------------------------------
# basic/bulk_users.sql
# ---------------------------------------------------------------------------

def create_bulk_users_table(dialect, table_name: str = "bulk_users") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("name", VarCharType(length=255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("age", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=0)]),
            ColumnDefinition("email", VarCharType(length=255),
                constraints=[ColumnConstraint(ColumnConstraintType.DEFAULT, default_value="")]),
        ],
    )


# ---------------------------------------------------------------------------
# basic/posts.sql
# ---------------------------------------------------------------------------

def create_posts_table(dialect, table_name: str = "posts") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("author", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("title", VarCharType(length=255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("content", TextType()),
            ColumnDefinition("published_at", TimestampTzType()),
            ColumnDefinition("published", BooleanType(),
                constraints=[ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=False)]),
            ColumnDefinition("created_at", TimestampTzType()),
            ColumnDefinition("updated_at", TimestampTzType()),
        ],
        table_constraints=[
            ForeignKeyConstraint(columns=["author"], foreign_key_table="users", foreign_key_columns=["id"],
                on_delete=_CASCADE),
        ],
    )


# ---------------------------------------------------------------------------
# basic/comments.sql
# ---------------------------------------------------------------------------

def create_comments_table(dialect, table_name: str = "comments") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("post_ref", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("author", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("text", TextType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("created_at", TimestampTzType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("updated_at", TimestampTzType()),
            ColumnDefinition("approved", BooleanType(),
                constraints=[ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=False)]),
        ],
        table_constraints=[
            ForeignKeyConstraint(columns=["post_ref"], foreign_key_table="posts", foreign_key_columns=["id"],
                on_delete=_CASCADE),
            ForeignKeyConstraint(columns=["author"], foreign_key_table="users", foreign_key_columns=["id"],
                on_delete=_CASCADE),
        ],
    )


# ---------------------------------------------------------------------------
# basic/column_mapping_items.sql
# ---------------------------------------------------------------------------

def create_column_mapping_items_table(dialect, table_name: str = "column_mapping_items") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("name", VarCharType(length=255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("item_total", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("remarks", IntegerType()),
        ],
    )


# ---------------------------------------------------------------------------
# basic/mixed_annotation_items.sql
# ---------------------------------------------------------------------------

def create_mixed_annotation_items_table(dialect, table_name: str = "mixed_annotation_items") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("name", VarCharType(length=255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("tags", ArrayType(element_type=TextType())),
            ColumnDefinition("meta", TextType()),
            ColumnDefinition("description", TextType()),
            ColumnDefinition("status", TextType()),
        ],
    )


# ---------------------------------------------------------------------------
# basic/type_adapter_tests.sql
# ---------------------------------------------------------------------------

def create_type_adapter_tests_table(dialect, table_name: str = "type_adapter_tests") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=True,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("name", VarCharType(length=255),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("optional_name", VarCharType(length=255)),
            ColumnDefinition("optional_age", IntegerType()),
            ColumnDefinition("last_login", TimestampTzType()),
            ColumnDefinition("is_premium", BooleanType()),
            ColumnDefinition("unsupported_union", VarCharType(length=255)),
            ColumnDefinition("custom_bool", VarCharType(length=10)),
            ColumnDefinition("optional_custom_bool", VarCharType(length=10)),
        ],
    )


# ---------------------------------------------------------------------------
# composite_pk/order_items.sql (composite PK)
# ---------------------------------------------------------------------------

def create_composite_pk_order_items_table(dialect, table_name: str = "order_items") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("order_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("product_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("quantity", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=1)]),
            ColumnDefinition("unit_price", DecimalType(precision=10, scale=2),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
        ],
        table_constraints=[
            TableConstraint(constraint_type=TableConstraintType.PRIMARY_KEY,
                columns=["order_id", "product_id"]),
        ],
    )


# ---------------------------------------------------------------------------
# composite_pk/store_inventory.sql (composite PK, no FK)
# ---------------------------------------------------------------------------

def create_store_inventory_table(dialect, table_name: str = "store_inventory") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("store_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("product_id", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("batch_id", VarCharType(length=64),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("stock", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(ColumnConstraintType.DEFAULT, default_value=0)]),
        ],
        table_constraints=[
            TableConstraint(constraint_type=TableConstraintType.PRIMARY_KEY,
                columns=["store_id", "product_id", "batch_id"]),
        ],
    )


# ---------------------------------------------------------------------------
# derived_field/orders.sql (single PK)
# ---------------------------------------------------------------------------

def create_orders_table(dialect, table_name: str = "orders") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("total", DecimalType(precision=10, scale=2),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("created_at", TextType()),
            ColumnDefinition("updated_at", TextType()),
        ],
    )


# ---------------------------------------------------------------------------
# derived_field/product.sql (single PK)
# ---------------------------------------------------------------------------

def create_product_table(dialect, table_name: str = "product") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=True,
        columns=[
            ColumnDefinition("id", PostgresSerialType(),
                constraints=[ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition("name", TextType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("price", FloatType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition("quantity", IntegerType(),
                constraints=[ColumnConstraint(ColumnConstraintType.NOT_NULL)]),
        ],
    )


TABLE_EXPRESSIONS: Dict[str, Callable] = {
    "users": create_users_table,
    "type_cases": create_type_cases_table,
    "type_tests": create_type_tests_table,
    "validated_field_users": create_validated_field_users_table,
    "validated_users": create_validated_users_table,
    "pydantic_validated_models": create_pydantic_validated_models_table,
    "bulk_users": create_bulk_users_table,
    "posts": create_posts_table,
    "comments": create_comments_table,
    "column_mapping_items": create_column_mapping_items_table,
    "mixed_annotation_items": create_mixed_annotation_items_table,
    "type_adapter_tests": create_type_adapter_tests_table,
    "order_items": create_composite_pk_order_items_table,
    "store_inventory": create_store_inventory_table,
    "orders": create_orders_table,
    "product": create_product_table,
}
