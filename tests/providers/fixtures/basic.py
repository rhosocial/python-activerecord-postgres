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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "username", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "email", VarCharType(length=255, dialect=dialect),
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
# basic/type_cases.sql
# ---------------------------------------------------------------------------

def create_type_cases_table(dialect, table_name: str = "type_cases") -> CreateTableExpression:
    return CreateTableExpression(
        dialect=dialect,
        table=table_name,
        if_not_exists=False,
        columns=[
            ColumnDefinition(dialect, "id", PostgresUUIDType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "username", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "email", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "tiny_int", SmallIntType(dialect=dialect)),
            ColumnDefinition(dialect, "small_int", SmallIntType(dialect=dialect)),
            ColumnDefinition(dialect, "big_int", BigIntType(dialect=dialect)),
            ColumnDefinition(dialect, "float_val", FloatType(dialect=dialect)),
            ColumnDefinition(dialect, "double_val", DoubleType(dialect=dialect)),
            ColumnDefinition(dialect, "decimal_val", DecimalType(precision=10, scale=4, dialect=dialect)),
            ColumnDefinition(dialect, "char_val", VarCharType(length=255, dialect=dialect)),
            ColumnDefinition(dialect, "varchar_val", VarCharType(length=255, dialect=dialect)),
            ColumnDefinition(dialect, "text_val", TextType(dialect=dialect)),
            ColumnDefinition(dialect, "date_val", DateType(dialect=dialect)),
            ColumnDefinition(dialect, "time_val", TimeTzType(dialect=dialect)),
            ColumnDefinition(dialect, "timestamp_val", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "blob_val", PostgresByteaType(dialect=dialect)),
            ColumnDefinition(dialect, "json_val", JsonBType(dialect=dialect)),
            ColumnDefinition(dialect, "array_val", ArrayType(IntegerType(dialect=dialect), dialect=dialect)),
            ColumnDefinition(dialect, "is_active", BooleanType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=True)]),
        ],
        table_constraints=[
            TableConstraint(dialect, constraint_type=TableConstraintType.PRIMARY_KEY,
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
            ColumnDefinition(dialect, "id", PostgresUUIDType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "string_field", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "int_field", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "float_field", FloatType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "decimal_field", DecimalType(precision=10, scale=2, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "bool_field", BooleanType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "datetime_field", TimestampTzType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "json_field", JsonBType(dialect=dialect)),
            ColumnDefinition(dialect, "nullable_field", VarCharType(length=255, dialect=dialect)),
        ],
        table_constraints=[
            TableConstraint(dialect, constraint_type=TableConstraintType.PRIMARY_KEY,
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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "username", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "email", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "age", IntegerType(dialect=dialect)),
            ColumnDefinition(dialect, "balance", DecimalType(precision=10, scale=2, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "credit_score", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=300)]),
            ColumnDefinition(dialect, "status", VarCharType(length=50, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value="active")]),
            ColumnDefinition(dialect, "is_active", BooleanType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=True)]),
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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "username", VarCharType(length=50, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "email", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "age", IntegerType(dialect=dialect)),
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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "code", VarCharType(length=32, dialect=dialect)),
            ColumnDefinition(dialect, "quantity", IntegerType(dialect=dialect)),
            ColumnDefinition(dialect, "step_count", IntegerType(dialect=dialect)),
            ColumnDefinition(dialect, "price", DecimalType(precision=10, scale=2, dialect=dialect)),
            ColumnDefinition(dialect, "start_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "end_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "status", VarCharType(length=32, dialect=dialect)),
            ColumnDefinition(dialect, "normalized_name", VarCharType(length=50, dialect=dialect)),
            ColumnDefinition(dialect, "created_token", VarCharType(length=255, dialect=dialect)),
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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "name", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "age", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=0)]),
            ColumnDefinition(dialect, "email", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value="")]),
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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "author", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "title", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "content", TextType(dialect=dialect)),
            ColumnDefinition(dialect, "published_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "published", BooleanType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=False)]),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
        ],
        table_constraints=[
            ForeignKeyConstraint(dialect, columns=["author"], foreign_key_table="users", foreign_key_columns=["id"],
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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "post_ref", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "author", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "text", TextType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "created_at", TimestampTzType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "updated_at", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "approved", BooleanType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=False)]),
        ],
        table_constraints=[
            ForeignKeyConstraint(dialect, columns=["post_ref"], foreign_key_table="posts", foreign_key_columns=["id"],
                on_delete=_CASCADE),
            ForeignKeyConstraint(dialect, columns=["author"], foreign_key_table="users", foreign_key_columns=["id"],
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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "name", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "item_total", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "remarks", IntegerType(dialect=dialect)),
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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "name", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "tags", ArrayType(TextType(dialect=dialect), dialect=dialect)),
            ColumnDefinition(dialect, "meta", TextType(dialect=dialect)),
            ColumnDefinition(dialect, "description", TextType(dialect=dialect)),
            ColumnDefinition(dialect, "status", TextType(dialect=dialect)),
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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "name", VarCharType(length=255, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "optional_name", VarCharType(length=255, dialect=dialect)),
            ColumnDefinition(dialect, "optional_age", IntegerType(dialect=dialect)),
            ColumnDefinition(dialect, "last_login", TimestampTzType(dialect=dialect)),
            ColumnDefinition(dialect, "is_premium", BooleanType(dialect=dialect)),
            ColumnDefinition(dialect, "unsupported_union", VarCharType(length=255, dialect=dialect)),
            ColumnDefinition(dialect, "custom_bool", VarCharType(length=10, dialect=dialect)),
            ColumnDefinition(dialect, "optional_custom_bool", VarCharType(length=10, dialect=dialect)),
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
            ColumnDefinition(dialect, "order_id", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "product_id", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "quantity", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=1)]),
            ColumnDefinition(dialect, "unit_price", DecimalType(precision=10, scale=2, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
        ],
        table_constraints=[
            TableConstraint(dialect, constraint_type=TableConstraintType.PRIMARY_KEY,
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
            ColumnDefinition(dialect, "store_id", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "product_id", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "batch_id", VarCharType(length=64, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "stock", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL),
                             ColumnConstraint(dialect, ColumnConstraintType.DEFAULT, default_value=0)]),
        ],
        table_constraints=[
            TableConstraint(dialect, constraint_type=TableConstraintType.PRIMARY_KEY,
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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "total", DecimalType(precision=10, scale=2, dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "created_at", TextType(dialect=dialect)),
            ColumnDefinition(dialect, "updated_at", TextType(dialect=dialect)),
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
            ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)]),
            ColumnDefinition(dialect, "name", TextType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "price", FloatType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
            ColumnDefinition(dialect, "quantity", IntegerType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, ColumnConstraintType.NOT_NULL)]),
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
