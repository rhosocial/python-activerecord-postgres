# tests/rhosocial/activerecord_postgres_test/feature/query/cross_schema/test_namespace_isolation.py
"""DML/DQL paths that must not escape their schema namespace.

Two paths used to lose ``__schema_name__``:

* ``SoftDeleteMixin.restore()`` built its ``UpdateOptions`` without
  ``schema_name=``, and its WHERE predicate with bare, unqualified columns. On
  a schema-qualified model the UPDATE landed on whatever same-named table
  ``search_path`` resolved first.
* ``AggregateQueryMixin.aggregate()`` rebuilt the FROM range without
  ``schema_name=`` on the EXPLAIN branch, pairing a three-part column
  reference with an unqualified range.

Both are checked here against a live PostgreSQL. The soft-delete table is
provisioned through the core DDL expression system so the test needs no
provider change; the same table name is created in the default namespace and
in ``ar_crm``, so a missing qualifier has somewhere wrong to write.
"""

from typing import ClassVar, Optional

import pytest

from rhosocial.activerecord.base.field_proxy import FieldProxy
from rhosocial.activerecord.field.soft_delete import (
    DefaultAsyncSoftDeleteMixin,
    DefaultSoftDeleteMixin,
)
from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord

from rhosocial.activerecord_postgres_test.feature.query.cross_schema.models import (
    SCHEMA_A,
)

SOFT_TABLE = "ar_soft_orders"


def _ddl_statements(backend):
    """Build create/drop statements for both namespaces."""
    from rhosocial.activerecord.backend.expression.core import TableExpression
    from rhosocial.activerecord.backend.expression.statements.ddl_table import (
        ColumnConstraint,
        ColumnConstraintType,
        ColumnDefinition,
        CreateTableExpression,
        DropTableExpression,
    )
    from rhosocial.activerecord.backend.expression.types import (
        DateTimeType,
        IntegerType,
        TextType,
    )

    dialect = backend.dialect
    columns = [
        # A plain integer PK rather than an auto-increment one: identity columns
        # render as ``GENERATED ... AS IDENTITY``, which PostgreSQL only accepts
        # from 10.0 and this job's matrix still covers 9.x.
        ColumnDefinition(
            dialect,
            name="id",
            data_type=IntegerType(dialect),
            constraints=[
                ColumnConstraint(
                    dialect, constraint_type=ColumnConstraintType.PRIMARY_KEY
                )
            ],
        ),
        ColumnDefinition(
            dialect,
            name="label",
            data_type=TextType(dialect),
            constraints=[
                ColumnConstraint(dialect, constraint_type=ColumnConstraintType.NOT_NULL)
            ],
        ),
        ColumnDefinition(dialect, name="deleted_at", data_type=DateTimeType(dialect)),
    ]
    statements = []
    for schema in (None, SCHEMA_A):
        ref = (
            TableExpression(dialect, SOFT_TABLE, schema_name=schema)
            if schema
            else SOFT_TABLE
        )
        statements.append(
            DropTableExpression(dialect, ref, if_exists=True, cascade=True).to_sql()[0]
        )
        statements.append(
            CreateTableExpression(
                dialect, ref, columns=columns, if_not_exists=True
            ).to_sql()[0]
        )
    return statements


def _provision(backend) -> None:
    from rhosocial.activerecord.backend.base.execution import ExecutionOptions
    from rhosocial.activerecord.backend.schema import StatementType

    opts = ExecutionOptions(stmt_type=StatementType.DDL)
    for sql in _ddl_statements(backend):
        backend.execute(sql, options=opts)


def _bind(model, source_model) -> None:
    model.__connection_config__ = source_model.__connection_config__
    model.__backend_class__ = source_model.__backend_class__
    model.__backend__ = source_model.__backend__


class ScopedSoftOrder(DefaultSoftDeleteMixin, ActiveRecord):
    """Soft-delete model in the default namespace."""

    __table_name__ = SOFT_TABLE
    __pk_auto_generated__ = False
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    label: str


class ScopedSoftOrderInSchema(DefaultSoftDeleteMixin, ActiveRecord):
    """Identical model, bound to ``ar_crm``."""

    __table_name__ = SOFT_TABLE
    __schema_name__ = SCHEMA_A
    __pk_auto_generated__ = False
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    label: str


class AsyncScopedSoftOrder(DefaultAsyncSoftDeleteMixin, AsyncActiveRecord):
    __table_name__ = SOFT_TABLE
    __pk_auto_generated__ = False
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    label: str


class AsyncScopedSoftOrderInSchema(DefaultAsyncSoftDeleteMixin, AsyncActiveRecord):
    __table_name__ = SOFT_TABLE
    __schema_name__ = SCHEMA_A
    __pk_auto_generated__ = False
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    label: str


def test_soft_delete_restore_stays_namespace_scoped(pg_mixed_schema):
    """``restore()`` must write only into its own namespace."""
    User, _, _ = pg_mixed_schema
    _bind(ScopedSoftOrder, User)
    _bind(ScopedSoftOrderInSchema, User)
    _provision(ScopedSoftOrderInSchema.backend())

    plain = ScopedSoftOrder(id=1, label="plain")
    plain.save()
    scoped = ScopedSoftOrderInSchema(id=1, label="scoped")
    scoped.save()

    assert plain.id == scoped.id, "expected identical PKs in both namespaces"

    scoped.delete()
    assert ScopedSoftOrderInSchema.query_only_deleted().count() == 1
    assert ScopedSoftOrder.query_only_deleted().count() == 0, (
        "deleting in ar_crm must not soft-delete the default-namespace row"
    )

    affected = scoped.restore()

    assert affected == 1
    assert ScopedSoftOrderInSchema.query().count() == 1, (
        "restore() must clear deleted_at inside ar_crm"
    )
    assert ScopedSoftOrder.query().count() == 1, (
        "restore() must not touch the default-namespace row"
    )
    assert ScopedSoftOrder.query().one().deleted_at is None


def test_aggregate_explain_resolves_against_qualified_range(pg_mixed_schema):
    """The EXPLAIN branch must build the same qualified range as plain SELECT."""
    User, _, MixedSchemaOrder = pg_mixed_schema

    MixedSchemaOrder(user_id=1, order_number="expl-1", total_amount=5).save()
    MixedSchemaOrder(user_id=1, order_number="expl-2", total_amount=7).save()

    rows = MixedSchemaOrder.query().explain().aggregate()
    assert isinstance(rows, list), (
        "EXPLAIN over a schema-bound model must return a plan; a wrong range "
        f"raises on PostgreSQL (rows={rows!r})"
    )


@pytest.mark.asyncio
async def test_async_soft_delete_restore_stays_namespace_scoped(pg_async_mixed_schema):
    """Async mirror of the sync restore contract."""
    from rhosocial.activerecord.testsuite.core.registry import get_provider_registry
    from rhosocial.activerecord.testsuite.feature.query.conftest import PROVIDER_KEY_ASYNC

    provider = get_provider_registry().get_provider(PROVIDER_KEY_ASYNC)()
    AsyncUser, _, _ = await provider.setup_mixed_schema_fixtures(pg_async_mixed_schema)
    _bind(AsyncScopedSoftOrder, AsyncUser)
    _bind(AsyncScopedSoftOrderInSchema, AsyncUser)
    _provision(AsyncScopedSoftOrderInSchema.backend())

    plain = AsyncScopedSoftOrder(id=1, label="plain")
    await plain.save()
    scoped = AsyncScopedSoftOrderInSchema(id=1, label="scoped")
    await scoped.save()

    assert plain.id == scoped.id

    await scoped.delete()
    assert await AsyncScopedSoftOrderInSchema.query_only_deleted().count() == 1
    assert await AsyncScopedSoftOrder.query_only_deleted().count() == 0

    affected = await scoped.restore()

    assert affected == 1
    assert await AsyncScopedSoftOrderInSchema.query().count() == 1
    assert await AsyncScopedSoftOrder.query().count() == 1

    cleanup = getattr(provider, "cleanup_after_test", None)
    if cleanup:
        await cleanup(pg_async_mixed_schema)
