"""PostgreSQL provider for shared partition testsuite scenarios."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Type

from rhosocial.activerecord.base.field_proxy import FieldProxy
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    CreateTableExpression,
    PartitionKey,
    PartitionClause,
    PartitionStrategy,
)
from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresAttachPartitionExpression,
    PostgresCreatePartitionExpression,
    PostgresDetachPartitionExpression,
)
from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord
from rhosocial.activerecord.testsuite.feature.partition.interfaces import IPartitionProvider

from .scenarios import get_enabled_scenarios, get_scenario


class PartitionProvider(IPartitionProvider):
    """Concrete partition provider backed by real PostgreSQL scenarios."""

    TABLE_NAME = "ar_testsuite_partition_events"
    PARTITIONS = {
        "p2026_01": ("ar_testsuite_partition_events_p2026_01", "2026-01-01", "2026-02-01"),
        "p2026_02": ("ar_testsuite_partition_events_p2026_02", "2026-02-01", "2026-03-01"),
        "p2026_03": ("ar_testsuite_partition_events_p2026_03", "2026-03-01", "2026-04-01"),
    }

    def __init__(self):
        self._active_backends = []
        self._active_async_backends = []

    def get_test_scenarios(self) -> List[str]:
        """Return PostgreSQL scenarios configured for real backend tests."""
        return list(get_enabled_scenarios().keys())

    def get_partition_capabilities(self, scenario_name: str) -> Dict[str, bool]:
        backend = self._ensure_backend(scenario_name)
        return self._capabilities(backend)

    async def async_get_partition_capabilities(self, scenario_name: str) -> Dict[str, bool]:
        backend = await self._ensure_async_backend(scenario_name)
        return self._capabilities(backend)

    def setup_range_partitioned_event_fixtures(
        self,
        scenario_name: str,
    ) -> Tuple[Type[ActiveRecord], ...]:
        backend = self._ensure_backend(scenario_name)
        self._reset_partition_table(backend)
        Event = self._event_model(backend)
        return (Event,)

    async def setup_async_range_partitioned_event_fixtures(
        self,
        scenario_name: str,
    ) -> Tuple[Type[ActiveRecord], ...]:
        backend = await self._ensure_async_backend(scenario_name)
        await self._reset_partition_table_async(backend)
        Event = self._async_event_model(backend)
        return (Event,)

    def add_future_range_partition(self, scenario_name: str) -> None:
        backend = self._ensure_backend(scenario_name)
        name, from_value, to_value = self.PARTITIONS["p2026_03"]
        sql, params = self._create_range_partition_sql(backend.dialect, name, from_value, to_value)
        backend.execute(sql, params)

    async def async_add_future_range_partition(self, scenario_name: str) -> None:
        backend = await self._ensure_async_backend(scenario_name)
        name, from_value, to_value = self.PARTITIONS["p2026_03"]
        sql, params = self._create_range_partition_sql(backend.dialect, name, from_value, to_value)
        await backend.execute(sql, params)

    def truncate_partition(self, scenario_name: str, partition_key: str) -> None:
        backend = self._ensure_backend(scenario_name)
        backend.execute(f'TRUNCATE TABLE "{self._partition_name(partition_key)}"')

    async def async_truncate_partition(self, scenario_name: str, partition_key: str) -> None:
        backend = await self._ensure_async_backend(scenario_name)
        await backend.execute(f'TRUNCATE TABLE "{self._partition_name(partition_key)}"')

    def detach_partition(self, scenario_name: str, partition_key: str) -> None:
        backend = self._ensure_backend(scenario_name)
        sql, params = self._detach_partition_sql(backend.dialect, self._partition_name(partition_key))
        backend.execute(sql, params)

    async def async_detach_partition(self, scenario_name: str, partition_key: str) -> None:
        backend = await self._ensure_async_backend(scenario_name)
        sql, params = self._detach_partition_sql(backend.dialect, self._partition_name(partition_key))
        await backend.execute(sql, params)

    def attach_partition(self, scenario_name: str, partition_key: str) -> None:
        backend = self._ensure_backend(scenario_name)
        name, from_value, to_value = self.PARTITIONS[partition_key]
        sql, params = self._attach_partition_sql(backend.dialect, name, from_value, to_value)
        backend.execute(sql, params)

    async def async_attach_partition(self, scenario_name: str, partition_key: str) -> None:
        backend = await self._ensure_async_backend(scenario_name)
        name, from_value, to_value = self.PARTITIONS[partition_key]
        sql, params = self._attach_partition_sql(backend.dialect, name, from_value, to_value)
        await backend.execute(sql, params)

    def get_partition_metadata(self, scenario_name: str) -> Dict[str, Any]:
        backend = self._ensure_backend(scenario_name)
        return self._metadata(backend)

    async def async_get_partition_metadata(self, scenario_name: str) -> Dict[str, Any]:
        backend = await self._ensure_async_backend(scenario_name)
        return await self._metadata_async(backend)

    def create_valid_unique_constraint(self, scenario_name: str) -> None:
        backend = self._ensure_backend(scenario_name)
        backend.execute(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{self.TABLE_NAME}_id_created_at_uq" '
            f'ON "{self.TABLE_NAME}" (id, created_at)'
        )

    async def async_create_valid_unique_constraint(self, scenario_name: str) -> None:
        backend = await self._ensure_async_backend(scenario_name)
        await backend.execute(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{self.TABLE_NAME}_id_created_at_uq" '
            f'ON "{self.TABLE_NAME}" (id, created_at)'
        )

    def create_invalid_unique_constraint(self, scenario_name: str) -> None:
        backend = self._ensure_backend(scenario_name)
        backend.execute(
            f'CREATE UNIQUE INDEX "{self.TABLE_NAME}_id_only_uq" '
            f'ON "{self.TABLE_NAME}" (id)'
        )

    async def async_create_invalid_unique_constraint(self, scenario_name: str) -> None:
        backend = await self._ensure_async_backend(scenario_name)
        await backend.execute(
            f'CREATE UNIQUE INDEX "{self.TABLE_NAME}_id_only_uq" '
            f'ON "{self.TABLE_NAME}" (id)'
        )

    def cleanup_after_test(self, scenario_name: str) -> None:
        """Drop partition test tables and disconnect sync backends."""
        for backend in self._active_backends:
            try:
                self._drop_partition_tables(backend)
            finally:
                backend.disconnect()
        self._active_backends.clear()

    async def cleanup_after_test_async(self, scenario_name: str) -> None:
        """Drop partition test tables and disconnect async backends."""
        for backend in self._active_async_backends:
            try:
                await self._drop_partition_tables_async(backend)
            finally:
                await backend.disconnect()
        self._active_async_backends.clear()

    def _ensure_backend(self, scenario_name: str):
        if self._active_backends:
            return self._active_backends[0]
        backend_class, config = get_scenario(scenario_name)
        backend = backend_class(connection_config=config)
        backend.connect()
        backend.introspect_and_adapt()
        self._active_backends.append(backend)
        return backend

    async def _ensure_async_backend(self, scenario_name: str):
        if self._active_async_backends:
            return self._active_async_backends[0]
        _, config = get_scenario(scenario_name)
        backend = AsyncPostgresBackend(connection_config=config)
        await backend.connect()
        await backend.introspect_and_adapt()
        self._active_async_backends.append(backend)
        return backend

    def _capabilities(self, backend) -> Dict[str, bool]:
        supports_creation = backend.dialect.supports_partitioned_table_creation()
        return {
            "range_partitioning": supports_creation and backend.dialect.supports_range_table_partitioning(),
            "add_partition": supports_creation,
            "truncate_partition": supports_creation,
            "detach_partition": supports_creation,
            "attach_partition": supports_creation,
            "partition_introspection": supports_creation,
            "partition_bounds": supports_creation,
            "partitioned_unique_constraint": supports_creation and backend.dialect.version >= (11, 0, 0),
            "unique_requires_partition_key": supports_creation and backend.dialect.version >= (11, 0, 0),
        }

    def _reset_partition_table(self, backend) -> None:
        self._drop_partition_tables(backend)
        sql, params = self._create_parent_sql(backend.dialect)
        backend.execute(sql, params)
        for key in ("p2026_01", "p2026_02"):
            name, from_value, to_value = self.PARTITIONS[key]
            sql, params = self._create_range_partition_sql(backend.dialect, name, from_value, to_value)
            backend.execute(sql, params)

    async def _reset_partition_table_async(self, backend) -> None:
        await self._drop_partition_tables_async(backend)
        sql, params = self._create_parent_sql(backend.dialect)
        await backend.execute(sql, params)
        for key in ("p2026_01", "p2026_02"):
            name, from_value, to_value = self.PARTITIONS[key]
            sql, params = self._create_range_partition_sql(backend.dialect, name, from_value, to_value)
            await backend.execute(sql, params)

    def _drop_partition_tables(self, backend) -> None:
        for table_name in self._all_table_names():
            backend.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')

    async def _drop_partition_tables_async(self, backend) -> None:
        for table_name in self._all_table_names():
            await backend.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')

    def _all_table_names(self) -> Tuple[str, ...]:
        return tuple(partition[0] for partition in self.PARTITIONS.values()) + (self.TABLE_NAME,)

    def _partition_name(self, partition_key: str) -> str:
        return self.PARTITIONS[partition_key][0]

    def _create_parent_sql(self, dialect):
        expr = CreateTableExpression(
            dialect=dialect,
            table=self.TABLE_NAME,
            columns=[
                ColumnDefinition("id", "BIGINT GENERATED BY DEFAULT AS IDENTITY"),
                ColumnDefinition("created_at", "TIMESTAMP NOT NULL"),
                ColumnDefinition("tenant_id", "INTEGER NOT NULL"),
                ColumnDefinition("payload", "TEXT"),
                ColumnDefinition("amount", "NUMERIC(12, 2)"),
            ],
            partition=PartitionClause(
                dialect=dialect,
                strategy=PartitionStrategy.RANGE,
                key=PartitionKey(columns=["created_at"]),
            ),
        )
        return expr.to_sql()

    def _create_range_partition_sql(self, dialect, partition_name: str, from_value: str, to_value: str):
        expr = PostgresCreatePartitionExpression(
            dialect=dialect,
            partition_name=partition_name,
            parent_table=self.TABLE_NAME,
            partition_type="RANGE",
            partition_values={"from": from_value, "to": to_value},
        )
        return expr.to_sql()

    def _detach_partition_sql(self, dialect, partition_name: str):
        expr = PostgresDetachPartitionExpression(
            dialect=dialect,
            partition_name=partition_name,
            parent_table=self.TABLE_NAME,
        )
        return expr.to_sql()

    def _attach_partition_sql(self, dialect, partition_name: str, from_value: str, to_value: str):
        expr = PostgresAttachPartitionExpression(
            dialect=dialect,
            partition_name=partition_name,
            parent_table=self.TABLE_NAME,
            partition_type="RANGE",
            partition_values={"from": from_value, "to": to_value},
        )
        return expr.to_sql()

    def _event_model(self, backend):
        table_name = self.TABLE_NAME

        class PartitionEvent(ActiveRecord):
            __table_name__ = table_name
            __primary_key__ = "id"
            __backend__ = backend
            c: ClassVar[FieldProxy] = FieldProxy()

            id: Optional[int] = None
            created_at: datetime
            tenant_id: int
            payload: Optional[str] = None
            amount: Optional[Decimal] = None

        return PartitionEvent

    def _async_event_model(self, backend):
        table_name = self.TABLE_NAME

        class AsyncPartitionEvent(AsyncActiveRecord):
            __table_name__ = table_name
            __primary_key__ = "id"
            __backend__ = backend
            c: ClassVar[FieldProxy] = FieldProxy()

            id: Optional[int] = None
            created_at: datetime
            tenant_id: int
            payload: Optional[str] = None
            amount: Optional[Decimal] = None

        return AsyncPartitionEvent

    def _metadata(self, backend) -> Dict[str, Any]:
        parent = backend.fetch_one(
            """
            SELECT pg_get_partkeydef(c.oid) AS partition_key
            FROM pg_class c
            WHERE c.relname = %s
            """,
            (self.TABLE_NAME,),
        )
        partitions = backend.fetch_all(self._metadata_partitions_sql(), (self.TABLE_NAME,))
        return self._metadata_dict(parent, partitions)

    async def _metadata_async(self, backend) -> Dict[str, Any]:
        parent = await backend.fetch_one(
            """
            SELECT pg_get_partkeydef(c.oid) AS partition_key
            FROM pg_class c
            WHERE c.relname = %s
            """,
            (self.TABLE_NAME,),
        )
        partitions = await backend.fetch_all(self._metadata_partitions_sql(), (self.TABLE_NAME,))
        return self._metadata_dict(parent, partitions)

    def _metadata_partitions_sql(self) -> str:
        return """
            SELECT child.relname AS name,
                   pg_get_expr(child.relpartbound, child.oid) AS bound
            FROM pg_inherits i
            JOIN pg_class parent ON parent.oid = i.inhparent
            JOIN pg_class child ON child.oid = i.inhrelid
            WHERE parent.relname = %s
            ORDER BY child.relname
        """

    def _metadata_dict(self, parent, partitions) -> Dict[str, Any]:
        partition_key = parent["partition_key"] if parent else ""
        return {
            "is_partitioned": bool(partition_key),
            "strategy": "range" if partition_key.upper().startswith("RANGE") else "unknown",
            "key_columns": ["created_at"] if "created_at" in partition_key.lower() else [],
            "partitions": [
                {"name": row["name"], "bound": row["bound"]}
                for row in partitions
            ],
        }
