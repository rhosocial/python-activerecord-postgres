"""PostgreSQL provider for backend direct benchmark tests."""

from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.testsuite.benchmark.backend.interfaces import (
    BackendBenchmarkContext,
)
from rhosocial.activerecord.testsuite.benchmark.crud.fixtures.data import (
    make_user_payloads,
    payload_count_for_size,
)

from .scenarios import get_enabled_scenarios, get_scenario


class BackendBenchmarkProvider:
    def __init__(self):
        self._active_backends = []
        self._active_async_backends = []

    def get_benchmark_scenarios(self):
        return list(get_enabled_scenarios().keys())

    def setup_benchmark_sync(self, scenario: str, size: str):
        from rhosocial.activerecord.backend.impl.postgres import PostgresBackend

        _, config = get_scenario(scenario)
        backend = PostgresBackend(connection_config=config)
        self._initialize_schema(backend)
        payloads = make_user_payloads(payload_count_for_size(size))
        record_ids = self._seed_sync(backend, payloads)
        self._active_backends.append(backend)
        return BackendBenchmarkContext(
            scenario=scenario,
            size=size,
            backend=backend,
            payloads=payloads,
            record_ids=record_ids,
            sql=self._sql_templates(),
            params_factory=self._params_factory,
            backend_namespace="rhosocial.activerecord.backend.impl.postgres",
            backend_name="postgres",
        )

    def teardown_benchmark_sync(self, scenario: str, context: BackendBenchmarkContext) -> None:
        self._cleanup_sync()

    async def setup_benchmark_async(self, scenario: str, size: str):
        from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend

        _, config = get_scenario(scenario)
        backend = AsyncPostgresBackend(connection_config=config)
        await self._initialize_schema_async(backend)
        payloads = make_user_payloads(payload_count_for_size(size))
        record_ids = await self._seed_async(backend, payloads)
        self._active_async_backends.append(backend)
        return BackendBenchmarkContext(
            scenario=scenario,
            size=size,
            backend=backend,
            payloads=payloads,
            record_ids=record_ids,
            sql=self._sql_templates(),
            params_factory=self._params_factory,
            backend_namespace="rhosocial.activerecord.backend.impl.postgres",
            backend_name="postgres",
        )

    async def teardown_benchmark_async(self, scenario: str, context: BackendBenchmarkContext) -> None:
        await self._cleanup_async()

    def _initialize_schema(self, backend):
        options = ExecutionOptions(stmt_type=StatementType.DDL)
        backend.execute("DROP TABLE IF EXISTS benchmark_users", options=options)
        backend.execute(self._schema_sql(), options=options)

    async def _initialize_schema_async(self, backend):
        options = ExecutionOptions(stmt_type=StatementType.DDL)
        await backend.execute("DROP TABLE IF EXISTS benchmark_users", options=options)
        await backend.execute(self._schema_sql(), options=options)

    def _seed_sync(self, backend, payloads):
        record_ids = []
        for payload in payloads:
            result = backend.execute(
                self._sql_templates()["insert"],
                self._params_factory("insert", payload),
            )
            if result.affected_rows != 1 or not result.data:
                raise AssertionError("failed to seed sync backend benchmark row")
            record_ids.append(result.data[0]["id"])
        return record_ids

    async def _seed_async(self, backend, payloads):
        record_ids = []
        for payload in payloads:
            result = await backend.execute(
                self._sql_templates()["insert"],
                self._params_factory("insert", payload),
            )
            if result.affected_rows != 1 or not result.data:
                raise AssertionError("failed to seed async backend benchmark row")
            record_ids.append(result.data[0]["id"])
        return record_ids

    def _sql_templates(self):
        return {
            "insert": """
INSERT INTO benchmark_users (
    username, email, age, balance, notes, is_active, created_at, updated_at
) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
RETURNING id
""",
            "find_one": "SELECT * FROM benchmark_users WHERE id = %s",
            "update": "UPDATE benchmark_users SET username = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
            "delete": "DELETE FROM benchmark_users WHERE id = %s",
        }

    def _params_factory(self, operation, payload):
        if operation != "insert":
            raise ValueError(f"unsupported backend benchmark operation: {operation}")
        return (
            payload["username"],
            payload["email"],
            payload["age"],
            payload["balance"],
            payload["notes"],
            payload["is_active"],
        )

    def _schema_sql(self):
        return """
CREATE TABLE benchmark_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    age INTEGER,
    balance DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    notes TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
"""

    def _cleanup_sync(self):
        for backend in self._active_backends:
            try:
                backend.disconnect()
            except Exception:
                pass
        self._active_backends.clear()

    async def _cleanup_async(self):
        for backend in self._active_async_backends:
            try:
                await backend.disconnect()
            except Exception:
                pass
        self._active_async_backends.clear()
