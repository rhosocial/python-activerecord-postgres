# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/functions/test_timescaledb_functions.py
"""SQL construction tests for the TimescaleDB factory."""

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.timescaledb import create_hypertable
from rhosocial.activerecord.backend.impl.postgres.protocols.base import PostgresExtensionInfo


@pytest.fixture
def dialect():
    return PostgresDialect()


class TestCreateHypertable:
    def test_default(self, dialect):
        assert create_hypertable(dialect, "metrics", "ts").to_sql() == (
            "CREATE_HYPERTABLE(%s::REGCLASS, 'ts')",
            ("metrics",),
        )

    def test_with_partitioning(self, dialect):
        assert create_hypertable(
            dialect, "metrics", "ts", partitioning_column="device", number_partitions=4
        ).to_sql() == (
            "CREATE_HYPERTABLE(%s::REGCLASS, 'ts', 'device', %s)",
            ("metrics", 4),
        )

    def test_partitioning_column_without_count(self, dialect):
        assert create_hypertable(dialect, "metrics", "ts", partitioning_column="device").to_sql() == (
            "CREATE_HYPERTABLE(%s::REGCLASS, 'ts', 'device')",
            ("metrics",),
        )

    @pytest.mark.parametrize("value", [-1, True, "4"])
    def test_number_partitions_validation(self, dialect, value):
        with pytest.raises((TypeError, ValueError)):
            create_hypertable(
                dialect, "metrics", "ts", partitioning_column="device", number_partitions=value
            )

    def test_number_partitions_requires_column(self, dialect):
        with pytest.raises(ValueError):
            create_hypertable(dialect, "metrics", "ts", number_partitions=4)


class TestCapabilityGate:
    @pytest.mark.parametrize(
        ("version", "expected"),
        [("2.32.1", True), ("1.7.4", False)],
    )
    def test_version_gate(self, version, expected):
        dialect = PostgresDialect()
        dialect._extensions = {
            "timescaledb": PostgresExtensionInfo(
                name="timescaledb", installed=True, available=True, version=version
            )
        }
        assert dialect.supports_timescaledb() is expected

    def test_not_installed(self):
        assert PostgresDialect().supports_timescaledb() is False
