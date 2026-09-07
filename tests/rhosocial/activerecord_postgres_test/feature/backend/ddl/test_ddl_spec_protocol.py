# tests/rhosocial/activerecord_postgres_test/feature/backend/ddl/test_ddl_spec_protocol.py
"""PostgreSQL DDL feature-spec claiming tests (``build_spec``).

Covers the PostgreSQL-specific Specs (declarative partitions, sequence
defaults) and the generic Specs on the PostgreSQL dialect:

- PG partition Specs translate to the core ``PartitionClause`` (RANGE/LIST/HASH).
- ``PostgresSequenceDefault`` translates to a ``DEFAULT nextval(...)`` constraint.
- Generic Specs still translate via the core ``DDLSpecBuildingMixin``.
- Foreign Specs (``PartitionSpec`` marker, unknown objects) return ``None``.
"""

import pytest

from rhosocial.activerecord.base import (
    CheckSpec,
    PartitionSpec,
    PrimaryKeySpec,
    UniqueSpec,
)
from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.ddl_spec import (
    PostgresArrayColumnSpec,
    PostgresHashPartition,
    PostgresHstoreColumnSpec,
    PostgresJsonbColumnSpec,
    PostgresListPartition,
    PostgresNetworkColumnSpec,
    PostgresRangePartition,
    PostgresSequenceDefault,
    PostgresTsVectorColumnSpec,
)


@pytest.fixture
def dialect():
    d = PostgresDialect()
    d.version = (16, 0, 0)
    return d


class TestProtocolConformance:
    def test_build_spec_returns_none_for_unknown(self, dialect):
        assert dialect.build_spec(object()) is None

    def test_build_spec_returns_none_for_base_partition_marker(self, dialect):
        assert dialect.build_spec(PartitionSpec()) is None


class TestGenericSpecTranslation:
    def test_unique_spec(self, dialect):
        result = dialect.build_spec(UniqueSpec(["a", "b"], name="uq_ab"))
        assert result.columns == ["a", "b"]

    def test_check_spec_lazy(self, dialect):
        result = dialect.build_spec(
            CheckSpec(lambda d: Column(d, "age") >= 18, name="ck_age")
        )
        assert result.check_condition is not None

    def test_primary_key_single(self, dialect):
        result = dialect.build_spec(PrimaryKeySpec(["id"]))
        from rhosocial.activerecord.backend.expression.statements import (
            ColumnConstraint,
            ColumnConstraintType,
        )
        assert isinstance(result, ColumnConstraint)
        assert result.constraint_type == ColumnConstraintType.PRIMARY_KEY


class TestPostgresPartitionSpecs:
    def test_range_partition(self, dialect):
        expr = dialect.build_spec(PostgresRangePartition("created_at"))
        from rhosocial.activerecord.backend.expression.statements import PartitionClause
        assert isinstance(expr, PartitionClause)
        sql, _ = expr.to_sql()
        assert "PARTITION BY RANGE" in sql

    def test_list_partition(self, dialect):
        expr = dialect.build_spec(PostgresListPartition("status"))
        sql, _ = expr.to_sql()
        assert "PARTITION BY LIST" in sql

    def test_hash_partition(self, dialect):
        expr = dialect.build_spec(PostgresHashPartition("id"))
        sql, _ = expr.to_sql()
        assert "PARTITION BY HASH" in sql

    def test_partition_requires_column(self):
        with pytest.raises(ValueError):
            PostgresRangePartition("")


class TestPostgresSequenceDefault:
    def test_sequence_default(self, dialect):
        result = dialect.build_spec(
            PostgresSequenceDefault("id", "users_id_seq")
        )
        from rhosocial.activerecord.backend.expression.statements import (
            ColumnConstraint,
            ColumnConstraintType,
        )
        assert isinstance(result, ColumnConstraint)
        assert result.constraint_type == ColumnConstraintType.DEFAULT
        sql, _ = result.default_value.to_sql()
        assert sql == "nextval('users_id_seq')"

    def test_sequence_default_derived_name(self, dialect):
        result = dialect.build_spec(PostgresSequenceDefault("id"))
        sql, _ = result.default_value.to_sql()
        assert sql == "nextval('id_seq')"


class TestBuildSpecFeedsCreateTable:
    def test_partition_spec_feeds_create_table(self, dialect):
        from rhosocial.activerecord.backend.expression.statements import (
            ColumnDefinition,
            CreateTableExpression,
        )
        from rhosocial.activerecord.backend.expression.types import IntegerType

        part = dialect.build_spec(PostgresRangePartition("created_at"))
        pk = dialect.build_spec(PrimaryKeySpec(["id"]))
        seq = dialect.build_spec(PostgresSequenceDefault("id", "orders_id_seq"))
        cols = [
            ColumnDefinition("id", IntegerType(), constraints=[pk, seq]),
            ColumnDefinition("created_at", IntegerType()),
        ]
        expr = CreateTableExpression(
            dialect=dialect,
            table="orders",
            columns=cols,
            partition=part,
        )
        sql, _ = expr.to_sql()
        assert "PARTITION BY RANGE" in sql
        assert "nextval('orders_id_seq')" in sql


class TestPostgresTypeSpecs:
    """PostgreSQL column-type Specs (hstore / jsonb / tsvector / network / array)."""

    def test_hstore_column(self, dialect):
        result = dialect.build_spec(PostgresHstoreColumnSpec("attrs"))
        sql, _ = result.patched_data_type.to_sql(dialect)
        assert sql == "HSTORE"

    def test_jsonb_column(self, dialect):
        result = dialect.build_spec(PostgresJsonbColumnSpec("data"))
        sql, _ = result.patched_data_type.to_sql(dialect)
        assert sql == "JSONB"

    def test_tsvector_column(self, dialect):
        result = dialect.build_spec(PostgresTsVectorColumnSpec("doc"))
        sql, _ = result.patched_data_type.to_sql(dialect)
        assert sql == "TSVECTOR"

    def test_network_column(self, dialect):
        result = dialect.build_spec(PostgresNetworkColumnSpec("ip", kind="INET"))
        sql, _ = result.patched_data_type.to_sql(dialect)
        assert sql == "INET"

    def test_network_invalid_kind(self):
        with pytest.raises(ValueError):
            PostgresNetworkColumnSpec("ip", kind="BOGUS")

    def test_array_column(self, dialect):
        from rhosocial.activerecord.backend.expression.types import TextType
        result = dialect.build_spec(
            PostgresArrayColumnSpec("tags", TextType())
        )
        sql, _ = result.patched_data_type.to_sql(dialect)
        assert sql == "TEXT[]"

    def test_model_type_specs_render(self, dialect):
        from rhosocial.activerecord.backend.expression.types import TextType
        from rhosocial.activerecord.model import ActiveRecord

        class T(ActiveRecord):
            __table_name__ = "t"
            __table_constraints__ = [
                PostgresHstoreColumnSpec("attrs"),
                PostgresJsonbColumnSpec("data"),
                PostgresTsVectorColumnSpec("doc"),
                PostgresNetworkColumnSpec("ip", kind="INET"),
                PostgresArrayColumnSpec("tags", TextType()),
            ]
            attrs: object
            data: object
            doc: object
            ip: str
            tags: list

        expr = T.generate_create_table(dialect)
        sql, _ = expr.to_sql()
        assert "HSTORE" in sql
        assert "JSONB" in sql
        assert "TSVECTOR" in sql
        assert "INET" in sql
        assert "TEXT[]" in sql