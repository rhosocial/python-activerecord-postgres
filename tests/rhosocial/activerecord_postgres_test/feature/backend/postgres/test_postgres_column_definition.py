# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_postgres_column_definition.py
"""Tests for the PostgreSQL-specific column definition expressions."""

import pytest

from rhosocial.activerecord.backend.expression import ColumnDefinition
from rhosocial.activerecord.backend.expression.types import TextType
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresColumnDefinition,
    PostgresColumnOptions,
    PostgresColumnStorage,
)


@pytest.fixture
def dialect():
    return PostgresDialect((15, 0, 0))


def _column(dialect, **kwargs):
    return PostgresColumnDefinition(dialect, "body", TextType(dialect), **kwargs)


def test_derives_generic_column_definition():
    assert ColumnDefinition in PostgresColumnDefinition.__mro__


def test_compression(dialect):
    sql, _ = _column(dialect, compression="lz4").to_sql()
    assert sql == '"body" TEXT COMPRESSION lz4'


def test_storage(dialect):
    sql, _ = _column(dialect, storage=PostgresColumnStorage.EXTENDED).to_sql()
    assert sql == '"body" TEXT STORAGE EXTENDED'


def test_statistics(dialect):
    sql, _ = _column(dialect, statistics=500).to_sql()
    assert sql == '"body" TEXT STATISTICS 500'


def test_combined(dialect):
    sql, _ = _column(
        dialect,
        compression="pglz",
        storage=PostgresColumnStorage.EXTERNAL,
        statistics=100,
    ).to_sql()
    assert sql == '"body" TEXT COMPRESSION pglz STORAGE EXTERNAL STATISTICS 100'


def test_generic_column_comment_raises_on_postgres(dialect):
    # §5.19: PostgreSQL has no inline COMMENT syntax; a comment on a column
    # definition raises instead of rendering SQL PostgreSQL would reject.
    from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

    generic = ColumnDefinition(dialect, "body", TextType(dialect), comment="c")
    with pytest.raises(UnsupportedFeatureError, match="COLUMN COMMENT"):
        generic.to_sql()


def test_invalid_storage_type(dialect):
    with pytest.raises(TypeError, match="PostgresColumnStorage"):
        _column(dialect, storage="EXTENDED")


def test_statistics_out_of_range():
    with pytest.raises(ValueError, match="0..10000"):
        PostgresColumnOptions(statistics=20000)


def test_options_select_postgres_column_class():
    assert PostgresColumnOptions(compression="lz4").column_definition_class() is (
        PostgresColumnDefinition
    )


def test_options_apply_to(dialect):
    options = PostgresColumnOptions(
        compression="lz4", storage=PostgresColumnStorage.MAIN, statistics=10
    )
    col = PostgresColumnDefinition(dialect, "c", TextType(dialect))
    options.apply_to(col)
    assert col.compression == "lz4"
    assert col.storage is PostgresColumnStorage.MAIN
    assert col.statistics == 10


def test_options_apply_to_rejects_generic_column(dialect):
    options = PostgresColumnOptions(compression="lz4")
    generic = ColumnDefinition(dialect, "c", TextType(dialect))
    with pytest.raises(TypeError, match="PostgresColumnDefinition"):
        options.apply_to(generic)
