# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/ddl/test_auto_increment_ddl.py
"""Regression tests for DDL compilation of auto-increment, standard types,
boolean defaults, and timestamps.

Covers bugs found in rbac cross-backend testing:
- is_auto_increment=True on PRIMARY_KEY must produce a SERIAL type (was
  missing — caused INSERT id=NULL NOT NULL violation).
- format_create_table_statement must return a string for all types.
- BooleanType with DEFAULT True must be accepted.
- TimestampType must map to TIMESTAMP.

An auto-increment constraint compiles to the ``SERIAL`` pseudo-type on every
version: SERIAL predates the identity clause, its observable behaviour is the
one the constraint asks for, and one spelling keeps 9 through 19 identical.
The SQL-standard identity clause remains available to a caller that asks for
it explicitly (``IdentityClause``); it is simply not what an auto-increment
request means.
"""
from rhosocial.activerecord.backend.expression.statements import auto_increment_parts  # noqa: E402

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import (
    CreateTableStatement,
    ColumnDefinition,
    ColumnConstraint,
    ColumnDefaultConstraint,
    ColumnNotNullConstraint,
    ColumnPrimaryKeyConstraint,
)
from rhosocial.activerecord.backend.expression.types import (
    BooleanType, IntegerType, TextType, TimestampType, VarCharType,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression.objects import Table


@pytest.fixture
def dialect():
    # An adapted dialect: the identity probe carries the measured version
    # boundary (PostgreSQL 10), so an un-adapted dialect cannot answer it.
    return PostgresDialect(version=(16, 0, 0))


def _id_column(dialect, data_type):
    suggested_type, attributes = auto_increment_parts(dialect, data_type)
    return ColumnDefinition(
        dialect, "id", suggested_type,
        constraints=[ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))],
        attributes=attributes,
    )


def _build_table(dialect):
    return CreateTableStatement(
        dialect=dialect, table=Table(dialect, 'test_tbl'),
        columns=[
            _id_column(dialect, IntegerType(dialect=dialect)),
            ColumnDefinition(dialect, "name", TextType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))]),
            ColumnDefinition(dialect, "flag", BooleanType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect)),
                             ColumnConstraint(dialect, branch=ColumnDefaultConstraint(dialect, True))]),
            ColumnDefinition(dialect, "code", VarCharType(length=16, dialect=dialect)),
            ColumnDefinition(dialect, "created_at", TimestampType(dialect=dialect)),
        ],
    )


class TestAutoIncrementDDL:

    def test_auto_increment_in_ddl(self, dialect):
        sql, _ = _build_table(dialect).to_sql()
        assert "SERIAL" in sql.upper(), f"auto-increment missing: {sql}"
        assert "GENERATED" not in sql.upper(), f"unexpected identity clause: {sql}"

    def test_all_standard_types_compile(self, dialect):
        sql, params = _build_table(dialect).to_sql()
        assert isinstance(sql, str)
        assert isinstance(params, tuple)

    def test_boolean_default_true(self, dialect):
        expr = CreateTableStatement(
            dialect=dialect, table=Table(dialect, 'bool_test'),
            columns=[
                _id_column(dialect, IntegerType(dialect=dialect)),
                ColumnDefinition(dialect, "flag", BooleanType(dialect=dialect),
                    constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect)),
                                 ColumnConstraint(dialect, branch=ColumnDefaultConstraint(dialect, True))]),
            ],
        )
        sql, _ = expr.to_sql()
        assert "BOOLEAN" in sql.upper()

    def test_timestamp_type(self, dialect):
        expr = CreateTableStatement(
            dialect=dialect, table=Table(dialect, 'ts_test'),
            columns=[
                _id_column(dialect, IntegerType(dialect=dialect)),
                ColumnDefinition(dialect, "created_at", TimestampType(dialect=dialect)),
            ],
        )
        sql, _ = expr.to_sql()
        assert "TIMESTAMP" in sql.upper()


class TestAutoIncrementIsSerial:
    """An auto-increment request is a SERIAL type, on every version."""

    @pytest.mark.parametrize(
        "version", [(9, 6, 0), (10, 0, 0), (14, 0, 0), (18, 0, 0)], ids=lambda v: str(v)
    )
    def test_serial_on_every_version(self, version):
        sql, _ = _build_table(PostgresDialect(version=version)).to_sql()
        assert "SERIAL" in sql.upper()
        assert "GENERATED" not in sql.upper()

    def test_integer_widths_map_to_their_serial(self):
        dialect = PostgresDialect(version=(16, 0, 0))
        from rhosocial.activerecord.backend.expression.types import (
            BigIntType,
            SmallIntType,
        )

        for core_type, expected in (
            (SmallIntType, "SMALLSERIAL"),
            (BigIntType, "BIGSERIAL"),
        ):
            expr = CreateTableStatement(
                dialect=dialect,
                table=Table(dialect, "serial_tbl"),
                columns=[
                    _id_column(dialect, core_type(dialect=dialect))
                ],
            )
            sql, _ = expr.to_sql()
            assert expected in sql.upper(), sql

    def test_non_integer_auto_increment_is_refused(self):
        """SERIAL has three widths; anything else is refused, not approximated."""
        dialect = PostgresDialect(version=(16, 0, 0))
        # The backend suggestion is consulted when the column definition is
        # built, so the refusal happens there rather than at render time.
        with pytest.raises(UnsupportedFeatureError, match="AUTO_INCREMENT column"):
            _id_column(dialect, TextType(dialect=dialect))

    def test_serial_type_still_spellable_explicitly(self):
        """A caller that asks for SERIAL by type keeps that path."""
        dialect = PostgresDialect(version=(9, 6, 0))
        from rhosocial.activerecord.backend.impl.postgres.expression.types import (
            PostgresSerialType,
        )
        serial = CreateTableStatement(
            dialect=dialect, table=Table(dialect, 'serial_tbl'),
            columns=[ColumnDefinition(dialect, "id", PostgresSerialType(dialect=dialect))],
        )
        sql, _ = serial.to_sql()
        assert "SERIAL" in sql.upper()
