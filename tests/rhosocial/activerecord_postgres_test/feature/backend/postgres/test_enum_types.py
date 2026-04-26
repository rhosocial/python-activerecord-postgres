# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_enum_types.py
"""Unit tests for PostgreSQL ENUM types.

Tests for:
- PostgresEnumType expression class (BaseExpression)
- PostgresEnumAdapter conversion
- EnumTypeSupport protocol implementation
"""
import pytest
from enum import Enum
from typing import Tuple

from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.types import PostgresEnumType
from rhosocial.activerecord.backend.impl.postgres.adapters import PostgresEnumAdapter
from rhosocial.activerecord.backend.impl.postgres.protocols import PostgresEnumTypeSupport
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresCreateEnumTypeExpression,
    PostgresDropEnumTypeExpression,
    PostgresAlterEnumTypeAddValueExpression,
    PostgresAlterEnumTypeRenameValueExpression,
)


class StatusEnum(Enum):
    """Test Python Enum."""
    PENDING = 'pending'
    PROCESSING = 'processing'
    READY = 'ready'
    FAILED = 'failed'


class TestPostgresEnumType:
    """Tests for PostgresEnumType expression class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = PostgresDialect()

    def test_create_enum_type(self):
        """Test creating an enum type."""
        enum_type = PostgresEnumType(
            dialect=self.dialect,
            name='video_status',
            values=['pending', 'processing', 'ready', 'failed']
        )
        assert enum_type.name == 'video_status'
        assert enum_type.values == ['pending', 'processing', 'ready', 'failed']
        assert enum_type.schema is None

    def test_create_enum_type_with_schema(self):
        """Test creating an enum type with schema."""
        enum_type = PostgresEnumType(
            dialect=self.dialect,
            name='video_status',
            values=['draft', 'published'],
            schema='app'
        )
        assert enum_type.schema == 'app'

    def test_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="name cannot be empty"):
            PostgresEnumType(dialect=self.dialect, name='', values=['a', 'b'])

    def test_empty_values_raises_error(self):
        """Test that empty values raises ValueError."""
        with pytest.raises(ValueError, match="at least one value"):
            PostgresEnumType(dialect=self.dialect, name='test', values=[])

    def test_duplicate_values_raises_error(self):
        """Test that duplicate values raises ValueError."""
        with pytest.raises(ValueError, match="must be unique"):
            PostgresEnumType(dialect=self.dialect, name='test', values=['a', 'b', 'a'])

    def test_to_sql(self):
        """Test SQL type reference generation (returns Tuple[str, tuple])."""
        enum_type = PostgresEnumType(
            dialect=self.dialect,
            name='status',
            values=['a', 'b']
        )
        result = enum_type.to_sql()
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] == 'status'
        assert result[1] == ()

    def test_to_sql_with_schema(self):
        """Test SQL type reference with schema."""
        enum_type = PostgresEnumType(
            dialect=self.dialect,
            name='status',
            values=['a', 'b'],
            schema='app'
        )
        result = enum_type.to_sql()
        assert result == ('app.status', ())

    def test_create_type_sql(self):
        """Test CREATE TYPE SQL generation using PostgresCreateEnumTypeExpression."""
        expr = PostgresCreateEnumTypeExpression(
            dialect=self.dialect,
            name='status',
            values=['pending', 'ready']
        )
        result = expr.to_sql()
        assert isinstance(result, tuple)
        sql, params = result
        assert sql == "CREATE TYPE status AS ENUM ('pending', 'ready')"
        assert params == ()

    def test_create_type_sql_if_not_exists(self):
        """Test CREATE TYPE IF NOT EXISTS SQL generation."""
        expr = PostgresCreateEnumTypeExpression(
            dialect=self.dialect,
            name='status',
            values=['a', 'b'],
            if_not_exists=True
        )
        sql, params = expr.to_sql()
        assert 'IF NOT EXISTS' in sql
        assert params == ()

    def test_drop_type_sql(self):
        """Test DROP TYPE SQL generation using PostgresDropEnumTypeExpression."""
        expr = PostgresDropEnumTypeExpression(
            dialect=self.dialect,
            name='status'
        )
        sql, params = expr.to_sql()
        assert sql == "DROP TYPE status"
        assert params == ()

    def test_drop_type_sql_if_exists(self):
        """Test DROP TYPE IF EXISTS SQL generation."""
        expr = PostgresDropEnumTypeExpression(
            dialect=self.dialect,
            name='status',
            if_exists=True
        )
        sql, params = expr.to_sql()
        assert sql == "DROP TYPE IF EXISTS status"
        assert params == ()

    def test_drop_type_sql_cascade(self):
        """Test DROP TYPE CASCADE SQL generation."""
        expr = PostgresDropEnumTypeExpression(
            dialect=self.dialect,
            name='status',
            cascade=True
        )
        sql, params = expr.to_sql()
        assert 'CASCADE' in sql
        assert params == ()

    def test_add_value_sql(self):
        """Test ALTER TYPE ADD VALUE SQL generation using PostgresAlterEnumTypeAddValueExpression."""
        expr = PostgresAlterEnumTypeAddValueExpression(
            dialect=self.dialect,
            type_name='status',
            new_value='failed'
        )
        sql, params = expr.to_sql()
        assert sql == "ALTER TYPE status ADD VALUE 'failed'"
        assert params == ()

    def test_add_value_sql_before(self):
        """Test ADD VALUE with BEFORE clause."""
        expr = PostgresAlterEnumTypeAddValueExpression(
            dialect=self.dialect,
            type_name='status',
            new_value='processing',
            before='ready'
        )
        sql, params = expr.to_sql()
        assert "BEFORE 'ready'" in sql
        assert params == ()

    def test_add_value_sql_after(self):
        """Test ADD VALUE with AFTER clause."""
        expr = PostgresAlterEnumTypeAddValueExpression(
            dialect=self.dialect,
            type_name='status',
            new_value='processing',
            after='pending'
        )
        sql, params = expr.to_sql()
        assert "AFTER 'pending'" in sql
        assert params == ()

    def test_add_value_both_before_and_after_raises_error(self):
        """Test that specifying both before and after raises error."""
        with pytest.raises(ValueError, match="Cannot specify both 'before' and 'after'"):
            PostgresAlterEnumTypeAddValueExpression(
                dialect=self.dialect,
                type_name='status',
                new_value='processing',
                before='ready',
                after='pending'
            )

    def test_rename_value_sql(self):
        """Test ALTER TYPE RENAME VALUE SQL generation using PostgresAlterEnumTypeRenameValueExpression."""
        expr = PostgresAlterEnumTypeRenameValueExpression(
            dialect=self.dialect,
            type_name='status',
            old_value='pending',
            new_value='draft'
        )
        sql, params = expr.to_sql()
        assert sql == "ALTER TYPE status RENAME VALUE 'pending' TO 'draft'"
        assert params == ()

    def test_rename_value_empty_raises_error(self):
        """Test that empty value raises error."""
        with pytest.raises(ValueError, match="cannot be empty"):
            PostgresAlterEnumTypeRenameValueExpression(
                dialect=self.dialect,
                type_name='status',
                old_value='',
                new_value='draft'
            )

    def test_validate_value(self):
        """Test value validation."""
        enum_type = PostgresEnumType(
            dialect=self.dialect,
            name='status',
            values=['pending', 'ready']
        )
        assert enum_type.validate_value('pending') is True
        assert enum_type.validate_value('ready') is True
        assert enum_type.validate_value('failed') is False

    def test_equality(self):
        """Test enum type equality."""
        e1 = PostgresEnumType(dialect=self.dialect, name='status', values=['a', 'b'])
        e2 = PostgresEnumType(dialect=self.dialect, name='status', values=['a', 'b'])
        e3 = PostgresEnumType(dialect=self.dialect, name='status', values=['a', 'c'])
        assert e1 == e2
        assert e1 != e3

    def test_hash(self):
        """Test enum type hashability."""
        e1 = PostgresEnumType(dialect=self.dialect, name='status', values=['a', 'b'])
        e2 = PostgresEnumType(dialect=self.dialect, name='status', values=['a', 'b'])
        assert hash(e1) == hash(e2)
        assert len({e1, e2}) == 1

    def test_str_representation(self):
        """Test string representation."""
        enum_type = PostgresEnumType(
            dialect=self.dialect,
            name='status',
            values=['a', 'b']
        )
        assert str(enum_type) == 'status'

    def test_str_representation_with_schema(self):
        """Test string representation with schema."""
        enum_type = PostgresEnumType(
            dialect=self.dialect,
            name='status',
            values=['a', 'b'],
            schema='app'
        )
        assert str(enum_type) == 'app.status'


class TestPostgresEnumAdapter:
    """Tests for PostgresEnumAdapter."""

    def test_adapter_supported_types(self):
        """Test supported types property."""
        adapter = PostgresEnumAdapter()
        supported = adapter.supported_types
        assert str in supported

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresEnumAdapter()
        result = adapter.to_database('pending', str)
        assert result == 'pending'

    def test_to_database_python_enum(self):
        """Test converting Python Enum to database."""
        adapter = PostgresEnumAdapter()
        result = adapter.to_database(StatusEnum.PENDING, str)
        assert result == 'PENDING'

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresEnumAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_type(self):
        """Test converting invalid type raises error."""
        adapter = PostgresEnumAdapter()
        with pytest.raises(TypeError):
            adapter.to_database(123, str)

    def test_to_database_with_validation(self):
        """Test converting with enum type validation."""
        adapter = PostgresEnumAdapter()
        dialect = PostgresDialect()
        enum_type = PostgresEnumType(
            dialect=dialect,
            name='status',
            values=['pending', 'ready']
        )
        result = adapter.to_database('pending', str, options={'enum_type': enum_type})
        assert result == 'pending'

    def test_to_database_validation_fails(self):
        """Test validation fails for invalid value."""
        adapter = PostgresEnumAdapter()
        dialect = PostgresDialect()
        enum_type = PostgresEnumType(
            dialect=dialect,
            name='status',
            values=['pending', 'ready']
        )
        with pytest.raises(ValueError, match="Invalid enum value"):
            adapter.to_database('failed', str, options={'enum_type': enum_type})

    def test_from_database_string(self):
        """Test converting from database to string."""
        adapter = PostgresEnumAdapter()
        result = adapter.from_database('pending', str)
        assert result == 'pending'

    def test_from_database_to_python_enum(self):
        """Test converting from database to Python Enum."""
        adapter = PostgresEnumAdapter()
        result = adapter.from_database('PENDING', StatusEnum, options={'enum_class': StatusEnum})
        assert result == StatusEnum.PENDING

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresEnumAdapter()
        result = adapter.from_database(None, str)
        assert result is None

    def test_from_database_invalid_type(self):
        """Test converting invalid type from database."""
        adapter = PostgresEnumAdapter()
        with pytest.raises(TypeError):
            adapter.from_database(123, str)

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresEnumAdapter()
        values = ['pending', StatusEnum.READY, None]
        results = adapter.to_database_batch(values, str)
        assert results[0] == 'pending'
        assert results[1] == 'READY'
        assert results[2] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresEnumAdapter()
        values = ['pending', 'ready', None]
        results = adapter.from_database_batch(values, str)
        assert results[0] == 'pending'
        assert results[1] == 'ready'
        assert results[2] is None


class TestPostgresEnumTypeSupport:
    """Tests for PostgresEnumTypeSupport protocol implementation."""

    def test_dialect_implements_postgres_enum_type_support(self):
        """Test that PostgresDialect implements PostgresEnumTypeSupport."""
        dialect = PostgresDialect()
        # Check via MRO
        from rhosocial.activerecord.backend.impl.postgres.protocols import PostgresEnumTypeSupport
        assert PostgresEnumTypeSupport in dialect.__class__.__mro__

    def test_create_enum_type_method(self):
        """Test create_enum_type method."""
        dialect = PostgresDialect()
        sql = dialect.create_enum_type('status', ['pending', 'ready'])
        assert sql == "CREATE TYPE status AS ENUM ('pending', 'ready')"

    def test_create_enum_type_with_schema(self):
        """Test create_enum_type with schema."""
        dialect = PostgresDialect()
        sql = dialect.create_enum_type('status', ['pending', 'ready'], schema='app')
        assert sql == "CREATE TYPE app.status AS ENUM ('pending', 'ready')"

    def test_create_enum_type_if_not_exists(self):
        """Test create_enum_type with IF NOT EXISTS."""
        dialect = PostgresDialect()
        sql = dialect.create_enum_type('status', ['pending', 'ready'], if_not_exists=True)
        assert 'IF NOT EXISTS' in sql

    def test_drop_enum_type_method(self):
        """Test drop_enum_type method."""
        dialect = PostgresDialect()
        sql = dialect.drop_enum_type('status')
        assert sql == "DROP TYPE status"

    def test_drop_enum_type_if_exists(self):
        """Test drop_enum_type with IF EXISTS."""
        dialect = PostgresDialect()
        sql = dialect.drop_enum_type('status', if_exists=True)
        assert sql == "DROP TYPE IF EXISTS status"

    def test_drop_enum_type_cascade(self):
        """Test drop_enum_type with CASCADE."""
        dialect = PostgresDialect()
        sql = dialect.drop_enum_type('status', cascade=True)
        assert 'CASCADE' in sql

    def test_alter_enum_add_value_method(self):
        """Test alter_enum_add_value method."""
        dialect = PostgresDialect()
        sql = dialect.alter_enum_add_value('status', 'failed')
        assert sql == "ALTER TYPE status ADD VALUE 'failed'"

    def test_alter_enum_add_value_before(self):
        """Test alter_enum_add_value with BEFORE."""
        dialect = PostgresDialect()
        sql = dialect.alter_enum_add_value('status', 'processing', before='ready')
        assert "BEFORE 'ready'" in sql

    def test_alter_enum_add_value_after(self):
        """Test alter_enum_add_value with AFTER."""
        dialect = PostgresDialect()
        sql = dialect.alter_enum_add_value('status', 'processing', after='pending')
        assert "AFTER 'pending'" in sql


class TestEnumTypeMixin:
    """Tests for EnumTypeMixin formatting methods."""

    def test_format_enum_type_name(self):
        """Test format_enum_type_name method."""
        dialect = PostgresDialect()
        assert dialect.format_enum_type_name('status') == 'status'
        assert dialect.format_enum_type_name('status', schema='app') == 'app.status'

    def test_format_enum_values(self):
        """Test format_enum_values method."""
        dialect = PostgresDialect()
        result = dialect.format_enum_values(['a', 'b', 'c'])
        assert result == "'a', 'b', 'c'"

    def test_format_create_enum_type(self):
        """Test format_create_enum_type method."""
        dialect = PostgresDialect()
        sql = dialect.format_create_enum_type('status', ['pending', 'ready'])
        assert sql == "CREATE TYPE status AS ENUM ('pending', 'ready')"

    def test_format_drop_enum_type(self):
        """Test format_drop_enum_type method."""
        dialect = PostgresDialect()
        sql = dialect.format_drop_enum_type('status')
        assert sql == "DROP TYPE status"

    def test_format_alter_enum_add_value(self):
        """Test format_alter_enum_add_value method."""
        dialect = PostgresDialect()
        sql = dialect.format_alter_enum_add_value('status', 'failed')
        assert sql == "ALTER TYPE status ADD VALUE 'failed'"
