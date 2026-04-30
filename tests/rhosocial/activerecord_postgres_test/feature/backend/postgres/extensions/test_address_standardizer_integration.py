"""Integration tests for the address_standardizer extension.

These tests require a PostgreSQL database with both the postgis and
address_standardizer extensions installed.
Tests will be automatically skipped if the extensions are not available.
All database operations use expression objects, not raw SQL strings.

The actual PostgreSQL function signatures are:
- parse_address(text) — simple, takes just text
- standardize_address(lextab text, gaztab text, rultab text, address text)
  — requires lookup table names (e.g., 'pagc_lex', 'pagc_gaz', 'pagc_rules')
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    async_ensure_extension_installed,
    ensure_extension_installed,
)
from rhosocial.activerecord.backend.expression import QueryExpression
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.impl.postgres.functions.address_standardizer import (
    parse_address,
    standardize_address,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


@pytest.fixture
def addr_std_env(postgres_backend_single):
    """Test environment for address_standardizer extension.

    No table needed - the functions work on string literals directly.
    """
    backend = postgres_backend_single
    ensure_extension_installed(backend, "postgis", "address_standardizer")
    dialect = backend.dialect

    yield backend, dialect


class TestAddressStandardizerIntegration:
    """Integration tests for address_standardizer extension functions."""

    def test_parse_address(self, addr_std_env):
        """Test parse_address function for parsing address into components.

        parse_address(text) is the simpler function that takes just
        an address string and returns a normalized_address record.
        """
        backend, dialect = addr_std_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        parse_func = parse_address(
            dialect,
            Literal(dialect, "123 Main St, Springfield, IL 62701"),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[parse_func.as_("parsed_addr")],
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        # parse_address returns a composite record with address components
        assert result.data[0]["parsed_addr"] is not None

    def test_standardize_address(self, addr_std_env):
        """Test standardize_address function with lookup tables.

        standardize_address(lextab, gaztab, rultab, address) requires
        lookup table names. The standard tables installed with the
        extension are 'pagc_lex', 'pagc_gaz', and 'pagc_rules'.
        """
        backend, dialect = addr_std_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            std_func = standardize_address(
                dialect,
                "pagc_lex",
                "pagc_gaz",
                "pagc_rules",
                Literal(dialect, "123 Main St, Springfield, IL 62701"),
            )
            query = QueryExpression(
                dialect=dialect,
                select=[std_func.as_("std_addr")],
            )
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=opts)
            assert result.data is not None
            assert len(result.data) > 0
            # standardize_address returns a composite record
            assert result.data[0]["std_addr"] is not None
        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg:
                pytest.skip("address_standardizer lookup tables not installed")
            raise


@pytest_asyncio.fixture
async def async_addr_std_env(async_postgres_backend_single):
    """Test environment for address_standardizer extension (async).

    No table needed - the functions work on string literals directly.
    """
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "postgis", "address_standardizer")
    dialect = backend.dialect

    yield backend, dialect


class TestAsyncAddressStandardizerIntegration:
    """Async integration tests for address_standardizer extension functions."""

    @pytest.mark.asyncio
    async def test_async_parse_address(self, async_addr_std_env):
        """Test parse_address function for parsing address into components.

        parse_address(text) is the simpler function that takes just
        an address string and returns a normalized_address record.
        """
        backend, dialect = async_addr_std_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        parse_func = parse_address(
            dialect,
            Literal(dialect, "123 Main St, Springfield, IL 62701"),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[parse_func.as_("parsed_addr")],
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert len(result.data) > 0
        # parse_address returns a composite record with address components
        assert result.data[0]["parsed_addr"] is not None

    @pytest.mark.asyncio
    async def test_async_standardize_address(self, async_addr_std_env):
        """Test standardize_address function with lookup tables.

        standardize_address(lextab, gaztab, rultab, address) requires
        lookup table names. The standard tables installed with the
        extension are 'pagc_lex', 'pagc_gaz', and 'pagc_rules'.
        """
        backend, dialect = async_addr_std_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        try:
            std_func = standardize_address(
                dialect,
                "pagc_lex",
                "pagc_gaz",
                "pagc_rules",
                Literal(dialect, "123 Main St, Springfield, IL 62701"),
            )
            query = QueryExpression(
                dialect=dialect,
                select=[std_func.as_("std_addr")],
            )
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=opts)
            assert result.data is not None
            assert len(result.data) > 0
            # standardize_address returns a composite record
            assert result.data[0]["std_addr"] is not None
        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg:
                pytest.skip("address_standardizer lookup tables not installed")
            raise
