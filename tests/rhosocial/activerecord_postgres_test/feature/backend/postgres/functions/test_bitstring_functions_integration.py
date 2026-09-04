# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/functions/test_bitstring_functions_integration.py
"""
Integration tests for PostgreSQL bit string functions and operators.

These tests require a live PostgreSQL connection and execute expressions
created by functions.bit_string against the database.
"""
import pytest

from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.bit_string import (
    bit_and,
    bit_concat,
    bit_count,
    bit_get_bit,
    bit_length,
    bit_length_func,
    bit_not,
    bit_octet_length,
    bit_or,
    bit_set_bit,
    bit_shift_left,
    bit_shift_right,
    bit_xor,
)


def _bit(dialect, value):
    return core.Subquery(dialect, f"B'{value}'")


def _execute_expression(backend, expression, alias="result"):
    sql, params = expression.to_sql()
    return backend.fetch_one(f"SELECT {sql} AS {alias}", params)


def _execute_text_expression(backend, expression, alias="result"):
    sql, params = expression.to_sql()
    return backend.fetch_one(f"SELECT ({sql})::text AS {alias}", params)


class TestSyncBitStringOperatorsIntegration:
    """Synchronous integration tests for bit string operators."""

    def test_bit_concat_executes(self, postgres_backend):
        """Test bit string concatenation operator."""
        expr = bit_concat(
            postgres_backend.dialect,
            _bit(postgres_backend.dialect, "1010"),
            _bit(postgres_backend.dialect, "11"),
        )
        result = _execute_text_expression(postgres_backend, expr)

        assert result["result"] == "101011"

    def test_bitwise_and_executes(self, postgres_backend):
        """Test bitwise AND operator."""
        expr = bit_and(
            postgres_backend.dialect,
            _bit(postgres_backend.dialect, "1100"),
            _bit(postgres_backend.dialect, "1010"),
        )
        result = _execute_text_expression(postgres_backend, expr)

        assert result["result"] == "1000"

    def test_bitwise_or_executes(self, postgres_backend):
        """Test bitwise OR operator."""
        expr = bit_or(
            postgres_backend.dialect,
            _bit(postgres_backend.dialect, "1100"),
            _bit(postgres_backend.dialect, "1010"),
        )
        result = _execute_text_expression(postgres_backend, expr)

        assert result["result"] == "1110"

    def test_bitwise_xor_executes(self, postgres_backend):
        """Test bitwise XOR operator."""
        expr = bit_xor(
            postgres_backend.dialect,
            _bit(postgres_backend.dialect, "1100"),
            _bit(postgres_backend.dialect, "1010"),
        )
        result = _execute_text_expression(postgres_backend, expr)

        assert result["result"] == "0110"

    def test_bitwise_not_executes(self, postgres_backend):
        """Test bitwise NOT operator."""
        expr = bit_not(postgres_backend.dialect, _bit(postgres_backend.dialect, "1010"))
        result = _execute_text_expression(postgres_backend, expr)

        assert result["result"] == "0101"

    def test_bit_shift_left_executes(self, postgres_backend):
        """Test bitwise left shift operator."""
        expr = bit_shift_left(
            postgres_backend.dialect,
            _bit(postgres_backend.dialect, "1010"),
            1,
        )
        result = _execute_text_expression(postgres_backend, expr)

        assert result["result"] == "0100"

    def test_bit_shift_right_executes(self, postgres_backend):
        """Test bitwise right shift operator."""
        expr = bit_shift_right(
            postgres_backend.dialect,
            _bit(postgres_backend.dialect, "1010"),
            1,
        )
        result = _execute_text_expression(postgres_backend, expr)

        assert result["result"] == "0101"


class TestSyncBitStringFunctionsIntegration:
    """Synchronous integration tests for bit string functions."""

    def test_bit_length_executes(self, postgres_backend):
        """Test length(bit) function."""
        expr = bit_length(postgres_backend.dialect, _bit(postgres_backend.dialect, "10101"))
        result = _execute_expression(postgres_backend, expr)

        assert result["result"] == 5

    def test_bit_length_func_executes(self, postgres_backend):
        """Test bit_length(bit) function."""
        expr = bit_length_func(postgres_backend.dialect, _bit(postgres_backend.dialect, "10101"))
        result = _execute_expression(postgres_backend, expr)

        assert result["result"] == 5

    def test_bit_octet_length_executes(self, postgres_backend):
        """Test octet_length(bit) function."""
        expr = bit_octet_length(
            postgres_backend.dialect,
            _bit(postgres_backend.dialect, "101010101"),
        )
        result = _execute_expression(postgres_backend, expr)

        assert result["result"] == 2

    def test_bit_get_bit_executes(self, postgres_backend):
        """Test get_bit(bit, position) function."""
        expr = bit_get_bit(postgres_backend.dialect, _bit(postgres_backend.dialect, "1010"), 0)
        result = _execute_expression(postgres_backend, expr)

        assert result["result"] == 1

    def test_bit_set_bit_executes(self, postgres_backend):
        """Test set_bit(bit, position, value) function."""
        expr = bit_set_bit(postgres_backend.dialect, _bit(postgres_backend.dialect, "1010"), 1, 1)
        result = _execute_text_expression(postgres_backend, expr)

        assert result["result"] == "1110"

    def test_bit_count_executes_on_postgres_14_plus(self, postgres_backend):
        """Test bit_count(bit) function on PostgreSQL 14+."""
        if postgres_backend.get_server_version() < (14, 0, 0):
            pytest.skip("bit_count requires PostgreSQL 14+")

        expr = bit_count(postgres_backend.dialect, _bit(postgres_backend.dialect, "101011"))
        result = _execute_expression(postgres_backend, expr)

        assert result["result"] == 4

    def test_bit_count_skips_before_postgres_14(self, postgres_backend):
        """Test bit_count version gate for PostgreSQL versions before 14."""
        version = postgres_backend.get_server_version()
        if version >= (14, 0, 0):
            assert version >= (14, 0, 0)
            return

        pytest.skip("bit_count requires PostgreSQL 14+")


class TestAsyncBitStringFunctionsIntegration:
    """Asynchronous integration tests for representative bit string functions."""

    @pytest.mark.asyncio
    async def test_async_bit_concat_executes(self, async_postgres_backend):
        """Test async bit string concatenation operator."""
        expr = bit_concat(
            async_postgres_backend.dialect,
            _bit(async_postgres_backend.dialect, "1010"),
            _bit(async_postgres_backend.dialect, "11"),
        )
        sql, params = expr.to_sql()
        result = await async_postgres_backend.fetch_one(f"SELECT ({sql})::text AS result", params)

        assert result["result"] == "101011"

    @pytest.mark.asyncio
    async def test_async_bit_get_bit_executes(self, async_postgres_backend):
        """Test async get_bit(bit, position) function."""
        expr = bit_get_bit(
            async_postgres_backend.dialect,
            _bit(async_postgres_backend.dialect, "1010"),
            0,
        )
        sql, params = expr.to_sql()
        result = await async_postgres_backend.fetch_one(f"SELECT {sql} AS result", params)

        assert result["result"] == 1

    @pytest.mark.asyncio
    async def test_async_bit_count_executes_on_postgres_14_plus(self, async_postgres_backend):
        """Test async bit_count(bit) function on PostgreSQL 14+."""
        if await async_postgres_backend.get_server_version() < (14, 0, 0):
            pytest.skip("bit_count requires PostgreSQL 14+")

        expr = bit_count(
            async_postgres_backend.dialect,
            _bit(async_postgres_backend.dialect, "101011"),
        )
        sql, params = expr.to_sql()
        result = await async_postgres_backend.fetch_one(f"SELECT {sql} AS result", params)

        assert result["result"] == 4
