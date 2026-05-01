# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pgcrypto.py
"""
Unit tests for PostgreSQL pgcrypto extension functions.

Tests for:
- gen_salt
- crypt
- encrypt / decrypt
- hmac
- digest
- pgp_sym_encrypt / pgp_sym_decrypt
- gen_random_bytes
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.impl.postgres.functions.pgcrypto import (
    gen_salt,
    crypt,
    encrypt,
    decrypt,
    gen_random_bytes,
    hmac,
    digest,
    pgp_sym_encrypt,
    pgp_sym_decrypt,
)


class TestPostgresPgcryptoFunctions:
    """Tests for pgcrypto function factories."""

    def test_gen_salt(self):
        """gen_salt should return FunctionCall with gen_salt."""
        dialect = PostgresDialect((14, 0, 0))
        result = gen_salt(dialect)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "gen_salt" in sql.lower()

    def test_gen_salt_custom_algorithm(self):
        """gen_salt with custom algorithm should include it."""
        dialect = PostgresDialect((14, 0, 0))
        result = gen_salt(dialect, algorithm="sha256")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "gen_salt" in sql.lower()

    def test_crypt(self):
        """crypt should return FunctionCall with crypt."""
        dialect = PostgresDialect((14, 0, 0))
        result = crypt(dialect, "password", "salt")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "crypt" in sql.lower()

    def test_encrypt(self):
        """encrypt should return FunctionCall with encrypt."""
        dialect = PostgresDialect((14, 0, 0))
        result = encrypt(dialect, "data", "key")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "encrypt" in sql.lower()

    def test_decrypt(self):
        """decrypt should return FunctionCall with decrypt."""
        dialect = PostgresDialect((14, 0, 0))
        result = decrypt(dialect, "data", "key")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "decrypt" in sql.lower()

    def test_hmac(self):
        """hmac should return FunctionCall with hmac."""
        dialect = PostgresDialect((14, 0, 0))
        result = hmac(dialect, "data", "key", "sha256")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "hmac" in sql.lower()

    def test_digest(self):
        """digest should return FunctionCall with digest."""
        dialect = PostgresDialect((14, 0, 0))
        result = digest(dialect, "data", "sha256")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "digest" in sql.lower()

    def test_pgp_sym_encrypt(self):
        """pgp_sym_encrypt should return FunctionCall with pgp_sym_encrypt."""
        dialect = PostgresDialect((14, 0, 0))
        result = pgp_sym_encrypt(dialect, "secret", "password")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "pgp_sym_encrypt" in sql.lower()

    def test_pgp_sym_decrypt(self):
        """pgp_sym_decrypt should return FunctionCall with pgp_sym_decrypt."""
        dialect = PostgresDialect((14, 0, 0))
        result = pgp_sym_decrypt(dialect, "encrypted", "password")
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "pgp_sym_decrypt" in sql.lower()

    def test_gen_random_bytes(self):
        """gen_random_bytes should return FunctionCall with gen_random_bytes."""
        dialect = PostgresDialect((14, 0, 0))
        result = gen_random_bytes(dialect, 16)
        assert isinstance(result, core.FunctionCall)
        sql, params = result.to_sql()
        assert "gen_random_bytes" in sql.lower()
