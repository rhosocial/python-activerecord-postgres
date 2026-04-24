# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pgcrypto.py
"""
Unit tests for PostgreSQL pgcrypto extension mixin.

Tests for PostgresPgcryptoMixin format methods:
- format_gen_salt
- format_crypt
- format_hmac
- format_digest
- format_pgp_encrypt
- format_pgp_decrypt
- format_random_bytes
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestPostgresPgcryptoMixin:
    """Tests for PostgresPgcryptoMixin format methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dialect = PostgresDialect(version=(14, 0, 0))

    def test_format_gen_salt(self):
        """format_gen_salt should return SQL containing gen_salt."""
        result = self.dialect.format_gen_salt()
        assert "gen_salt" in result

    def test_format_gen_salt_custom_algorithm(self):
        """format_gen_salt with custom algorithm should include it."""
        result = self.dialect.format_gen_salt(algorithm="sha256")
        assert "gen_salt" in result
        assert "sha256" in result

    def test_format_crypt(self):
        """format_crypt should return SQL containing crypt."""
        result = self.dialect.format_crypt("password", "salt")
        assert "crypt" in result

    def test_format_hmac(self):
        """format_hmac should return SQL containing hmac."""
        result = self.dialect.format_hmac("data", "key", "sha256")
        assert "hmac" in result

    def test_format_digest(self):
        """format_digest should return tuple with SQL containing digest."""
        sql, params = self.dialect.format_digest("'data'", "sha256")
        assert "digest" in sql
        assert params == ()

    def test_format_pgp_encrypt(self):
        """format_pgp_encrypt should return tuple with SQL containing pgp_sym_encrypt."""
        sql, params = self.dialect.format_pgp_encrypt("'secret'", "'password'")
        assert "pgp_sym_encrypt" in sql
        assert params == ()

    def test_format_pgp_decrypt(self):
        """format_pgp_decrypt should return tuple with SQL containing pgp_sym_decrypt."""
        sql, params = self.dialect.format_pgp_decrypt("'encrypted'", "'password'")
        assert "pgp_sym_decrypt" in sql
        assert params == ()

    def test_format_random_bytes(self):
        """format_random_bytes should return SQL containing random_bytes."""
        result = self.dialect.format_random_bytes(16)
        assert "random_bytes" in result
