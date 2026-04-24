# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pgcrypto_integration.py
"""
Integration tests for PostgreSQL pgcrypto extension with real database.

These tests require a live PostgreSQL connection with pgcrypto extension installed
and test:
- crypt() + gen_salt() for password hashing
- digest() for hash generation
- hmac() for keyed hashing
- pgp_sym_encrypt/decrypt for encryption
"""
import pytest


class TestPgcryptoIntegration:
    """Integration tests for pgcrypto extension."""

    def test_crypt_password(self, postgres_backend_single):
        """Test crypt() with gen_salt() for password hashing."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("pgcrypto"):
            pytest.skip("Extension 'pgcrypto' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_pgcrypto_users")
            backend.execute("""
                CREATE TABLE test_pgcrypto_users (
                    id SERIAL PRIMARY KEY,
                    username TEXT,
                    password_hash TEXT
                )
            """)

            # Hash a password with gen_salt using bf (blowfish)
            backend.execute("""
                INSERT INTO test_pgcrypto_users (username, password_hash)
                VALUES ('alice', crypt('mysecretpassword', gen_salt('bf')))
            """)

            # Verify password: crypt() with existing hash returns the same hash if password matches
            result = backend.fetch_one("""
                SELECT password_hash = crypt('mysecretpassword', password_hash) AS is_match
                FROM test_pgcrypto_users WHERE username = 'alice'
            """)
            assert result['is_match'] is True

            # Wrong password should not match
            result = backend.fetch_one("""
                SELECT password_hash = crypt('wrongpassword', password_hash) AS is_match
                FROM test_pgcrypto_users WHERE username = 'alice'
            """)
            assert result['is_match'] is False
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_pgcrypto_users")
            except Exception:
                pass

    def test_crypt_gen_salt_bf(self, postgres_backend_single):
        """Test gen_salt() with bf algorithm produces different salts."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("pgcrypto"):
            pytest.skip("Extension 'pgcrypto' not installed")

        # gen_salt should produce different salts each time
        result1 = backend.fetch_one("SELECT gen_salt('bf') AS salt")
        result2 = backend.fetch_one("SELECT gen_salt('bf') AS salt")
        assert result1['salt'] != result2['salt']

        # Both should start with $2a$ (blowfish format)
        assert result1['salt'].startswith('$2a$')
        assert result2['salt'].startswith('$2a$')

    def test_digest(self, postgres_backend_single):
        """Test digest() for hash generation."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("pgcrypto"):
            pytest.skip("Extension 'pgcrypto' not installed")

        # Test SHA-256 digest
        result = backend.fetch_one("""
            SELECT digest('hello', 'sha256') AS hash
        """)
        assert result is not None
        assert result['hash'] is not None
        # SHA-256 produces 32 bytes (256 bits)
        hash_bytes = result['hash']
        if isinstance(hash_bytes, memoryview):
            hash_bytes = bytes(hash_bytes)
        assert len(hash_bytes) == 32

        # Same input should produce same hash
        result2 = backend.fetch_one("""
            SELECT digest('hello', 'sha256') AS hash
        """)
        hash_bytes2 = result2['hash']
        if isinstance(hash_bytes2, memoryview):
            hash_bytes2 = bytes(hash_bytes2)
        assert hash_bytes == hash_bytes2

    def test_digest_md5(self, postgres_backend_single):
        """Test digest() with MD5 algorithm."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("pgcrypto"):
            pytest.skip("Extension 'pgcrypto' not installed")

        result = backend.fetch_one("""
            SELECT encode(digest('hello', 'md5'), 'hex') AS hash_hex
        """)
        assert result is not None
        assert result['hash_hex'] is not None
        # MD5 of 'hello' is well-known
        assert result['hash_hex'] == '5d41402abc4b2a76b9719d911017c592'

    def test_hmac(self, postgres_backend_single):
        """Test hmac() for keyed hash generation."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("pgcrypto"):
            pytest.skip("Extension 'pgcrypto' not installed")

        # HMAC with SHA-256
        result1 = backend.fetch_one("""
            SELECT hmac('message', 'secret_key', 'sha256') AS mac
        """)
        assert result1 is not None
        assert result1['mac'] is not None

        # Same inputs should produce same HMAC
        result2 = backend.fetch_one("""
            SELECT hmac('message', 'secret_key', 'sha256') AS mac
        """)
        mac1 = result1['mac']
        mac2 = result2['mac']
        if isinstance(mac1, memoryview):
            mac1 = bytes(mac1)
        if isinstance(mac2, memoryview):
            mac2 = bytes(mac2)
        assert mac1 == mac2

        # Different key should produce different HMAC
        result3 = backend.fetch_one("""
            SELECT hmac('message', 'other_key', 'sha256') AS mac
        """)
        mac3 = result3['mac']
        if isinstance(mac3, memoryview):
            mac3 = bytes(mac3)
        assert mac1 != mac3

    def test_pgp_sym_encrypt_decrypt(self, postgres_backend_single):
        """Test pgp_sym_encrypt() and pgp_sym_decrypt() for symmetric encryption."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("pgcrypto"):
            pytest.skip("Extension 'pgcrypto' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_pgcrypto_encrypt")
            backend.execute("""
                CREATE TABLE test_pgcrypto_encrypt (
                    id SERIAL PRIMARY KEY,
                    secret_data BYTEA
                )
            """)

            # Encrypt data
            backend.execute("""
                INSERT INTO test_pgcrypto_encrypt (secret_data)
                VALUES (pgp_sym_encrypt('sensitive information', 'encryption_key'))
            """)

            # Decrypt data
            result = backend.fetch_one("""
                SELECT pgp_sym_decrypt(secret_data, 'encryption_key') AS decrypted
                FROM test_pgcrypto_encrypt WHERE id = 1
            """)
            assert result['decrypted'] == 'sensitive information'

            # Wrong key should fail
            with pytest.raises(Exception):
                backend.fetch_one("""
                    SELECT pgp_sym_decrypt(secret_data, 'wrong_key') AS decrypted
                    FROM test_pgcrypto_encrypt WHERE id = 1
                """)
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_pgcrypto_encrypt")
            except Exception:
                pass
