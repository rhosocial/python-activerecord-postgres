# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pgcrypto_integration.py
"""
Integration tests for PostgreSQL pgcrypto extension with real database.

These tests require a live PostgreSQL connection with pgcrypto extension installed
and test:
- crypt() + gen_salt() for password hashing
- digest() for hash generation
- hmac() for keyed hashing
- pgp_sym_encrypt/decrypt for encryption

All database operations use expression objects, not raw SQL strings.
"""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)
from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.expression import (
    CreateTableExpression,
    ColumnDefinition,
    ColumnConstraint,
    ColumnConstraintType,
    DropTableExpression,
    InsertExpression,
    QueryExpression,
    TableExpression,
    Column,
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.core import Literal, FunctionCall, Subquery
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


# Table names for sync tests
TABLE_USERS = "test_pgcrypto_users"
TABLE_ENCRYPT = "test_pgcrypto_encrypt"

# Table names for async tests
TABLE_USERS_ASYNC = "test_pgcrypto_users_async"
TABLE_ENCRYPT_ASYNC = "test_pgcrypto_encrypt_async"


def _setup_users_table(backend, dialect, table_name):
    """Create and populate the test_pgcrypto_users table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="username", data_type="TEXT"),
        ColumnDefinition(name="password_hash", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    # INSERT with nested function calls: crypt('mysecretpassword', gen_salt('bf'))
    crypt_func = FunctionCall(
        dialect, "crypt",
        Literal(dialect, "mysecretpassword"),
        FunctionCall(dialect, "gen_salt", Literal(dialect, "bf")),
    )
    rows = [
        [Literal(dialect, "alice"), crypt_func],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["username", "password_hash"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _setup_encrypt_table(backend, dialect, table_name):
    """Create and populate the test_pgcrypto_encrypt table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="secret_data", data_type="BYTEA"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    backend.execute(sql, params)

    # INSERT with function call: pgp_sym_encrypt('sensitive information', 'encryption_key')
    encrypt_func = FunctionCall(
        dialect, "pgp_sym_encrypt",
        Literal(dialect, "sensitive information"),
        Literal(dialect, "encryption_key"),
    )
    rows = [
        [encrypt_func],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["secret_data"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    backend.execute(sql, params)


def _teardown_table(backend, dialect, table_name):
    """Drop a test table using expression."""
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=table_name,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    backend.execute(sql, params)


async def _async_setup_users_table(backend, dialect, table_name):
    """Async: create and populate the test_pgcrypto_users table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="username", data_type="TEXT"),
        ColumnDefinition(name="password_hash", data_type="TEXT"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    # INSERT with nested function calls: crypt('mysecretpassword', gen_salt('bf'))
    crypt_func = FunctionCall(
        dialect, "crypt",
        Literal(dialect, "mysecretpassword"),
        FunctionCall(dialect, "gen_salt", Literal(dialect, "bf")),
    )
    rows = [
        [Literal(dialect, "alice"), crypt_func],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["username", "password_hash"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_setup_encrypt_table(backend, dialect, table_name):
    """Async: create and populate the test_pgcrypto_encrypt table using expressions."""
    columns = [
        ColumnDefinition(
            name="id",
            data_type="SERIAL",
            constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY),
            ],
        ),
        ColumnDefinition(name="secret_data", data_type="BYTEA"),
    ]
    create_expr = CreateTableExpression(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        if_not_exists=True,
    )
    sql, params = create_expr.to_sql()
    await backend.execute(sql, params)

    # INSERT with function call: pgp_sym_encrypt('sensitive information', 'encryption_key')
    encrypt_func = FunctionCall(
        dialect, "pgp_sym_encrypt",
        Literal(dialect, "sensitive information"),
        Literal(dialect, "encryption_key"),
    )
    rows = [
        [encrypt_func],
    ]
    insert_expr = InsertExpression(
        dialect=dialect,
        into=table_name,
        columns=["secret_data"],
        source=ValuesSource(dialect, rows),
    )
    sql, params = insert_expr.to_sql()
    await backend.execute(sql, params)


async def _async_teardown_table(backend, dialect, table_name):
    """Async: drop a test table using expression."""
    drop_expr = DropTableExpression(
        dialect=dialect,
        table_name=table_name,
        if_exists=True,
    )
    sql, params = drop_expr.to_sql()
    await backend.execute(sql, params)


# --- Sync fixture and tests ---


@pytest.fixture
def pgcrypto_env(postgres_backend_single):
    """Independent test environment for pgcrypto extension."""
    backend = postgres_backend_single
    ensure_extension_installed(backend, "pgcrypto")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [TABLE_USERS, TABLE_ENCRYPT]:
        _teardown_table(backend, dialect, table_name)

    _setup_users_table(backend, dialect, TABLE_USERS)
    _setup_encrypt_table(backend, dialect, TABLE_ENCRYPT)

    yield backend, dialect

    _teardown_table(backend, dialect, TABLE_USERS)
    _teardown_table(backend, dialect, TABLE_ENCRYPT)


class TestPgcryptoIntegration:
    """Integration tests for pgcrypto extension."""

    def test_crypt_password(self, pgcrypto_env):
        """Test crypt() with gen_salt() for password hashing."""
        backend, dialect = pgcrypto_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Verify password: password_hash = crypt('mysecretpassword', password_hash)
        crypt_func = FunctionCall(
            dialect, "crypt",
            Literal(dialect, "mysecretpassword"),
            Column(dialect, "password_hash"),
        )
        # TODO: Subquery wrapper needed because ComparisonPredicate lacks .as_().
        # Replace with ComparisonPredicate.as_() once it gains AliasableMixin support.
        match_expr = Subquery(dialect, ComparisonPredicate(
            dialect, "=",
            Column(dialect, "password_hash"),
            crypt_func,
        ), alias="is_match")
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "username"),
            Literal(dialect, "alice"),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[match_expr],
            from_=TableExpression(dialect, TABLE_USERS),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data[0]["is_match"] is True

        # Wrong password should not match
        crypt_func_wrong = FunctionCall(
            dialect, "crypt",
            Literal(dialect, "wrongpassword"),
            Column(dialect, "password_hash"),
        )
        # TODO: Subquery wrapper needed because ComparisonPredicate lacks .as_().
        match_expr_wrong = Subquery(dialect, ComparisonPredicate(
            dialect, "=",
            Column(dialect, "password_hash"),
            crypt_func_wrong,
        ), alias="is_match")
        query_wrong = QueryExpression(
            dialect=dialect,
            select=[match_expr_wrong],
            from_=TableExpression(dialect, TABLE_USERS),
            where=where_pred,
        )
        sql, params = query_wrong.to_sql()
        result_wrong = backend.execute(sql, params, options=opts)
        assert result_wrong.data[0]["is_match"] is False

    def test_crypt_gen_salt_bf(self, pgcrypto_env):
        """Test gen_salt() with bf algorithm produces different salts."""
        backend, dialect = pgcrypto_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # gen_salt should produce different salts each time
        salt_func1 = FunctionCall(
            dialect, "gen_salt",
            Literal(dialect, "bf"),
        ).as_("salt")
        query1 = QueryExpression(dialect=dialect, select=[salt_func1])
        sql, params = query1.to_sql()
        result1 = backend.execute(sql, params, options=opts)

        salt_func2 = FunctionCall(
            dialect, "gen_salt",
            Literal(dialect, "bf"),
        ).as_("salt")
        query2 = QueryExpression(dialect=dialect, select=[salt_func2])
        sql, params = query2.to_sql()
        result2 = backend.execute(sql, params, options=opts)

        assert result1.data[0]["salt"] != result2.data[0]["salt"]

        # Both should start with $2a$ (blowfish format)
        assert result1.data[0]["salt"].startswith("$2a$")
        assert result2.data[0]["salt"].startswith("$2a$")

    def test_digest(self, pgcrypto_env):
        """Test digest() for hash generation."""
        backend, dialect = pgcrypto_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test SHA-256 digest
        digest_func = FunctionCall(
            dialect, "digest",
            Literal(dialect, "hello"),
            Literal(dialect, "sha256"),
        ).as_("hash")
        query = QueryExpression(dialect=dialect, select=[digest_func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]["hash"] is not None
        # SHA-256 produces 32 bytes (256 bits)
        hash_bytes = result.data[0]["hash"]
        if isinstance(hash_bytes, memoryview):
            hash_bytes = bytes(hash_bytes)
        assert len(hash_bytes) == 32

        # Same input should produce same hash
        digest_func2 = FunctionCall(
            dialect, "digest",
            Literal(dialect, "hello"),
            Literal(dialect, "sha256"),
        ).as_("hash")
        query2 = QueryExpression(dialect=dialect, select=[digest_func2])
        sql, params = query2.to_sql()
        result2 = backend.execute(sql, params, options=opts)
        hash_bytes2 = result2.data[0]["hash"]
        if isinstance(hash_bytes2, memoryview):
            hash_bytes2 = bytes(hash_bytes2)
        assert hash_bytes == hash_bytes2

    def test_digest_md5(self, pgcrypto_env):
        """Test digest() with MD5 algorithm."""
        backend, dialect = pgcrypto_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # encode(digest('hello', 'md5'), 'hex')
        encode_func = FunctionCall(
            dialect, "encode",
            FunctionCall(
                dialect, "digest",
                Literal(dialect, "hello"),
                Literal(dialect, "md5"),
            ),
            Literal(dialect, "hex"),
        ).as_("hash_hex")
        query = QueryExpression(dialect=dialect, select=[encode_func])
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]["hash_hex"] is not None
        # MD5 of 'hello' is well-known
        assert result.data[0]["hash_hex"] == "5d41402abc4b2a76b9719d911017c592"

    def test_hmac(self, pgcrypto_env):
        """Test hmac() for keyed hash generation."""
        backend, dialect = pgcrypto_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # HMAC with SHA-256
        hmac_func1 = FunctionCall(
            dialect, "hmac",
            Literal(dialect, "message"),
            Literal(dialect, "secret_key"),
            Literal(dialect, "sha256"),
        ).as_("mac")
        query1 = QueryExpression(dialect=dialect, select=[hmac_func1])
        sql, params = query1.to_sql()
        result1 = backend.execute(sql, params, options=opts)
        assert result1.data is not None
        assert result1.data[0]["mac"] is not None

        # Same inputs should produce same HMAC
        hmac_func2 = FunctionCall(
            dialect, "hmac",
            Literal(dialect, "message"),
            Literal(dialect, "secret_key"),
            Literal(dialect, "sha256"),
        ).as_("mac")
        query2 = QueryExpression(dialect=dialect, select=[hmac_func2])
        sql, params = query2.to_sql()
        result2 = backend.execute(sql, params, options=opts)
        mac1 = result1.data[0]["mac"]
        mac2 = result2.data[0]["mac"]
        if isinstance(mac1, memoryview):
            mac1 = bytes(mac1)
        if isinstance(mac2, memoryview):
            mac2 = bytes(mac2)
        assert mac1 == mac2

        # Different key should produce different HMAC
        hmac_func3 = FunctionCall(
            dialect, "hmac",
            Literal(dialect, "message"),
            Literal(dialect, "other_key"),
            Literal(dialect, "sha256"),
        ).as_("mac")
        query3 = QueryExpression(dialect=dialect, select=[hmac_func3])
        sql, params = query3.to_sql()
        result3 = backend.execute(sql, params, options=opts)
        mac3 = result3.data[0]["mac"]
        if isinstance(mac3, memoryview):
            mac3 = bytes(mac3)
        assert mac1 != mac3

    def test_pgp_sym_encrypt_decrypt(self, pgcrypto_env):
        """Test pgp_sym_encrypt() and pgp_sym_decrypt() for symmetric encryption."""
        backend, dialect = pgcrypto_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Decrypt data: pgp_sym_decrypt(secret_data, 'encryption_key')
        decrypt_func = FunctionCall(
            dialect, "pgp_sym_decrypt",
            Column(dialect, "secret_data"),
            Literal(dialect, "encryption_key"),
        ).as_("decrypted")
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "id"),
            Literal(dialect, 1),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[decrypt_func],
            from_=TableExpression(dialect, TABLE_ENCRYPT),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = backend.execute(sql, params, options=opts)
        assert result.data[0]["decrypted"] == "sensitive information"

        # Wrong key should fail
        decrypt_func_wrong = FunctionCall(
            dialect, "pgp_sym_decrypt",
            Column(dialect, "secret_data"),
            Literal(dialect, "wrong_key"),
        ).as_("decrypted")
        query_wrong = QueryExpression(
            dialect=dialect,
            select=[decrypt_func_wrong],
            from_=TableExpression(dialect, TABLE_ENCRYPT),
            where=where_pred,
        )
        sql, params = query_wrong.to_sql()
        with pytest.raises(DatabaseError):
            backend.execute(sql, params, options=opts)


# --- Async fixture and tests ---


@pytest_asyncio.fixture
async def async_pgcrypto_env(async_postgres_backend_single):
    """Independent async test environment for pgcrypto extension."""
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "pgcrypto")
    dialect = backend.dialect

    # Clean up residual tables from previous runs
    for table_name in [TABLE_USERS_ASYNC, TABLE_ENCRYPT_ASYNC]:
        await _async_teardown_table(backend, dialect, table_name)

    await _async_setup_users_table(backend, dialect, TABLE_USERS_ASYNC)
    await _async_setup_encrypt_table(backend, dialect, TABLE_ENCRYPT_ASYNC)

    yield backend, dialect

    await _async_teardown_table(backend, dialect, TABLE_USERS_ASYNC)
    await _async_teardown_table(backend, dialect, TABLE_ENCRYPT_ASYNC)


class TestAsyncPgcryptoIntegration:
    """Async integration tests for pgcrypto extension."""

    @pytest.mark.asyncio
    async def test_async_crypt_password(self, async_pgcrypto_env):
        """Test crypt() with gen_salt() for password hashing."""
        backend, dialect = async_pgcrypto_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Verify password: password_hash = crypt('mysecretpassword', password_hash)
        crypt_func = FunctionCall(
            dialect, "crypt",
            Literal(dialect, "mysecretpassword"),
            Column(dialect, "password_hash"),
        )
        # TODO: Subquery wrapper needed because ComparisonPredicate lacks .as_().
        # Replace with ComparisonPredicate.as_() once it gains AliasableMixin support.
        match_expr = Subquery(dialect, ComparisonPredicate(
            dialect, "=",
            Column(dialect, "password_hash"),
            crypt_func,
        ), alias="is_match")
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "username"),
            Literal(dialect, "alice"),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[match_expr],
            from_=TableExpression(dialect, TABLE_USERS_ASYNC),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data[0]["is_match"] is True

        # Wrong password should not match
        crypt_func_wrong = FunctionCall(
            dialect, "crypt",
            Literal(dialect, "wrongpassword"),
            Column(dialect, "password_hash"),
        )
        # TODO: Subquery wrapper needed because ComparisonPredicate lacks .as_().
        match_expr_wrong = Subquery(dialect, ComparisonPredicate(
            dialect, "=",
            Column(dialect, "password_hash"),
            crypt_func_wrong,
        ), alias="is_match")
        query_wrong = QueryExpression(
            dialect=dialect,
            select=[match_expr_wrong],
            from_=TableExpression(dialect, TABLE_USERS_ASYNC),
            where=where_pred,
        )
        sql, params = query_wrong.to_sql()
        result_wrong = await backend.execute(sql, params, options=opts)
        assert result_wrong.data[0]["is_match"] is False

    @pytest.mark.asyncio
    async def test_async_crypt_gen_salt_bf(self, async_pgcrypto_env):
        """Test gen_salt() with bf algorithm produces different salts."""
        backend, dialect = async_pgcrypto_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # gen_salt should produce different salts each time
        salt_func1 = FunctionCall(
            dialect, "gen_salt",
            Literal(dialect, "bf"),
        ).as_("salt")
        query1 = QueryExpression(dialect=dialect, select=[salt_func1])
        sql, params = query1.to_sql()
        result1 = await backend.execute(sql, params, options=opts)

        salt_func2 = FunctionCall(
            dialect, "gen_salt",
            Literal(dialect, "bf"),
        ).as_("salt")
        query2 = QueryExpression(dialect=dialect, select=[salt_func2])
        sql, params = query2.to_sql()
        result2 = await backend.execute(sql, params, options=opts)

        assert result1.data[0]["salt"] != result2.data[0]["salt"]

        # Both should start with $2a$ (blowfish format)
        assert result1.data[0]["salt"].startswith("$2a$")
        assert result2.data[0]["salt"].startswith("$2a$")

    @pytest.mark.asyncio
    async def test_async_digest(self, async_pgcrypto_env):
        """Test digest() for hash generation."""
        backend, dialect = async_pgcrypto_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Test SHA-256 digest
        digest_func = FunctionCall(
            dialect, "digest",
            Literal(dialect, "hello"),
            Literal(dialect, "sha256"),
        ).as_("hash")
        query = QueryExpression(dialect=dialect, select=[digest_func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]["hash"] is not None
        # SHA-256 produces 32 bytes (256 bits)
        hash_bytes = result.data[0]["hash"]
        if isinstance(hash_bytes, memoryview):
            hash_bytes = bytes(hash_bytes)
        assert len(hash_bytes) == 32

        # Same input should produce same hash
        digest_func2 = FunctionCall(
            dialect, "digest",
            Literal(dialect, "hello"),
            Literal(dialect, "sha256"),
        ).as_("hash")
        query2 = QueryExpression(dialect=dialect, select=[digest_func2])
        sql, params = query2.to_sql()
        result2 = await backend.execute(sql, params, options=opts)
        hash_bytes2 = result2.data[0]["hash"]
        if isinstance(hash_bytes2, memoryview):
            hash_bytes2 = bytes(hash_bytes2)
        assert hash_bytes == hash_bytes2

    @pytest.mark.asyncio
    async def test_async_digest_md5(self, async_pgcrypto_env):
        """Test digest() with MD5 algorithm."""
        backend, dialect = async_pgcrypto_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # encode(digest('hello', 'md5'), 'hex')
        encode_func = FunctionCall(
            dialect, "encode",
            FunctionCall(
                dialect, "digest",
                Literal(dialect, "hello"),
                Literal(dialect, "md5"),
            ),
            Literal(dialect, "hex"),
        ).as_("hash_hex")
        query = QueryExpression(dialect=dialect, select=[encode_func])
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data is not None
        assert result.data[0]["hash_hex"] is not None
        # MD5 of 'hello' is well-known
        assert result.data[0]["hash_hex"] == "5d41402abc4b2a76b9719d911017c592"

    @pytest.mark.asyncio
    async def test_async_hmac(self, async_pgcrypto_env):
        """Test hmac() for keyed hash generation."""
        backend, dialect = async_pgcrypto_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # HMAC with SHA-256
        hmac_func1 = FunctionCall(
            dialect, "hmac",
            Literal(dialect, "message"),
            Literal(dialect, "secret_key"),
            Literal(dialect, "sha256"),
        ).as_("mac")
        query1 = QueryExpression(dialect=dialect, select=[hmac_func1])
        sql, params = query1.to_sql()
        result1 = await backend.execute(sql, params, options=opts)
        assert result1.data is not None
        assert result1.data[0]["mac"] is not None

        # Same inputs should produce same HMAC
        hmac_func2 = FunctionCall(
            dialect, "hmac",
            Literal(dialect, "message"),
            Literal(dialect, "secret_key"),
            Literal(dialect, "sha256"),
        ).as_("mac")
        query2 = QueryExpression(dialect=dialect, select=[hmac_func2])
        sql, params = query2.to_sql()
        result2 = await backend.execute(sql, params, options=opts)
        mac1 = result1.data[0]["mac"]
        mac2 = result2.data[0]["mac"]
        if isinstance(mac1, memoryview):
            mac1 = bytes(mac1)
        if isinstance(mac2, memoryview):
            mac2 = bytes(mac2)
        assert mac1 == mac2

        # Different key should produce different HMAC
        hmac_func3 = FunctionCall(
            dialect, "hmac",
            Literal(dialect, "message"),
            Literal(dialect, "other_key"),
            Literal(dialect, "sha256"),
        ).as_("mac")
        query3 = QueryExpression(dialect=dialect, select=[hmac_func3])
        sql, params = query3.to_sql()
        result3 = await backend.execute(sql, params, options=opts)
        mac3 = result3.data[0]["mac"]
        if isinstance(mac3, memoryview):
            mac3 = bytes(mac3)
        assert mac1 != mac3

    @pytest.mark.asyncio
    async def test_async_pgp_sym_encrypt_decrypt(self, async_pgcrypto_env):
        """Test pgp_sym_encrypt() and pgp_sym_decrypt() for symmetric encryption."""
        backend, dialect = async_pgcrypto_env
        opts = ExecutionOptions(stmt_type=StatementType.DQL)

        # Decrypt data: pgp_sym_decrypt(secret_data, 'encryption_key')
        decrypt_func = FunctionCall(
            dialect, "pgp_sym_decrypt",
            Column(dialect, "secret_data"),
            Literal(dialect, "encryption_key"),
        ).as_("decrypted")
        where_pred = ComparisonPredicate(
            dialect, "=",
            Column(dialect, "id"),
            Literal(dialect, 1),
        )
        query = QueryExpression(
            dialect=dialect,
            select=[decrypt_func],
            from_=TableExpression(dialect, TABLE_ENCRYPT_ASYNC),
            where=where_pred,
        )
        sql, params = query.to_sql()
        result = await backend.execute(sql, params, options=opts)
        assert result.data[0]["decrypted"] == "sensitive information"

        # Wrong key should fail
        decrypt_func_wrong = FunctionCall(
            dialect, "pgp_sym_decrypt",
            Column(dialect, "secret_data"),
            Literal(dialect, "wrong_key"),
        ).as_("decrypted")
        query_wrong = QueryExpression(
            dialect=dialect,
            select=[decrypt_func_wrong],
            from_=TableExpression(dialect, TABLE_ENCRYPT_ASYNC),
            where=where_pred,
        )
        sql, params = query_wrong.to_sql()
        with pytest.raises(DatabaseError):
            await backend.execute(sql, params, options=opts)
