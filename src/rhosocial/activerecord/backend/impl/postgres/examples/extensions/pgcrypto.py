# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pgcrypto.py
"""
pgcrypto extension - cryptographic functions.

This example demonstrates:
1. Check if pgcrypto extension is available
2. CREATE EXTENSION using CreateExtensionExpression
3. Use crypt + gen_salt for password hashing
4. Use digest for data integrity
5. Use hmac for message authentication
6. Use pgp_sym_encrypt / pgp_sym_decrypt for PGP symmetric encryption
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import (
    PostgresConnectionConfig,
)

config = PostgresConnectionConfig(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    database=os.getenv("PG_DATABASE", "test"),
    username=os.getenv("PG_USERNAME", "postgres"),
    password=os.getenv("PG_PASSWORD", ""),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
backend.introspect_and_adapt()
dialect = backend.dialect

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.impl.postgres.expression import (
    CreateExtensionExpression,
)
from rhosocial.activerecord.backend.expression import (
    QueryExpression,
    Column,
    FunctionCall,
    Literal,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

# Check if pgcrypto extension is available
available = dialect.is_extension_available("pgcrypto")
installed = dialect.is_extension_installed("pgcrypto")
print(f"Extension check: pgcrypto available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = CreateExtensionExpression(
        dialect=dialect,
        name="pgcrypto",
    )
    sql, params = create_ext.to_sql()
    print(f"\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("pgcrypto")

if installed:
    opts = ExecutionOptions(stmt_type=StatementType.DQL)

    # Example 1: Password hashing with crypt + gen_salt
    # gen_salt(type) generates a random salt for use with crypt()
    # bf = Blowfish (recommended), 8 = number of rounds (4-31)
    gen_salt_call = FunctionCall(
        dialect, "gen_salt", Literal(dialect, "bf"), Literal(dialect, 8)
    )
    crypt_call = FunctionCall(
        dialect, "crypt", Literal(dialect, "my_secret_password"), gen_salt_call
    ).as_("hashed_password")
    query = QueryExpression(
        dialect=dialect,
        select=[crypt_call],
    )
    sql, params = query.to_sql()
    print(f"\n--- Password hashing (crypt + gen_salt) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Hashed password: {result.data}")

    # Example 2: Verify password against stored hash
    # crypt() extracts the salt from the existing hash automatically
    # In practice: crypt(input_password, stored_hash) should equal stored_hash
    stored_hash = result.data[0]['hashed_password'] if result.data else None
    if stored_hash:
        # Re-hash the input password using the stored hash as salt
        # If the result equals the stored hash, the password is correct
        verify_crypt = FunctionCall(
            dialect, "crypt", Literal(dialect, "my_secret_password"), Literal(dialect, stored_hash)
        ).as_("rehashed")
        verify_query = QueryExpression(
            dialect=dialect,
            select=[verify_crypt],
        )
        sql, params = verify_query.to_sql()
        print(f"\n--- Password verification ---")
        print(f"SQL: {sql}")
        print(f"Params: {params}")
        result = backend.execute(sql, params, options=opts)
        rehashed = result.data[0]['rehashed'] if result.data else None
        matches = rehashed == stored_hash
        print(f"Re-hashed: {rehashed}")
        print(f"Password matches: {matches}")
    print(f"Note: In practice, store the hash from step 1, then verify with:")
    print(f"  crypt(input_password, stored_hash) = stored_hash")

    # Example 3: Data digest (hash) using digest()
    # digest(data, algorithm) returns bytea
    # Algorithms: md5, sha1, sha224, sha256, sha384, sha512
    digest_call = FunctionCall(
        dialect, "digest", Literal(dialect, "Hello, World!"), Literal(dialect, "sha256")
    )
    encode_call = FunctionCall(
        dialect, "encode", digest_call, Literal(dialect, "hex")
    ).as_("sha256_digest")
    query = QueryExpression(
        dialect=dialect,
        select=[encode_call],
    )
    sql, params = query.to_sql()
    print(f"\n--- Data digest (SHA-256) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"SHA-256 of 'Hello, World!': {result.data}")

    # Example 4: Multiple digest algorithms comparison
    md5_call = FunctionCall(
        dialect, "encode",
        FunctionCall(dialect, "digest", Literal(dialect, "test data"), Literal(dialect, "md5")),
        Literal(dialect, "hex"),
    ).as_("md5")
    sha1_call = FunctionCall(
        dialect, "encode",
        FunctionCall(dialect, "digest", Literal(dialect, "test data"), Literal(dialect, "sha1")),
        Literal(dialect, "hex"),
    ).as_("sha1")
    sha256_call = FunctionCall(
        dialect, "encode",
        FunctionCall(dialect, "digest", Literal(dialect, "test data"), Literal(dialect, "sha256")),
        Literal(dialect, "hex"),
    ).as_("sha256")
    query = QueryExpression(
        dialect=dialect,
        select=[md5_call, sha1_call, sha256_call],
    )
    sql, params = query.to_sql()
    print(f"\n--- Multiple digest algorithms ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Results: {result.data}")

    # Example 5: HMAC (Hash-based Message Authentication Code)
    # hmac(data, key, algorithm) returns bytea
    hmac_call = FunctionCall(
        dialect, "hmac",
        Literal(dialect, "message data"),
        Literal(dialect, "secret_key"),
        Literal(dialect, "sha256"),
    )
    encode_hmac = FunctionCall(
        dialect, "encode", hmac_call, Literal(dialect, "hex")
    ).as_("hmac_sha256")
    query = QueryExpression(
        dialect=dialect,
        select=[encode_hmac],
    )
    sql, params = query.to_sql()
    print(f"\n--- HMAC-SHA256 ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"HMAC of 'message data' with key 'secret_key': {result.data}")

    # Example 6: pgp_sym_encrypt / pgp_sym_decrypt (PGP symmetric encryption)
    # Only available if PostgreSQL was built with OpenSSL
    encrypt_call = FunctionCall(
        dialect, "pgp_sym_encrypt",
        Literal(dialect, "sensitive data"),
        Literal(dialect, "encryption_key"),
    ).as_("encrypted")
    query = QueryExpression(
        dialect=dialect,
        select=[encrypt_call],
    )
    sql, params = query.to_sql()
    print(f"\n--- PGP symmetric encryption ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Encrypted (PGP message): {result.data}")

    # Decrypt the data back
    encrypted = result.data[0]['encrypted'] if result.data else None
    if encrypted:
        decrypt_call = FunctionCall(
            dialect, "pgp_sym_decrypt",
            Literal(dialect, encrypted),
            Literal(dialect, "encryption_key"),
        ).as_("decrypted")
        query = QueryExpression(
            dialect=dialect,
            select=[decrypt_call],
        )
        sql, params = query.to_sql()
        print(f"\n--- PGP symmetric decryption ---")
        print(f"SQL: {sql}")
        print(f"Params: {params}")
        result = backend.execute(sql, params, options=opts)
        print(f"Decrypted: {result.data}")

else:
    print("\nSkipping execution - pgcrypto not available on this server")
    print("To enable pgcrypto, run: CREATE EXTENSION pgcrypto;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
