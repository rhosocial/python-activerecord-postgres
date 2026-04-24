# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pgcrypto.py
"""
pgcrypto extension - cryptographic functions.

This example demonstrates:
1. Check if pgcrypto extension is available
2. CREATE EXTENSION using CreateExtensionExpression
3. Use crypt + gen_salt for password hashing
4. Use digest for data integrity
5. Use hmac for message authentication
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
    result = backend.execute(
        "SELECT crypt('my_secret_password', gen_salt('bf', 8)) AS hashed_password",
        (),
        options=opts,
    )
    print(f"\n--- Password hashing (crypt + gen_salt) ---")
    print(f"Hashed password: {result.data}")

    # Example 2: Verify password against stored hash
    # crypt() extracts the salt from the existing hash automatically
    result = backend.execute(
        "SELECT crypt('my_secret_password', "
        "  (SELECT crypt('my_secret_password', gen_salt('bf', 8)))"
        ") = (SELECT crypt('my_secret_password', gen_salt('bf', 8))) AS password_matches_demo",
        (),
        options=opts,
    )
    print(f"\n--- Password verification ---")
    print(f"Note: In practice, store the hash from step 1, then verify with:")
    print(f"  crypt(input_password, stored_hash) = stored_hash")

    # Example 3: Data digest (hash) using digest()
    # digest(data, algorithm) returns bytea
    # Algorithms: md5, sha1, sha224, sha256, sha384, sha512
    result = backend.execute(
        "SELECT encode(digest('Hello, World!', 'sha256'), 'hex') AS sha256_digest",
        (),
        options=opts,
    )
    print(f"\n--- Data digest (SHA-256) ---")
    print(f"SHA-256 of 'Hello, World!': {result.data}")

    # Example 4: Multiple digest algorithms comparison
    result = backend.execute(
        "SELECT "
        "  encode(digest('test data', 'md5'), 'hex') AS md5, "
        "  encode(digest('test data', 'sha1'), 'hex') AS sha1, "
        "  encode(digest('test data', 'sha256'), 'hex') AS sha256",
        (),
        options=opts,
    )
    print(f"\n--- Multiple digest algorithms ---")
    print(f"Results: {result.data}")

    # Example 5: HMAC (Hash-based Message Authentication Code)
    # hmac(data, key, algorithm) returns bytea
    result = backend.execute(
        "SELECT encode(hmac('message data', 'secret_key', 'sha256'), 'hex') AS hmac_sha256",
        (),
        options=opts,
    )
    print(f"\n--- HMAC-SHA256 ---")
    print(f"HMAC of 'message data' with key 'secret_key': {result.data}")

    # Example 6: pgp_sym_encrypt / pgp_sym_decrypt (PGP symmetric encryption)
    # Only available if PostgreSQL was built with OpenSSL
    result = backend.execute(
        "SELECT pgp_sym_encrypt('sensitive data', 'encryption_key') AS encrypted",
        (),
        options=opts,
    )
    print(f"\n--- PGP symmetric encryption ---")
    print(f"Encrypted (PGP message): {result.data}")

    # Decrypt the data back
    encrypted = result.data[0][0] if result.data else None
    if encrypted:
        result = backend.execute(
            "SELECT pgp_sym_decrypt(%s, 'encryption_key') AS decrypted",
            (encrypted,),
            options=opts,
        )
        print(f"Decrypted: {result.data}")

else:
    print("\nSkipping execution - pgcrypto not available on this server")
    print("To enable pgcrypto, run: CREATE EXTENSION pgcrypto;")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
