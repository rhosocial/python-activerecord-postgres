"""
UPSERT (INSERT ... ON CONFLICT) - PostgreSQL 9.5+.

This example demonstrates:
1. INSERT ... ON CONFLICT DO NOTHING
2. INSERT ... ON CONFLICT DO UPDATE
3. UPSERT with conditional updates
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host=os.getenv('PG_HOST', 'localhost'),
    port=int(os.getenv('PG_PORT', 5432)),
    database=os.getenv('PG_DATABASE', 'test'),
    username=os.getenv('PG_USERNAME', 'postgres'),
    password=os.getenv('PG_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()

# Create table for testing
backend.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(100) UNIQUE NOT NULL,
        email VARCHAR(100) NOT NULL,
        login_count INTEGER DEFAULT 0
    )
""")
backend.execute("TRUNCATE TABLE users RESTART IDENTITY")

# ============================================================
# SECTION: INSERT ON CONFLICT DO NOTHING
# ============================================================
# Ignore duplicate key violations

backend.execute("""
    INSERT INTO users (username, email, login_count)
    VALUES ('alice', 'alice@example.com', 1)
    ON CONFLICT (username) DO NOTHING
""")

# Try to insert again - will be ignored
backend.execute("""
    INSERT INTO users (username, email, login_count)
    VALUES ('alice', 'different@example.com', 2)
    ON CONFLICT (username) DO NOTHING
""")

result = backend.execute("SELECT * FROM users WHERE username = 'alice'")
print(f"DO NOTHING result: {result.data}")

# ============================================================
# SECTION: INSERT ON CONFLICT DO UPDATE
# ============================================================
# Update existing rows on conflict

backend.execute("""
    INSERT INTO users (username, email, login_count)
    VALUES ('bob', 'bob@example.com', 1)
    ON CONFLICT (username) DO UPDATE SET
        email = EXCLUDED.email,
        login_count = users.login_count + 1
""")

# Insert again - will update
backend.execute("""
    INSERT INTO users (username, email, login_count)
    VALUES ('bob', 'bob_new@example.com', 1)
    ON CONFLICT (username) DO UPDATE SET
        email = EXCLUDED.email,
        login_count = users.login_count + 1
""")

result = backend.execute("SELECT * FROM users WHERE username = 'bob'")
print(f"DO UPDATE result: {result.data}")

# ============================================================
# SECTION: Conditional UPSERT
# ============================================================
# Use WHERE clause for conditional updates

backend.execute("""
    INSERT INTO users (username, email, login_count)
    VALUES ('charlie', 'charlie@example.com', 1)
    ON CONFLICT (username) DO UPDATE SET
        login_count = users.login_count + 1
    WHERE users.login_count < 5
""")

# ============================================================
# SECTION: UPSERT with RETURNING
# ============================================================
# Return the inserted/updated row

result = backend.execute("""
    INSERT INTO users (username, email, login_count)
    VALUES ('david', 'david@example.com', 1)
    ON CONFLICT (username) DO UPDATE SET
        login_count = users.login_count + 1
    RETURNING *
""")
print(f"UPSERT RETURNING result: {result.data}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.execute("DROP TABLE IF EXISTS users")
backend.disconnect()

# ============================================================
# SECTION: Summary
# ============================================================
# Key points:
# 1. Requires PostgreSQL 9.5+
# 2. Use ON CONFLICT (column) to specify conflict target
# 3. DO NOTHING to skip conflicts
# 4. DO UPDATE to update existing rows
# 5. EXCLUDED table references the attempted values
# 6. Use WHERE for conditional updates
# 7. Can use RETURNING with UPSERT