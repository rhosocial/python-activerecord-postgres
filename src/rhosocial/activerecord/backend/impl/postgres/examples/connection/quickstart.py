"""
Quick Start: Connect to PostgreSQL and execute queries.

This example demonstrates:
1. How to establish a connection to PostgreSQL
2. How to view connection information
3. How to execute queries
4. How to access query results (result data structure)

For more details on PostgreSQL configuration, see:
- PostgreSQLConnectionConfig
- PostgresBackend
"""

# ============================================================
# SECTION: Connection Setup
# ============================================================
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

# Create connection configuration
# See available parameters in PostgresConnectionConfig
config = PostgresConnectionConfig(
    host='localhost',       # PostgreSQL server host
    port=5432,              # PostgreSQL server port
    database='test_db',     # Database name
    username='postgres',    # Username
    password='password',    # Password
    # Optional parameters:
    # sslmode='prefer',     # SSL mode (disable/prefer/require)
    # connect_timeout=10,   # Connection timeout in seconds
    # application_name='myapp',  # Application name for logging
)

# Create backend instance
backend = PostgresBackend(connection_config=config)

# ============================================================
# SECTION: Establish Connection
# ============================================================
# Connect to the database
# This will:
# 1. Establish TCP connection
# 2. Perform authentication
# 3. Initialize session
backend.connect()

# After connecting, you can access:
# - backend.connection: the underlying connection
# - backend.dialect: the dialect for SQL generation

# ============================================================
# SECTION: View Connection Information
# ============================================================
print(f"Connected to: {config.host}:{config.port}")
print(f"Database: {config.database}")
print(f"User: {config.username}")

# ============================================================
# SECTION: Execute Queries
# ============================================================
# Execute a simple query
result = backend.execute("SELECT 1 AS test")

# ============================================================
# SECTION: Access Query Results
# ============================================================
# QueryResult structure:
# - data: List[Dict] - query results as list of dictionaries
# - affected_rows: int - number of rows affected (for INSERT/UPDATE/DELETE)
# - last_insert_id: Any - last inserted ID (for INSERT with auto-increment)
# - duration: float - query execution duration in seconds

print(f"Result data: {result.data}")       # [{'test': 1}]
print(f"Rows: {result.affected_rows}")    # 1 (for SELECT, shows number of rows)
print(f"Duration: {result.duration:.3f}s")

# Access individual rows
if result.data:
    row = result.data[0]
    value = row['test']
    print(f"Value from first row: {value}")

# ============================================================
# SECTION: Execute Parameterized Queries (Recommended)
# ============================================================
# Use parameterized queries to prevent SQL injection
# Parameters are passed as a separate tuple
result = backend.execute(
    "SELECT * FROM users WHERE id = %s AND status = %s",
    (1, 'active')
)
print(f"Parameterized query result: {result.data}")

# ============================================================
# SECTION: Handle Transactions
# ============================================================
# Default: autocommit is False, each execute is in its own transaction
# For explicit transaction control:
with backend.connection() as conn:
    conn.execute("BEGIN")
    try:
        conn.execute("INSERT INTO logs (message) VALUES (%s)", ('test',))
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        raise e

# ============================================================
# SECTION: Disconnect
# ============================================================
# Always disconnect when done to release resources
backend.disconnect()

# ============================================================
# SECTION: Error Handling
# ============================================================
try:
    backend.connect()
    result = backend.execute("SELECT * FROM nonexistent_table")
except Exception as e:
    print(f"Error: {e}")
    # Handle specific exceptions:
    # - ConnectionError: connection failed
    # - OperationalError: SQL execution error
    # - ProgrammingError: SQL syntax error
finally:
    if backend.connection:
        backend.disconnect()

# ============================================================
# SECTION: Summary
# ============================================================
# Key points:
# 1. Create PostgresConnectionConfig with connection parameters
# 2. Create PostgresBackend with the config
# 3. Call backend.connect() to establish connection
# 4. Use backend.execute() to run queries
# 5. Access result.data for query results
# 6. Call backend.disconnect() when done
#
# Result data structure:
# - QueryResult.data: List[Dict] - query results
# - QueryResult.affected_rows: int
# - QueryResult.last_insert_id: Any
# - QueryResult.duration: float