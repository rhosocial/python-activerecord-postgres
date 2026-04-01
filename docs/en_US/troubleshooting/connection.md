# Common Connection Errors

## Overview

This section covers common PostgreSQL connection errors and their solutions.

## Connection Refused

### Error Message
```
psycopg.OperationalError: connection failed: Connection refused
```

### Causes
- PostgreSQL service is not running
- Incorrect port
- Firewall blocking
- pg_hba.conf not allowing the connection

### Solutions
```bash
# Check if PostgreSQL is running
sudo systemctl status postgresql

# Check port
telnet localhost 5432

# Check pg_hba.conf allows connections
# Edit /etc/postgresql/16/main/pg_hba.conf
# Add: host all all 0.0.0.0/0 md5
```

## Authentication Failed

### Error Message
```
psycopg.OperationalError: FATAL: password authentication failed for user "postgres"
```

### Causes
- Incorrect username or password
- User does not have database access permissions

### Solutions
```sql
-- Execute on PostgreSQL server
CREATE USER test_user WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE test TO test_user;
```

## Connection Timeout

### Error Message
```
psycopg.OperationalError: connection timeout expired
```

### Causes
- Network issues
- connect_timeout setting is too short
- Server overloaded

### Solutions
```python
config = PostgreSQLConnectionConfig(
    host='remote.host.com',
    connect_timeout=30,  # Increase timeout
)
```

## SSL Connection Error

### Error Message
```
psycopg.OperationalError: SSL connection failed
```

### Causes
- SSL certificate issues
- Incorrect SSL configuration
- Server requires SSL but client not configured

### Solutions
```python
config = PostgreSQLConnectionConfig(
    host='remote.host.com',
    sslmode='require',  # Options: disable, prefer, require, verify-ca, verify-full
    sslrootcert='/path/to/ca.crt',  # For verify-ca or verify-full
)
```

## Database Does Not Exist

### Error Message
```
psycopg.OperationalError: FATAL: database "test" does not exist
```

### Solutions
```bash
# Create the database
psql -U postgres -c "CREATE DATABASE test;"

# Or using createdb utility
createdb -U postgres test
```

## Connection Loss and Automatic Recovery

### Overview

In long-running applications, database connections may be dropped for various reasons. The PostgreSQL backend implements a dual-layer protection mechanism to ensure automatic recovery when connections are lost.

### Common Connection Loss Scenarios

| Scenario | Cause | SQLSTATE Codes |
|----------|-------|----------------|
| `idle_in_transaction_session_timeout` expiry | Transaction idle time exceeds PostgreSQL's setting | 08006 |
| Connection terminated | DBA executes `pg_terminate_backend()` | 57P01 |
| Network instability | Network issues cause TCP connection to drop | 08006 |
| Server restart | PostgreSQL server restart or crash | 08006, 57P01 |
| Firewall timeout | Firewall closes long-idle TCP connections | 08006 |
| Admin shutdown | Server is being shut down by administrator | 57P01, 57P02 |

### Automatic Recovery Mechanism

The PostgreSQL backend implements two layers of automatic recovery:

#### Plan A: Pre-Query Connection Check

Before each query execution, the backend automatically checks the connection status:

```python
def _get_cursor(self):
    """Get a database cursor, ensuring connection is active."""
    if not self._connection:
        # No connection, establish new one
        self.connect()
    elif self._connection.closed or self._connection.broken:
        # Connection lost, reconnect
        self._connection = None
        self.connect()
    return self._connection.cursor()
```

**Features**:
- Proactive checking, detects issues before query execution
- Uses psycopg v3's `closed` and `broken` attributes to check connection state
- Completely transparent to the application layer

**Note**: Unlike MySQL's `is_connected()` method which actively polls the server, psycopg v3's `closed` and `broken` attributes are updated lazily. They may not immediately reflect server-side disconnection until the next operation is attempted. This is why Plan B (error retry) is also necessary.

#### Plan B: Error Retry Mechanism

When a connection error occurs during query execution, the backend automatically retries:

```python
# PostgreSQL connection error SQLSTATE codes
CONNECTION_ERROR_SQLSTATES = {
    "08000",  # CONNECTION_EXCEPTION
    "08001",  # SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
    "08003",  # CONNECTION_DOES_NOT_EXIST
    "08004",  # SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
    "08006",  # CONNECTION_FAILURE
    "08007",  # TRANSACTION_RESOLUTION_UNKNOWN
    "08P01",  # PROTOCOL_VIOLATION
    "57P01",  # ADMIN_SHUTDOWN
    "57P02",  # CRASH_SHUTDOWN
    "57P03",  # CANNOT_CONNECT_NOW
    "57P04",  # DATABASE_DROPPED
}

async def execute(self, sql, params=None, *, options=None, max_retries=2):
    for attempt in range(max_retries + 1):
        try:
            return await super().execute(sql, params, options=options)
        except OperationalError as e:
            if self._is_connection_error_message(str(e)) and attempt < max_retries:
                await self._reconnect()
                continue
            raise
```

**Features**:
- Reactive recovery, triggered on query failure
- Maximum 2 retries
- Only retries on connection errors, other errors are raised directly
- Detects connection errors via SQLSTATE codes and error message patterns

### Manual Keep-Alive Mechanism

For scenarios requiring proactive connection maintenance, use the `ping()` method:

```python
# Check connection status without auto-reconnect
is_alive = backend.ping(reconnect=False)

# Check connection status with auto-reconnect if disconnected
is_alive = backend.ping(reconnect=True)
```

#### Keep-Alive Best Practices

In multi-process worker scenarios, implement periodic keep-alive:

```python
import threading
import time

def keepalive_worker(backend, interval=60):
    """Background keep-alive thread"""
    while True:
        time.sleep(interval)
        if backend.ping(reconnect=True):
            logger.debug("Connection keepalive successful")
        else:
            logger.warning("Connection keepalive failed")

# Start keep-alive thread
keepalive_thread = threading.Thread(
    target=keepalive_worker,
    args=(backend, 60),
    daemon=True
)
keepalive_thread.start()
```

### Async Backend Support

The async backend (`AsyncPostgresBackend`) provides the same connection recovery mechanism:

```python
# Async ping
is_alive = await async_backend.ping(reconnect=True)

# Async queries also auto-reconnect
result = await async_backend.execute("SELECT 1")
```

### Best Practices

#### 1. Configure PostgreSQL Timeout Parameters Appropriately

```sql
-- View current settings
SHOW idle_in_transaction_session_timeout;
SHOW statement_timeout;

-- Recommended settings (adjust based on your requirements)
SET GLOBAL idle_in_transaction_session_timeout = '10min';
-- statement_timeout should be set per-session based on query complexity
```

#### 2. Use Connection Pool

For high-concurrency scenarios, configure connection pooling:

```python
config = PostgreSQLConnectionConfig(
    host='localhost',
    database='mydb',
    # Connection pool settings
    # Note: psycopg v3 has built-in connection pool support
)
```

#### 3. Multi-Process Worker Scenarios

Each worker process should have its own backend instance:

```python
def worker_process(worker_id, config):
    """Worker process entry point"""
    # Create independent backend instance within the process
    backend = PostgresBackend(connection_config=config)
    backend.connect()

    try:
        # Execute tasks
        do_work(backend)
    finally:
        backend.disconnect()
```

#### 4. Monitor Connection Status

```python
import logging

# Enable backend logging
logging.getLogger('rhosocial.activerecord.backend').setLevel(logging.DEBUG)

# Backend will automatically log connection recovery events
# DEBUG: Connection lost, reconnecting...
# DEBUG: Reconnected successfully
```

### Connection Error SQLSTATE Codes Reference

| SQLSTATE | Name | Description |
|----------|------|-------------|
| 08000 | CONNECTION_EXCEPTION | General connection exception |
| 08001 | SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION | Client unable to establish connection |
| 08003 | CONNECTION_DOES_NOT_EXIST | Connection does not exist |
| 08004 | SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION | Server rejected connection |
| 08006 | CONNECTION_FAILURE | Connection failure |
| 08007 | TRANSACTION_RESOLUTION_UNKNOWN | Transaction resolution unknown |
| 08P01 | PROTOCOL_VIOLATION | Protocol violation |
| 57P01 | ADMIN_SHUTDOWN | Administrator commanded shutdown |
| 57P02 | CRASH_SHUTDOWN | Server crash |
| 57P03 | CANNOT_CONNECT_NOW | Cannot connect now |
| 57P04 | DATABASE_DROPPED | Database dropped |

💡 *AI Prompt:* "How does PostgreSQL backend automatically recover from connection losses? What is the dual-layer protection mechanism?"
