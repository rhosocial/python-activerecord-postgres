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

💡 *AI Prompt:* "How to troubleshoot PostgreSQL connection errors?"
