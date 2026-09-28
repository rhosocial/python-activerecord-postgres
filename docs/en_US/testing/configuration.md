# Test Configuration

## Overview

This section describes how to configure the testing environment for the PostgreSQL backend.

## Unit Testing with Dummy Backend

The `dummy` backend is recommended for unit tests as it does not require a real database connection:

```python
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.backend.impl.dummy.backend import DummyBackend
rhosocial.activerecord.backend.config import ConnectionConfig
class User(ActiveRecord):
    name: str
    email: str
    
    c: ClassVar[FieldProxy] = FieldProxy()
    
    @classmethod
    def table_name(cls) -> str:
        return 'users'


# Configure Dummy backend
config = ConnectionConfig()
User.configure(config, DummyBackend)
```

## Integration Testing with SQLite Backend

For tests requiring real database behavior, use the SQLite backend:

```python
from rhosocial.activerecord.backend.impl.sqlite.backend import SQLiteBackend
from rhosocial.activerecord.backend.impl.sqlite.config import SQLiteConnectionConfig


class User(ActiveRecord):
    name: str
    email: str
    
    c: ClassVar[FieldProxy] = FieldProxy()
    
    @classmethod
    def table_name(cls) -> str:
        return 'users'


# Configure SQLite in-memory database
config = SQLiteConnectionConfig(database=':memory:')
User.configure(config, SQLiteBackend)
```

## End-to-End Testing with PostgreSQL Backend

For complete PostgreSQL behavior testing, use the PostgreSQL backend:

```python
import os
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
class User(ActiveRecord):
    name: str
    email: str
    
    c: ClassVar[FieldProxy] = FieldProxy()
    
    @classmethod
    def table_name(cls) -> str:
        return 'users'


# Read configuration from environment variables
config = PostgresConnectionConfig(
    host=os.environ.get('PG_HOST', 'localhost'),
    port=int(os.environ.get('PG_PORT', 5432)),
    database=os.environ.get('PG_DATABASE', 'test'),
    username=os.environ.get('PG_USER', 'postgres'),
    password=os.environ.get('PG_PASSWORD', ''),
)
User.configure(config, PostgresBackend)
```

## Test Fixtures

```python
import pytest
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
@pytest.fixture
def postgres_config():
    return PostgresConnectionConfig(
        host='localhost',
        port=5432,
        database='test',
        username='postgres',
        password='password',
    )


@pytest.fixture
def postgres_backend(postgres_config):
    backend = PostgresBackend(connection_config=postgres_config)
    backend.connect()
    yield backend
    backend.disconnect()


def test_connection(postgres_backend):
    version = postgres_backend.get_server_version()
    assert version is not None
```

💡 *AI Prompt:* "What is the difference between unit tests, integration tests, and end-to-end tests?"
