# Test Configuration

## Overview

This section describes how to configure the testing environment for the PostgreSQL backend.

For general testing strategies (DummyBackend, SQLite integration testing), see the [Core Backend Testing Guide](https://github.com/Rhosocial/python-activerecord/tree/main/docs/en_US/testing/backend_testing.md).

## End-to-End Testing with PostgreSQL Backend

For complete PostgreSQL behavior testing, use the PostgreSQL backend:

```python
import os
from rhosocial.activerecord.backend.impl.postgres import PostgreSQLBackend, PostgreSQLConnectionConfig


class User(ActiveRecord):
    name: str
    email: str
    
    c: ClassVar[FieldProxy] = FieldProxy()
    
    @classmethod
    def table_name(cls) -> str:
        return 'users'


# Read configuration from environment variables
config = PostgreSQLConnectionConfig(
    host=os.environ.get('PG_HOST', 'localhost'),
    port=int(os.environ.get('PG_PORT', 5432)),
    database=os.environ.get('PG_DATABASE', 'test'),
    username=os.environ.get('PG_USER', 'postgres'),
    password=os.environ.get('PG_PASSWORD', ''),
)
User.configure(config, PostgreSQLBackend)
```

## Test Fixtures

```python
import pytest
from rhosocial.activerecord.backend.impl.postgres import PostgreSQLBackend, PostgreSQLConnectionConfig


@pytest.fixture
def postgres_config():
    return PostgreSQLConnectionConfig(
        host='localhost',
        port=5432,
        database='test',
        username='postgres',
        password='password',
    )


@pytest.fixture
def postgres_backend(postgres_config):
    backend = PostgreSQLBackend(connection_config=postgres_config)
    backend.connect()
    yield backend
    backend.disconnect()


def test_connection(postgres_backend):
    version = postgres_backend.get_server_version()
    assert version is not None
```

💡 *AI Prompt:* "What is the difference between unit tests, integration tests, and end-to-end tests?"
