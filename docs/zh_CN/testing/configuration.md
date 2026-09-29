# 测试配置

## 概述

本节介绍如何配置 PostgreSQL 后端的测试环境。

## 使用 Dummy 后端进行单元测试

推荐使用 `dummy` 后端进行单元测试，它不需要真实的数据库连接：

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


# 配置 Dummy 后端
config = ConnectionConfig()
User.configure(config, DummyBackend)
```

## 使用 SQLite 后端进行集成测试

对于需要真实数据库行为的测试，使用 SQLite 后端：

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


# 配置 SQLite 内存数据库
config = SQLiteConnectionConfig(database=':memory:')
User.configure(config, SQLiteBackend)
```

## 使用 PostgreSQL 后端进行端到端测试

对于完整的 PostgreSQL 行为测试，使用 PostgreSQL 后端：

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


# 从环境变量读取配置
config = PostgresConnectionConfig(
    host=os.environ.get('PG_HOST', 'localhost'),
    port=int(os.environ.get('PG_PORT', 5432)),
    database=os.environ.get('PG_DATABASE', 'test'),
    username=os.environ.get('PG_USER', 'postgres'),
    password=os.environ.get('PG_PASSWORD', ''),
)
User.configure(config, PostgresBackend)
```

## 测试 Fixtures

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

💡 *AI 提示词：* "单元测试、集成测试和端到端测试之间有什么区别？"
