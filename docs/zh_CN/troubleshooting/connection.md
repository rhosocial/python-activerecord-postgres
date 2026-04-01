# 常见连接错误

## 概述

本节介绍常见的 PostgreSQL 连接错误及其解决方案。

## 连接被拒绝

### 错误信息
```
psycopg.OperationalError: connection failed: Connection refused
```

### 原因
- PostgreSQL 服务未运行
- 端口不正确
- 防火墙阻止
- pg_hba.conf 不允许该连接

### 解决方案
```bash
# 检查 PostgreSQL 是否运行
sudo systemctl status postgresql

# 检查端口
telnet localhost 5432

# 检查 pg_hba.conf 允许连接
# 编辑 /etc/postgresql/16/main/pg_hba.conf
# 添加：host all all 0.0.0.0/0 md5
```

## 认证失败

### 错误信息
```
psycopg.OperationalError: FATAL: password authentication failed for user "postgres"
```

### 原因
- 用户名或密码不正确
- 用户没有数据库访问权限

### 解决方案
```sql
-- 在 PostgreSQL 服务器上执行
CREATE USER test_user WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE test TO test_user;
```

## 连接超时

### 错误信息
```
psycopg.OperationalError: connection timeout expired
```

### 原因
- 网络问题
- connect_timeout 设置过短
- 服务器过载

### 解决方案
```python
config = PostgreSQLConnectionConfig(
    host='remote.host.com',
    connect_timeout=30,  # 增加超时时间
)
```

## SSL 连接错误

### 错误信息
```
psycopg.OperationalError: SSL connection failed
```

### 原因
- SSL 证书问题
- SSL 配置不正确
- 服务器要求 SSL 但客户端未配置

### 解决方案
```python
config = PostgreSQLConnectionConfig(
    host='remote.host.com',
    sslmode='require',  # 选项：disable, prefer, require, verify-ca, verify-full
    sslrootcert='/path/to/ca.crt',  # 用于 verify-ca 或 verify-full
)
```

## 数据库不存在

### 错误信息
```
psycopg.OperationalError: FATAL: database "test" does not exist
```

### 解决方案
```bash
# 创建数据库
psql -U postgres -c "CREATE DATABASE test;"

# 或使用 createdb 工具
createdb -U postgres test
```

## 连接丢失与自动恢复

### 概述

在长时间运行的应用程序中，数据库连接可能因各种原因被断开。PostgreSQL 后端实现了双层保护机制，确保连接丢失时能够自动恢复。

### 常见连接丢失场景

| 场景 | 原因 | SQLSTATE 代码 |
|------|------|---------------|
| `idle_in_transaction_session_timeout` 超时 | 事务空闲时间超过 PostgreSQL 设置 | 08006 |
| 连接被终止 | DBA 执行 `pg_terminate_backend()` | 57P01 |
| 网络不稳定 | 网络问题导致 TCP 连接断开 | 08006 |
| 服务器重启 | PostgreSQL 服务器重启或崩溃 | 08006, 57P01 |
| 防火墙超时 | 防火墙关闭长时间空闲的 TCP 连接 | 08006 |
| 管理员关闭 | 服务器正在被管理员关闭 | 57P01, 57P02 |

### 自动恢复机制

PostgreSQL 后端实现了两层自动恢复：

#### 方案 A：查询前连接检查

在每次查询执行前，后端自动检查连接状态：

```python
def _get_cursor(self):
    """获取数据库游标，确保连接处于活动状态"""
    if not self._connection:
        # 无连接，建立新连接
        self.connect()
    elif self._connection.closed or self._connection.broken:
        # 连接已断开，重新连接
        self._connection = None
        self.connect()
    return self._connection.cursor()
```

**特点**：
- 主动检查，在查询执行前检测问题
- 使用 psycopg v3 的 `closed` 和 `broken` 属性检查连接状态
- 对应用层完全透明

**注意**：与 MySQL 的 `is_connected()` 方法主动轮询服务器不同，psycopg v3 的 `closed` 和 `broken` 属性是惰性更新的。它们可能不会立即反映服务器端的断开，直到尝试下一次操作。这就是为什么方案 B（错误重试）也是必要的。

#### 方案 B：错误重试机制

当查询执行过程中发生连接错误时，后端自动重试：

```python
# PostgreSQL 连接错误 SQLSTATE 代码
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

**特点**：
- 被动恢复，在查询失败时触发
- 最多重试 2 次
- 仅对连接错误重试，其他错误直接抛出
- 通过 SQLSTATE 代码和错误消息模式检测连接错误

### 手动保活机制

对于需要主动维护连接的场景，使用 `ping()` 方法：

```python
# 检查连接状态，不自动重连
is_alive = backend.ping(reconnect=False)

# 检查连接状态，如果断开则自动重连
is_alive = backend.ping(reconnect=True)
```

#### 保活最佳实践

在多进程 Worker 场景中，实现定期保活：

```python
import threading
import time

def keepalive_worker(backend, interval=60):
    """后台保活线程"""
    while True:
        time.sleep(interval)
        if backend.ping(reconnect=True):
            logger.debug("连接保活成功")
        else:
            logger.warning("连接保活失败")

# 启动保活线程
keepalive_thread = threading.Thread(
    target=keepalive_worker,
    args=(backend, 60),
    daemon=True
)
keepalive_thread.start()
```

### 异步后端支持

异步后端（`AsyncPostgresBackend`）提供相同的连接恢复机制：

```python
# 异步 ping
is_alive = await async_backend.ping(reconnect=True)

# 异步查询也会自动重连
result = await async_backend.execute("SELECT 1")
```

### 最佳实践

#### 1. 合理配置 PostgreSQL 超时参数

```sql
-- 查看当前设置
SHOW idle_in_transaction_session_timeout;
SHOW statement_timeout;

-- 推荐设置（根据您的需求调整）
SET GLOBAL idle_in_transaction_session_timeout = '10min';
-- statement_timeout 应根据查询复杂度在每个会话中设置
```

#### 2. 使用连接池

对于高并发场景，配置连接池：

```python
config = PostgreSQLConnectionConfig(
    host='localhost',
    database='mydb',
    # 连接池设置
    # 注意：psycopg v3 内置连接池支持
)
```

#### 3. 多进程 Worker 场景

每个 Worker 进程应有自己的后端实例：

```python
def worker_process(worker_id, config):
    """Worker 进程入口"""
    # 在进程内创建独立的后端实例
    backend = PostgresBackend(connection_config=config)
    backend.connect()

    try:
        # 执行任务
        do_work(backend)
    finally:
        backend.disconnect()
```

#### 4. 监控连接状态

```python
import logging

# 启用后端日志
logging.getLogger('rhosocial.activerecord.backend').setLevel(logging.DEBUG)

# 后端会自动记录连接恢复事件
# DEBUG: Connection lost, reconnecting...
# DEBUG: Reconnected successfully
```

### 连接错误 SQLSTATE 代码参考

| SQLSTATE | 名称 | 描述 |
|----------|------|------|
| 08000 | CONNECTION_EXCEPTION | 一般连接异常 |
| 08001 | SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION | 客户端无法建立连接 |
| 08003 | CONNECTION_DOES_NOT_EXIST | 连接不存在 |
| 08004 | SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION | 服务器拒绝连接 |
| 08006 | CONNECTION_FAILURE | 连接失败 |
| 08007 | TRANSACTION_RESOLUTION_UNKNOWN | 事务解决状态未知 |
| 08P01 | PROTOCOL_VIOLATION | 协议违规 |
| 57P01 | ADMIN_SHUTDOWN | 管理员命令关闭 |
| 57P02 | CRASH_SHUTDOWN | 服务器崩溃 |
| 57P03 | CANNOT_CONNECT_NOW | 当前无法连接 |
| 57P04 | DATABASE_DROPPED | 数据库已删除 |

💡 *AI 提示词：* "PostgreSQL 后端如何自动从连接丢失中恢复？双层保护机制是什么？"
