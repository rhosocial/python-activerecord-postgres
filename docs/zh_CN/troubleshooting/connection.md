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

💡 *AI 提示词：* "如何排查 PostgreSQL 连接错误？"
