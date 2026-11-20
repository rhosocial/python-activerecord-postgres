# 迁移（Migrations）— PostgreSQL 后端

> **范围说明：** 本章只讲 PostgreSQL 后端与迁移相关的特有事项：如何为迁移配置 PostgreSQL 后端、
> 事务性 DDL、受版本门禁的特性/类型，以及两套迁移 API 如何接到本后端上。本章**不**重复讲解迁移
> 系统本身——两套编写 API、顺序与依赖规则、版本记录、dry run 内部机制、`migrate` CLI 以及异常
> 层级，全部记录在[核心库迁移章节](https://github.com/rhosocial/python-activerecord/blob/main/docs/zh_CN/migration/README.md)中。
> 如果你是初次接触迁移，请先阅读那一章；需要 PostgreSQL 细节时再回到这里。

> 💡 **AI 提示词：** "给我展示一个 PostgreSQL 迁移：先在 dry run 中渲染 DDL，然后在事务中应用它，
> 并在后续语句失败时干净地回滚。"

## 1. 为迁移配置 PostgreSQL 后端

迁移运行在由你提供的后端实例上。构造本后端的 `PostgresConnectionConfig` 与 `PostgresBackend`，
然后——很重要——在运行迁移前调用 `introspect_and_adapt()`。该调用会探测实际服务器版本与已安装
的扩展，使方言能正确渲染受版本门禁的 DDL（见 §3）。

```python
import os

from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    database=os.getenv("POSTGRES_DATABASE", "test"),
    username=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", ""),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
backend.introspect_and_adapt()  # detect server version + installed extensions
```

本后端特有的注意点：

- `PostgresConnectionConfig` 接受 PostgreSQL/libpq 风格的选项（`sslmode`、`search_path`、
  `default_schema`、`application_name`、`target_session_attrs`、`connect_timeout`、`service` 等）。
  当迁移面向非 `public` schema 时，设置 `search_path` / `default_schema` 很有价值。参见
  [连接配置](../installation_and_configuration/configuration.md)。
- 连接以驱动层 autocommit 运行；事务边界由框架显式管理。你**不需要**自己在迁移组外层开启事务——
  `run_migration_group` 会处理（§2）。
- 当无法查询服务器时，`introspect_and_adapt()` 会回退到默认版本。如果某个迁移依赖特定服务器版本，
  请显式检查，而不要假设（§3）。

## 2. 事务性 DDL — PostgreSQL 的关键优势

**PostgreSQL 的 DDL 是事务性的。** `CREATE TABLE`、`ALTER TABLE`、`DROP TABLE` 等模式语句会参与事务，
并且可以**干净地回滚**。这是相对于 DDL *非事务性* 的后端而言真正的优势：在那些后端上，迁移中途
失败会留下部分应用的 schema，必须手工修复。而在 PostgreSQL 上，同样的失败会让 schema 保持原样。

系统如何利用这一点：

- 本后端的方言声明 `supports_transactional_ddl() -> True`。`run_migration_group` 路径会探测
  `backend.dialect.supports_transactional_ddl()`，并在函数失败时把结果记录到
  `MigrationFunctionExecutionError.transactional_ddl` 上。
- 在默认的 `TransactionMode.AUTO` 下，一个组会开启一个事务，运行组内所有函数，只有全部成功才提交。
  任何异常都会回滚整个组——包括 DDL。dry run 从不开启事务，也不写入任何内容。
- 因此，PostgreSQL 上的多语句迁移实际上是**全有或全无（all-or-nothing）**：你可以把
  `up()`/`down()` 函数写成一串 DDL 步骤，而无需为某个失败步骤手写补偿逻辑。

```python
from rhosocial.activerecord.backend.migration import (
    MigrationFunctionExecutionError,
    run_migration_group,
)

assert backend.dialect.supports_transactional_ddl() is True

run_migration_group(backend, [user_fqn], dry_run=True)      # renders SQL, writes nothing
run_migration_group(backend, [user_fqn], record_store=store)  # one transaction, then commit

try:
    run_migration_group(backend, [failing_fqn], record_store=store)
except MigrationFunctionExecutionError as exc:
    assert exc.transactional_ddl is True  # the failed DDL was rolled back
```

仍然适用的 PostgreSQL 特有注意事项：

- 少数 PostgreSQL 语句根本不能在事务块中执行——尤其是 `CREATE INDEX CONCURRENTLY`、
  `DROP INDEX CONCURRENTLY`、`REFRESH MATERIALIZED VIEW CONCURRENTLY` 以及
  `CREATE DATABASE`/`DROP DATABASE`。请把它们排除在事务性迁移组之外（用
  `rhosocial.activerecord.backend.named_expression` 中的 `transaction_mode=TransactionMode.NONE`
  单独运行），并自行处理补偿。
- DDL 会加锁；针对同一关系的并发迁移可能阻塞或死锁。必要时参见本后端的
  [死锁处理](../transaction_support/deadlock.md)指南。
- 隔离级别、savepoint 与 DEFERRABLE 约束参见 [事务支持](../transaction_support/README.md)。

## 3. 受版本门禁的特性与类型

PostgreSQL 的特性在不同服务器版本中陆续引入，本后端只渲染目标服务器支持的内容。
`introspect_and_adapt()` 会把实际服务器版本缓存到方言上；迁移应据此对版本相关 DDL 做门禁。

- `backend.dialect.version_at_least((major, minor, patch))` —— 安全的判定方法；版本未知时返回
  `False`（从不抛异常）。`backend.dialect.version_or_none` 返回版本元组或 `None`。
  `backend.get_server_version()` 直接查询服务器。
- 受门禁能力的例子：`JSONB`（9.4+）、存储生成列（12+）、`multirange` 类型（如
  `PostgresInt4MultirangeType`，14+）、`UNIQUE NULLS NOT DISTINCT` 与 `MERGE`（15+）。
  完整表格见[支持版本](../introduction/supported_versions.md)，本后端的版本相关类型位于
  `rhosocial.activerecord.backend.impl.postgres.expression.types`。

```python
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresInt4MultirangeType,
)

def create_archive(ctx):  # decorated with @migration in a real module
    # Archive has a PostgresInt4MultirangeType column (PG 14+); skip on older servers.
    if not ctx.backend.dialect.version_at_least((14, 0, 0)):
        return
    ctx.apply(Archive.create_table_statement(backend=ctx.backend))
```

请始终通过 `ctx.apply(...)` 发出 DDL，这样 dry run 与版本记录才能正确工作。

## 4. 函数式迁移（`run_migration_group`）

对于新工作，使用 `@migration` 函数与 `run_migration_group`，与核心章节所述完全一致——只有后端不同。
一个完整的 PostgreSQL 专用脚本见
[`docs/examples/migration/01_transactional_ddl.py`](../../examples/migration/01_transactional_ddl.py)：
它创建 `PostgresBackend`，用 `dry_run=True` 渲染迁移，用 `JSONFileMigrationRecordStore` 实际应用，
演示一个受版本门禁的类型，并展示失败时的 `transactional_ddl` 标记。

```bash
# from the repository root
PYTHONPATH=src python docs/examples/migration/01_transactional_ddl.py
```

成功运行需要可连接的 PostgreSQL 服务器；请用 `POSTGRES_*` 环境变量配置。顺序、依赖校验与
record-store 语义属于系统层面的内容——请看核心章节，而不是本页。

## 5. 类式 `named-migration` CLI 接线

类式（`NamedMigration`）API 对既有迁移提供完整支持。本后端的 CLI 暴露 `named-migration` 子命令，
其处理器位于
[`cli/named_migration.py`](../../../src/rhosocial/activerecord/backend/impl/postgres/cli/named_migration.py)，
并委托给共享的核心辅助函数。它根据连接参数构造 `PostgresBackend`（使用 `--async` 时为
`AsyncPostgresBackend`），并在运行前调用 `introspect_and_adapt()`。

```bash
python -m rhosocial.activerecord.backend.impl.postgres named-migration \
    myapp.migrations.v001.CreateUsersTable --host localhost --database mydb --direction up \
    --record-store ./migrations.json
python -m rhosocial.activerecord.backend.impl.postgres named-migration \
    myapp.migrations.v001.CreateUsersTable --database mydb --direction up --dry-run
python -m rhosocial.activerecord.backend.impl.postgres named-migration \
    myapp.migrations --all --record-store ./migrations.json --single-transaction
```

`--direction up|down`、`--record-store`、`--dry-run`、`--describe`、`--list`、`--param KEY=VALUE`、
`--all`、`--single-transaction` 与 `--async` 均受支持。`--single-transaction` 会把整批包装进一个事务——
由于 PostgreSQL 的 DDL 是事务性的，这能真正做到整批失败即全部回滚。完整的端到端演示（单个迁移、
依赖链、批量、异步、参数）位于
[`src/rhosocial/activerecord/backend/impl/postgres/examples/named_migrations/`](../../../src/rhosocial/activerecord/backend/impl/postgres/examples/named_migrations/)，
CLI 自身的选项见 [CLI README](../../../src/rhosocial/activerecord/backend/impl/postgres/cli/README.md)。

类式迁移也可以通过 `MigrationRunner` 以编程方式运行；见第二个示例
[`docs/examples/migration/02_named_migration.py`](../../examples/migration/02_named_migration.py)。

## 6. 下一步

- 迁移**系统**（权威指南）：
  [核心库迁移章节](https://github.com/rhosocial/python-activerecord/blob/main/docs/zh_CN/migration/README.md)
- 可运行示例：[本地迁移示例](../../examples/migration/README.md)
- PostgreSQL DDL 语句：[DDL 概览](../ddl/README.md)
- 各版本的服务器能力：[支持版本](../introduction/supported_versions.md)
- 事务行为：[事务支持](../transaction_support/README.md)
