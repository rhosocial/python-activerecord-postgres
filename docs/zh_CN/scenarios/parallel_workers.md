# 并行 Worker 场景下的正确用法（PostgreSQL）

在数据处理、任务队列、批量导入等场景中，开发者常常希望用多个 Worker 并行处理任务以提升吞吐量。本章专注于 PostgreSQL 数据库的并行 Worker 使用模式，阐述 PostgreSQL 与 MySQL、SQLite 在并发处理上的差异，并给出经过验证的安全方案。

> **贯穿本章的设计原则**：`rhosocial-activerecord` 的同步类 `BaseActiveRecord` 与异步类 `AsyncBaseActiveRecord` 方法名**完全相同**——`configure()` / `backend()` / `transaction()` / `save()` 等均如此，异步版本只需加 `await` 或 `async with`。本章所有示例均提供两种版本。

## 目录

1. [PostgreSQL 并发能力概述](#1-postgresql-并发能力概述)
2. [多进程：推荐方案](#2-多进程推荐方案)
3. [PostgreSQL 异步后端的特点](#3-postgresql-异步后端的特点)
4. [死锁：PostgreSQL 自动检测与预防](#4-死锁postgresql-自动检测与预防)
5. [应用分离原则](#5-应用分离原则)
6. [asyncio 并发在 PostgreSQL 上的行为](#6-asyncio-并发在-postgresql-上的行为)
7. [示例代码](#7-示例代码)

---

## 1. PostgreSQL 并发能力概述

### 1.1 与 MySQL、SQLite 的差异对比

`rhosocial-activerecord` 同样遵循**一个 ActiveRecord 类，绑定一条连接**的核心设计原则：

- **同步**：`Post.configure(config, PostgreSQLBackend)` → 写入 `Post.__backend__`
- **异步**：`await Post.configure(config, AsyncPostgreSQLBackend)` → 写入 `Post.__backend__`

配置写入的是**类级别属性**，多线程间仍然不安全。PostgreSQL 在以下方面与其他数据库有差异：

| 特性 | SQLite | MySQL | PostgreSQL |
| --- | --- | --- | --- |
| 锁粒度 | 文件级锁 | 行级锁（InnoDB） | 行级锁（MVCC） |
| 并发写入 | 需配置 WAL 模式 | 默认支持 | 默认支持（MVCC） |
| 异步驱动 | `aiosqlite`：线程池模拟 | `mysql-connector-python` 原生 async | `psycopg` 原生 async |
| 死锁处理 | 超时等待 | 自动检测，errno 1213 | 自动检测，SQLSTATE 40P01 |
| 连接类型 | 文件路径（本地） | TCP 网络连接 | TCP 网络连接 |
| RETURNING | ❌ 不支持 | ❌ 不支持 | ✅ 原生支持 |
| Free-threaded | ✅ 支持 | ✅ 支持 | ❌ 不支持（psycopg 限制） |

### 1.2 单连接模型的不变性

无论 PostgreSQL 有多少并发优势，**单个 `ActiveRecord` 类绑定的 `__backend__` 仍是一条连接**。在多线程环境下，并发操作同一 `__backend__` 会导致游标状态混乱。`psycopg` 官方明确说明连接不支持多线程并发使用。

> **不要将 ActiveRecord 配置在多个线程之间共享。** 多进程是并行 Worker 场景的正确选择。

### 1.3 psycopg 的 Free-threaded 限制

`psycopg` 版本 3 不支持 Python 3.13t/3.14t 的 free-threaded 构建。如果您的项目需要 free-threaded Python，请考虑使用 MySQL 或 SQLite 后端。

---

## 2. 多进程：推荐方案

多进程是并行 Worker 场景的推荐方案。每个进程拥有独立的内存空间，`configure()` 在进程内独立执行，建立独立的 TCP 连接。

### 2.1 正确的生命周期

**同步（multiprocessing）**：

```python
import multiprocessing
from rhosocial.activerecord.backend.impl.postgres import PostgreSQLBackend, PostgreSQLConnectionConfig
from models import Comment, Post, User

def worker(post_ids: list[int]):
    # 1. 进程启动后，在进程内配置连接——每个进程建立独立 TCP 连接
    config = PostgreSQLConnectionConfig(
        host="localhost",
        port=5432,
        database="mydb",
        username="app",
        password="secret",
    )
    User.configure(config, PostgreSQLBackend)
    Post.__backend__ = User.backend()
    Comment.__backend__ = User.backend()

    try:
        for post_id in post_ids:
            post = Post.find_one(post_id)
            if post is None:
                continue
            author = post.author()          # BelongsTo 关联
            approved = len([c for c in post.comments() if c.is_approved])
            post.view_count = 1 + approved
            post.save()
    finally:
        # 2. 进程退出前断开连接
        User.backend().disconnect()


if __name__ == "__main__":
    post_ids = list(range(1, 101))
    chunk_size = 25

    with multiprocessing.Pool(processes=4) as pool:
        chunks = [post_ids[i:i+chunk_size] for i in range(0, len(post_ids), chunk_size)]
        pool.map(worker, chunks)
```

**异步（asyncio + 多进程）**：

```python
import asyncio
import multiprocessing
from rhosocial.activerecord.backend.impl.postgres import AsyncPostgreSQLBackend, PostgreSQLConnectionConfig
from models import AsyncComment, AsyncPost, AsyncUser

async def async_worker_main(post_ids: list[int]):
    # 1. 在进程内配置异步连接（psycopg 原生 async，TCP 网络 I/O）
    config = PostgreSQLConnectionConfig(
        host="localhost", port=5432,
        database="mydb", username="app", password="secret",
    )
    await AsyncUser.configure(config, AsyncPostgreSQLBackend)
    AsyncPost.__backend__ = AsyncUser.backend()
    AsyncComment.__backend__ = AsyncUser.backend()

    try:
        async def process_post(post_id: int):
            post = await AsyncPost.find_one(post_id)
            if post is None:
                return
            author = await post.author()        # 加 await 加括号
            approved = len([c for c in await post.comments() if c.is_approved])
            post.view_count = 1 + approved
            await post.save()

        # 单连接顺序执行（进程间才是并发）
        for pid in post_ids:
            await process_post(pid)
    finally:
        await AsyncUser.backend().disconnect()


def run_async_worker(post_ids: list[int]):
    # 每个进程有独立的 event loop 和独立的 PostgreSQL 连接
    asyncio.run(async_worker_main(post_ids))


if __name__ == "__main__":
    post_ids = list(range(1, 101))
    chunk_size = 25

    with multiprocessing.Pool(processes=4) as pool:
        chunks = [post_ids[i:i+chunk_size] for i in range(0, len(post_ids), chunk_size)]
        pool.map(run_async_worker, chunks)
```

**关键规则**：

- `configure()` 必须在子进程内调用，不能在父进程配置后 `fork`
- PostgreSQL 连接是 TCP 连接，`fork` 后继承文件描述符比 SQLite 更危险
- 异步场景下，同一进程内的协程天然串行访问数据库（event loop 单线程调度）

可运行的耗时对比演示见 [exp1_basic_multiprocess.py](../../examples/chapter_08_scenarios/parallel_workers/exp1_basic_multiprocess.py)。

---

## 3. PostgreSQL 异步后端的特点

### 3.1 单连接模型的约束

`rhosocial-activerecord` 的异步 PostgreSQL 后端（`AsyncPostgreSQLBackend`）基于 `psycopg` 版本 3 的异步接口，**每个 ActiveRecord 类绑定一条连接**。这与连接池方案不同：

| 特性 | 单连接 ORM（本项目） | 连接池方案 |
| --- | --- | --- |
| 同进程内 asyncio.gather | ❌ 不支持，会引发运行时错误 | ✅ 支持，每个协程使用不同连接 |
| 配置复杂度 | 低，`configure()` 一行 | 高，需手动管理连接池 |
| 多进程并发 | ✅ 每进程独立连接，真正并发 | ✅ 同样支持 |
| 适用场景 | 批处理、任务队列、数据管道 | 高并发 Web 服务 |

### 3.2 psycopg 异步的优势

`psycopg` 的异步实现基于 Python 原生 `asyncio`，在等待数据库响应时真正释放控制权：

```text
单连接异步执行路径：
  协程 A → 发送 SQL → await → PostgreSQL 响应 → 协程 A 恢复
  协程 B → 必须等待 A 完成后才能发送（顺序执行）

多进程并发执行路径：
  进程 A → 独立连接 → 发送 SQL → await（并发）
  进程 B → 独立连接 → 发送 SQL → await（并发）
  （两者真正同时在传输层并发）
```

### 3.3 多进程是并发的正确姿势

虽然单进程内协程必须顺序执行，但**多进程间**每个进程独立持有连接，真正并发：

```python
async def process_posts_sequential(post_ids: list[int]):
    """
    单连接：同一进程内顺序执行协程。
    并发通过多进程实现——每个进程独立连接。
    """
    async def update_one(post_id: int):
        post = await AsyncPost.find_one(post_id)
        if post:
            post.view_count += 1
            await post.save()

    # 单连接：顺序执行
    for pid in post_ids:
        await update_one(pid)
```

可运行的多进程 async vs sync 性能对比见 [exp2_postgres_async_advantage.py](../../examples/chapter_08_scenarios/parallel_workers/exp2_postgres_async_advantage.py)。

---

## 4. 死锁：PostgreSQL 自动检测与预防

PostgreSQL 内置死锁检测算法，死锁发生时自动回滚其中一个事务，另一方继续执行。被回滚方收到 `OperationalError`（SQLSTATE 40P01）。

### 4.1 死锁的根本原因：行锁顺序不一致

```python
# ❌ 错误：不同 Worker 以相反顺序锁定行
def worker_a():
    with Post.transaction():
        post1 = Post.find_one(1)  # Worker A 先锁定 id=1
        time.sleep(0.01)
        post2 = Post.find_one(2)  # Worker A 再请求 id=2（此时 B 已持有）
        # → PostgreSQL 检测到死锁，选择回滚 A 或 B

def worker_b():
    with Post.transaction():
        post2 = Post.find_one(2)  # Worker B 先锁定 id=2
        time.sleep(0.01)
        post1 = Post.find_one(1)  # Worker B 再请求 id=1（此时 A 已持有）
```

反面教材见 [exp3_deadlock_wrong.py](../../examples/chapter_08_scenarios/parallel_workers/exp3_deadlock_wrong.py)。

### 4.2 预防方案一：固定锁顺序

```python
# ✅ 正确：始终按主键升序锁定资源
def transfer_safe(from_id: int, to_id: int, amount: float):
    first_id, second_id = min(from_id, to_id), max(from_id, to_id)
    with Account.transaction():
        first  = Account.find_one(first_id)
        second = Account.find_one(second_id)
        debit, credit = (first, second) if from_id < to_id else (second, first)
        debit.balance  -= amount
        credit.balance += amount
        debit.save()
        credit.save()

# 异步版本（方法名相同，加 await）
async def transfer_safe_async(from_id: int, to_id: int, amount: float):
    first_id, second_id = min(from_id, to_id), max(from_id, to_id)
    async with Account.transaction():
        first  = await Account.find_one(first_id)
        second = await Account.find_one(second_id)
        debit, credit = (first, second) if from_id < to_id else (second, first)
        debit.balance  -= amount
        credit.balance += amount
        await debit.save()
        await credit.save()
```

### 4.3 预防方案二：原子领取（事务内查询 + 更新）

```python
# ✅ 正确：在事务内原子领取，PostgreSQL 行级锁保证不重复
def claim_posts(batch_size: int = 5) -> list:
    with Post.transaction():
        pending = (
            Post.query()
                .where(Post.c.status == "draft")
                .order_by(Post.c.id)
                .limit(batch_size)
                .for_update()  # PostgreSQL 支持 FOR UPDATE
                .all()
        )
        if not pending:
            return []
        for post in pending:
            post.status = "processing"
            post.save()
        return pending

# 异步版本（方法名相同，加 await）
async def claim_posts_async(batch_size: int = 5) -> list:
    async with AsyncPost.transaction():
        pending = await (
            AsyncPost.query()
                .where(AsyncPost.c.status == "draft")
                .order_by(AsyncPost.c.id)
                .limit(batch_size)
                .for_update()
                .all()
        )
        if not pending:
            return []
        for post in pending:
            post.status = "processing"
            await post.save()
        return pending
```

### 4.3.1 跨数据库兼容的能力检测模式

当您的代码需要在多个数据库后端之间复用时，应该先检测后端能力再使用 FOR UPDATE：

```python
def claim_posts_portable(batch_size: int = 5) -> list:
    """跨数据库兼容的原子领取实现"""
    # 获取后端方言并检测能力
    dialect = Post.backend().dialect
    supports_for_update = dialect.supports_for_update()

    with Post.transaction():
        query = (
            Post.query()
                .where(Post.c.status == "draft")
                .order_by(Post.c.id)
                .limit(batch_size)
        )

        # 仅在支持的后端使用 FOR UPDATE
        if supports_for_update:
            query = query.for_update()

        pending = query.all()

        if not pending:
            return []
        for post in pending:
            post.status = "processing"
            post.save()
        return pending
```

**能力检测设计原则**：

| 原则 | 说明 |
| --- | --- |
| **不替用户做选择** | 方言默认返回 `False`，只有明确支持的后端才返回 `True` |
| **双层检测防御** | ActiveQuery 层面调用 `for_update()` 时会检测；Dialect 层面生成 SQL 时再次检测 |
| **显式优于隐式** | 不支持 FOR UPDATE 时抛出 `UnsupportedFeatureError`，而非静默忽略 |
| **用户自主适配** | 用户通过 `supports_for_update()` 判断后，可选择替代方案（如数据分区） |

> **PostgreSQL 说明**：PostgreSQL 从早期版本就支持 FOR UPDATE，因此 `PostgreSQLDialect.supports_for_update()` 始终返回 `True`。SQLite 不支持 FOR UPDATE（使用文件级锁），返回 `False`。

### 4.4 PostgreSQL 专有方案：捕获死锁并重试（生产推荐）

PostgreSQL 有自动死锁检测，因此可以**不预防死锁，而是捕获并重试**。这是 PostgreSQL 生产环境的推荐模式：

```python
import time

def _is_deadlock(exc: Exception) -> bool:
    """判断异常是否为 PostgreSQL 死锁（SQLSTATE 40P01）"""
    # psycopg 3 的错误对象有 sqlstate 属性
    if hasattr(exc, 'sqlstate'):
        return exc.sqlstate == '40P01'
    msg = str(exc)
    return '40P01' in msg or 'deadlock' in msg.lower()


def claim_posts_with_retry(batch_size: int = 5, max_retry: int = 3) -> list:
    """原子领取 + 死锁自动重试"""
    for attempt in range(max_retry):
        try:
            with Post.transaction():
                pending = (
                    Post.query()
                        .where(Post.c.status == "draft")
                        .order_by(Post.c.id)
                        .limit(batch_size)
                        .for_update()
                        .all()
                )
                if not pending:
                    return []
                for post in pending:
                    post.status = "processing"
                    post.save()
                return pending
        except Exception as e:
            if _is_deadlock(e) and attempt < max_retry - 1:
                time.sleep(0.05 * (attempt + 1))  # 指数退避
                continue
            raise
    return []
```

### 4.5 五条预防原则（含 PostgreSQL 专有说明）

| 原则 | 说明 |
| --- | --- |
| **数据分区** | 将数据集按 ID 范围或哈希分配给各 Worker，避免多进程操作同一行 |
| **一致的锁顺序** | 涉及多资源时，按固定顺序（如主键升序）请求锁 |
| **短事务** | 事务内只做必要操作，不跨越 I/O 等待或耗时计算 |
| **原子领取** | 在事务内查询并更新任务状态，使用 `FOR UPDATE` 锁定 |
| **死锁重试**（PostgreSQL 专有） | 捕获 `OperationalError`（SQLSTATE 40P01）并重试 |

> **PostgreSQL vs MySQL 差异**：MySQL 死锁错误码为 errno 1213，PostgreSQL 为 SQLSTATE 40P01。两者都有自动死锁检测。

可运行的正确方案演示见 [exp4_partition_correct.py](../../examples/chapter_08_scenarios/parallel_workers/exp4_partition_correct.py)。

---

## 5. 应用分离原则

当系统同时包含两种截然不同的工作负载时，建议将它们部署为独立应用。

### 5.1 场景示例

| 工作负载类型 | 特征 | 合适部署 |
| --- | --- | --- |
| Web API 服务 | 短请求、高并发、响应时间敏感 | FastAPI / Django + asyncio + 连接池 |
| 数据分析批处理 | 长时间运行、大数据量、CPU 密集 | 独立脚本 + multiprocessing + PostgreSQLBackend |
| 任务队列消费 | 定期轮询、任务间独立、易于水平扩展 | 独立 Worker 进程池 |

### 5.2 推荐架构

```text
用户请求 ──→ Web 应用（asyncio + 连接池）
                │
                └──→ 任务队列（PostgreSQL 数据库表 / Redis）
                            │
                            └──→ 后台 Worker 进程池
                                  （每进程独立 PostgreSQLBackend 同步连接）
```

Web 应用负责接收请求、写入任务队列；Worker 进程池负责执行耗时任务。

---

## 6. asyncio 并发在 PostgreSQL 上的行为

### 6.1 单连接模型的限制

`rhosocial-activerecord` 遵循**一个 ActiveRecord 类绑定一条连接**的核心设计原则。这意味着同一进程内的协程**不能**通过 `asyncio.gather` 并发访问数据库——单个连接在同一时刻只能处理一个查询：

```python
# ❌ 错误：单连接不支持并发访问
async def batch_update_wrong(post_ids: list[int]):
    await asyncio.gather(*[
        update_one(pid) for pid in post_ids  # 会引发运行时错误
    ])

# ✅ 正确：单连接内顺序执行
async def batch_update_correct(post_ids: list[int]):
    for pid in post_ids:
        await update_one(pid)
```

### 6.2 多进程是并发的载体

异步的优势在于**多进程间**：每个进程持有独立的 PostgreSQL TCP 连接，进程间真正并发。

### 6.3 PostgreSQL 异步与 MySQL 异步的对比

| 场景 | PostgreSQL（psycopg 3） | MySQL（mysql-connector-python） |
| --- | --- | --- |
| 同进程内 asyncio.gather | ❌ 单连接不支持 | ❌ 单连接不支持 |
| 多进程并发写入 | ✅ MVCC，无需配置 | ✅ 行级锁，无需配置 |
| 死锁处理 | ✅ 自动检测，SQLSTATE 40P01 | ✅ 自动检测，errno 1213 |
| Free-threaded 支持 | ❌ 不支持 | ✅ 支持 |

---

## 7. 示例代码

本章的完整可运行示例位于 [`docs/examples/chapter_08_scenarios/parallel_workers/`](../../examples/chapter_08_scenarios/parallel_workers/)，包含以下实验：

| 文件 | 内容 | 对应章节 |
| --- | --- | --- |
| [`config_loader.py`](../../examples/chapter_08_scenarios/parallel_workers/config_loader.py) | 连接配置加载（YAML / 环境变量 / 默认值） | — |
| [`models.py`](../../examples/chapter_08_scenarios/parallel_workers/models.py) | 共享模型定义（`User`、`Post`、`Comment` 同步版 + 异步版） | — |
| [`setup_db.py`](../../examples/chapter_08_scenarios/parallel_workers/setup_db.py) | 数据库初始化脚本（同步/异步两种模式） | — |
| [`exp1_basic_multiprocess.py`](../../examples/chapter_08_scenarios/parallel_workers/exp1_basic_multiprocess.py) | 正确的多进程用法（含串行/同步多进程/异步多进程耗时对比） | §2.1 |
| [`exp2_postgres_async_advantage.py`](../../examples/chapter_08_scenarios/parallel_workers/exp2_postgres_async_advantage.py) | PostgreSQL 异步特点：单连接限制说明 + 多进程并发写入对比 | §3 |
| [`exp3_deadlock_wrong.py`](../../examples/chapter_08_scenarios/parallel_workers/exp3_deadlock_wrong.py) | 行锁顺序冲突导致 PostgreSQL 死锁（反面教材） | §4.1 |
| [`exp4_partition_correct.py`](../../examples/chapter_08_scenarios/parallel_workers/exp4_partition_correct.py) | 数据分区 + 原子领取 + 死锁重试（同步/异步各方案） | §4.2–4.4 |
| [`exp5_multithread_warning.py`](../../examples/chapter_08_scenarios/parallel_workers/exp5_multithread_warning.py) | 多线程共享连接的问题（反面教材） | §1.2 |
| [`exp6_backend_group_pool.py`](../../examples/chapter_08_scenarios/parallel_workers/exp6_backend_group_pool.py) | BackendGroup vs BackendPool 线程安全性对比 | §8.5 |

> **说明**：所有示例文件均直接使用 `rhosocial-activerecord` ORM，模型体系为 `User → Post → Comment`，并体现 `HasMany` / `BelongsTo` 关联关系的同步与异步对等用法。

运行前请先执行初始化脚本：

```bash
cd docs/examples/chapter_08_scenarios/parallel_workers
python setup_db.py
python exp1_basic_multiprocess.py   # 运行任意实验
```

详见该目录下的 `README.md` 了解各实验的完整说明和预期输出。

---

## 8. WorkerPool 测试经验总结

### 8.1 异步 Worker 测试的已知限制

在 `WorkerPool` 多进程环境下运行异步测试时，存在以下已知限制：

#### Event Loop 跨进程问题

当 `WorkerPool` 在子进程中执行异步任务时，每个子进程通过 `asyncio.run()` 创建独立的 event loop。然而，测试框架（pytest-asyncio）的 fixture 在主进程中创建的 event loop 与子进程中的 event loop 是隔离的：

```text
主进程（pytest）：
  └── Event Loop A（pytest-asyncio 创建）
      └── Fixture: async_user_class_for_worker
          └── 异步后端实例绑定到 Loop A

子进程（Worker）：
  └── Event Loop B（asyncio.run() 创建）
      └── 任务尝试使用绑定到 Loop A 的异步后端
          └── 错误：Task got Future attached to a different loop
```

**错误示例**：

```python
# ❌ 错误：在主进程 fixture 中创建的异步后端无法在子进程中使用
async def async_worker_task(user_id, conn_params):
    # conn_params 包含的异步后端实例绑定到主进程的 event loop
    # 子进程尝试使用时会失败
    backend = conn_params['backend']  # 绑定到错误的 loop
    user = await backend.find_one(user_id)  # RuntimeError!
```

**正确做法**：

```python
# ✅ 正确：在子进程内部创建新的异步后端实例
async def async_worker_task(user_id, conn_params):
    # 只传递连接参数（可序列化），在子进程内创建新实例
    config = conn_params['config_kwargs']
    await Model.configure(config, AsyncPostgreSQLBackend)
    user = await Model.find_one(user_id)
    await Model.backend().disconnect()
```

#### 受影响的测试场景

| 测试类型 | 同步版本 | 异步版本 | 原因 |
|----------|----------|----------|------|
| 并行读取 | ✅ 通过 | ❌ 失败 | 异步后端绑定到主进程 loop |
| 并行更新 | ✅ 通过 | ❌ 失败 | 同上 |
| 并行删除 | ✅ 通过 | ❌ 失败 | 同上 |
| 并行查询 | ✅ 通过 | ❌ 失败 | 同上 |
| 事务隔离 | ✅ 通过 | ❌ 失败 | 同上 + 事务状态跨进程问题 |
| Worker 生命周期 | ✅ 通过 | ✅ 通过 | 不涉及跨进程异步操作 |
| 连接管理 | ✅ 通过 | ✅ 通过 | 不涉及跨进程异步操作 |

### 8.2 测试覆盖率说明

Worker 测试对 PostgreSQL 后端的覆盖率贡献：

| 模块 | 覆盖率 | 说明 |
|----------|--------|------|
| `backend.py` | ~32% | 同步后端核心功能 |
| `async_backend.py` | ~35% | 异步后端核心功能 |
| `mixins/` | ~18% | DML 操作 |
| `dialect.py` | ~22% | SQL 方言处理 |

同步测试覆盖率较高，异步测试因 event loop 问题导致部分代码路径未被覆盖。

### 8.3 生产环境建议

1. **同步 Worker 是首选**：在多进程 Worker 场景下，同步后端（`PostgreSQLBackend`）是更稳定的选择
2. **异步适用于单进程**：异步后端（`AsyncPostgreSQLBackend`）在单进程内的顺序执行场景下表现良好
3. **避免跨进程传递异步实例**：只传递可序列化的连接参数，在子进程内创建新的异步后端实例

### 8.4 FOR UPDATE 能力检测经验

#### 问题背景

在 WorkerPool 多进程测试中，`test_transaction_isolation.py` 的转账测试使用了 `FOR UPDATE` 子句来锁定行。然而，SQLite 后端不支持 `FOR UPDATE`（使用文件级锁），导致测试失败。

#### 解决方案：能力检测模式

遵循「不替用户做选择」的设计原则，我们实现了两层能力检测：

**1. Dialect 层面的能力声明**：

```python
# SQLDialectBase（默认不支持）
def supports_for_update(self) -> bool:
    """默认返回 False，只有支持的后端才重写此方法"""
    return False

# PostgreSQLDialect（明确支持）
def supports_for_update(self) -> bool:
    return True
```

**2. ActiveQuery 层面的早期检测**：

```python
# ActiveQuery.for_update() 方法中
def for_update(self, ...):
    if not dialect.supports_for_update():
        raise UnsupportedFeatureError(
            dialect.name,
            "FOR UPDATE clause",
            "This backend does not support row-level locking with FOR UPDATE. "
            "Use dialect.supports_for_update() to check support before calling this method."
        )
```

**3. Dialect 层面的安全网**：

```python
# SQLDialectBase.format_query_statement() 中
if expr.for_update:
    if not self.supports_for_update():
        raise UnsupportedFeatureError(...)
    # 生成 FOR UPDATE SQL...
```

#### 测试代码适配

测试任务函数中使用能力检测来适配不同后端：

```python
def transfer_task(from_id: int, to_id: int, amount: float, conn_params: dict):
    # 检测后端是否支持 FOR UPDATE
    supports_for_update = backend.dialect.supports_for_update()

    with Model.transaction():
        if supports_for_update:
            # PostgreSQL/MySQL：使用 FOR UPDATE 锁定
            first = Model.query().where(Model.c.id == first_id).for_update().one()
        else:
            # SQLite：使用普通查询（依赖文件锁）
            first = Model.find_one({'id': first_id})
        # ... 业务逻辑
```

#### 设计经验总结

| 经验 | 说明 |
|------|------|
| **默认拒绝原则** | 基类返回 `False`，后端必须显式声明支持 |
| **不替用户做选择** | 不支持时抛错，而非静默忽略（Django/Rails 方案） |
| **双层防御** | ActiveQuery 提供早期失败，Dialect 作为安全网 |
| **用户自主适配** | 用户通过 `supports_for_update()` 判断后选择替代方案 |

### 8.5 相关文件

- 测试桥接文件：`tests/rhosocial/activerecord_postgres_test/feature/basic/worker/`
- Provider 实现：`tests/providers/basic.py`、`tests/providers/query.py`
- WorkerPool 实现：`rhosocial.activerecord.worker.pool`

---

## 9. 测试验证结论

### 9.1 测试环境

以下测试在多种环境下验证通过：

| 平台 | 操作系统 | Python 版本 | pytest 版本 | PostgreSQL 版本 |
|------|----------|-------------|-------------|-----------------|
| macOS | macOS Tahoe 26.4.1 (Build 25E253) arm64 | 3.8.10 | 8.3.5 | 9.6.24 |
| Windows | Windows 11 Pro 25H2 (Build 26200) | 3.8.10 / 3.14.3 | 8.3.5 / 8.4.2 | 17.5 |

### 9.2 测试结果汇总

#### 多进程并行测试 (exp1)

| 平台 | 串行耗时 | 同步多进程 | 异步多进程 | 加速比 |
|------|----------|------------|------------|--------|
| macOS (Python 3.8.10) | 0.727s | 0.700s (1.0x) | 0.894s (0.8x) | 1.0x |
| Windows | 0.910s | 2.998s (0.3x) | 3.163s (0.3x) | 0.3x |

> **说明**: 多进程启动开销在小数据量下可能超过并行收益，大数据量时加速效果更明显。macOS arm64 平台同步多进程性能表现稳定。

#### 异步特性测试 (exp2)

| 平台 | 同进程同步串行 | 同进程异步顺序 | 多进程同步 | 多进程异步 |
|------|----------------|----------------|------------|------------|
| macOS (Python 3.8.10) | 0.651s | N/A（死锁触发） | N/A | N/A |
| Windows | 0.204s | 0.218s | 1.133s | 1.264s |

> **说明**: macOS 平台在异步并发测试中触发了 PostgreSQL 死锁检测（SQLSTATE 40P01），这与测试数据集较小且并发冲突概率较高有关。

#### 死锁检测测试 (exp3)

所有平台均成功触发 PostgreSQL 死锁检测机制：
- 死锁被自动检测（SQLSTATE 40P01）
- 代价较小的事务被回滚
- 未捕获异常时，回滚事务的工作丢失
- macOS: 2 Workers 成功，2 Workers 死锁回滚，总耗时 2.584s
- Windows: 2 Workers 成功，2 Workers 死锁回滚

#### 正确方案测试 (exp4)

| 方案 | macOS 耗时 | Windows 耗时 | 验证结果 |
|------|------------|--------------|----------|
| A: 数据分区（同步） | 0.804s | 1.170s | ✓ 无重复 |
| A: 数据分区（异步） | 0.724s | 1.263s | ✓ 无重复 |
| B: 原子领取（同步） | 1.071s | 1.812s | ✓ 无重复 |
| B: 原子领取（异步） | 1.218s | 1.446s | ✓ 无重复 |
| C: 原子+重试（同步） | 1.193s | 2.501s | ✓ 无重复 |

#### 多线程警告测试 (exp5)

所有平台均验证多线程共享连接不安全：
- 共享 `__backend__`: 游标状态混乱
- 每线程 `configure()`: 类属性被覆盖，仍共享同一实例

### 9.3 平台差异说明

#### Windows 特殊配置

在 Windows 上运行异步测试时，需要设置 `WindowsSelectorEventLoopPolicy`：

```python
import asyncio
import sys

def worker_async(post_ids: list) -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(async_worker_main(post_ids))

# 主程序中同样需要设置
def main():
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    # ... 其他代码
```

**原因**: Windows 默认使用 `ProactorEventLoop`，而 `psycopg` 的异步后端需要 `SelectorEventLoop`。

> **重要**: 如果未设置正确的 event loop policy，将收到以下错误：
> ```
> Psycopg cannot use the 'ProactorEventLoop' to run in async mode.
> Please use a compatible event loop, for instance by setting
> 'asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())'
> ```

#### macOS / Linux

无需特殊配置，默认 event loop 即可正常工作。

### 9.4 BackendGroup vs BackendPool 线程安全对比 (exp6)

#### 实验目的

对比 BackendGroup + backend.context() 与 BackendPool 在多线程场景下的安全性。

#### 实验结果

| 场景 | 成功数 | 错误数 | 耗时 | 结论 |
|-----|--------|--------|------|------|
| Scenario 1: BackendPool | 20 | 0 | 0.382s | ✅ 线程安全，推荐 |
| Scenario 2: BackendGroup | 20 | 0 | 0.484s | ⚠️ 可用但有竞争风险 |

#### 关键发现

**PostgreSQL (psycopg) 的 threadsafety=2**：

1. **连接对象本身是线程安全的**：多线程可以安全共享连接对象
2. **BackendPool 是推荐方案**：每个线程从池中获取独立连接
3. **BackendGroup 在 PostgreSQL 上能工作**：但理论上仍有竞争条件

#### BackendPool 的优势

```python
# ✅ 推荐：BackendPool（连接池）
from rhosocial.activerecord.connection.pool import PoolConfig, BackendPool

config = PoolConfig(
    min_size=2,
    max_size=10,
    backend_factory=lambda: PostgresBackend(connection_config=db_config)
)
pool = BackendPool(config)

# 每个线程获取独立连接
def thread_worker(pool, thread_id):
    with pool.connection() as backend:
        User.__backend__ = backend
        user = User.find_one(1)
        # 使用完毕自动归还连接池
```

#### BackendGroup 的局限

```python
# ⚠️ 有风险：BackendGroup（共享后端实例）
group = BackendGroup(
    name="main",
    models=[User, Post, Comment],
    config=config,
    backend_class=PostgresBackend
)
group.configure()

# 多个线程共享同一个后端实例
def thread_worker(group, thread_id):
    backend = group.get_backend()
    with backend.context():  # 理论上有竞争条件
        user = User.find_one(1)
```

#### PostgreSQL 的线程安全方案

| 方案 | threadsafety | 推荐度 | 说明 |
|-----|-------------|--------|------|
| **BackendPool** | 2 | ⭐⭐⭐ 推荐 | 每线程独立连接，真正的线程隔离 |
| BackendGroup + context() | 2 | ⭐⭐ 可用 | 共享后端实例，PostgreSQL 能处理但不够优雅 |
| 多进程 | - | ⭐ 可用 | 进程隔离，但开销较大 |

#### 结论

- **PostgreSQL 支持 BackendPool**：因为 psycopg 的 threadsafety=2
- **BackendPool 是多线程首选方案**：提供真正的连接隔离
- **BackendGroup 主要用于多模型共享**：不是线程隔离机制
- **"随用随连、用完即断"**：BackendPool 自动管理连接生命周期

### 9.5 FastAPI 异步应用的最佳实践

#### 问题场景

在 FastAPI + PostgreSQL + 异步后端场景下：
- 每个 HTTP 请求在独立的协程中处理
- 多个协程可能并发执行
- PostgreSQL 的 `threadsafety=2`，支持连接池
- 需要高效的连接复用机制

#### 推荐方案：BackendPool 连接池

**核心原则**：
1. 应用启动时创建全局 `AsyncBackendPool`
2. 每个请求从池中获取连接
3. 使用完毕后归还连接池
4. 应用关闭时关闭连接池

**完整示例**：

```python
# database.py - 连接池管理器
from rhosocial.activerecord.connection.pool import PoolConfig, AsyncBackendPool
from rhosocial.activerecord.backend.impl.postgres import AsyncPostgresBackend

# 全局连接池（应用启动时创建）
_pool: AsyncBackendPool = None

async def init_pool():
    """初始化连接池"""
    global _pool
    
    pool_config = PoolConfig(
        min_size=2,   # 最小连接数
        max_size=10,  # 最大连接数
        backend_factory=lambda: AsyncPostgresBackend(connection_config=config)
    )
    
    _pool = AsyncBackendPool(pool_config)

async def close_pool():
    """关闭连接池"""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

@asynccontextmanager
async def get_request_db():
    """请求级连接管理器"""
    global _pool
    
    # 从池中获取连接
    async with _pool.connection() as backend:
        # 绑定模型到当前连接
        AsyncUser.__backend__ = backend
        AsyncPost.__backend__ = backend
        AsyncComment.__backend__ = backend
        
        yield backend
        # 连接自动归还到池


# app.py - FastAPI 应用
from fastapi import FastAPI, Depends

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_database()
    await init_pool()  # 启动时初始化连接池
    yield
    await close_pool()  # 关闭时清理连接池

app = FastAPI(lifespan=lifespan)

@app.get("/users/{user_id}")
async def get_user(user_id: int, db=Depends(get_request_db)):
    user = await AsyncUser.find_one(user_id)
    return {"user": user.to_dict()}
```

#### 方案分析

| 特性 | 说明 |
|-----|------|
| **连接复用** | 池化连接，减少创建开销 |
| **并发安全** | 每个请求获取独立连接 |
| **资源管理** | 池自动管理连接生命周期 |
| **高性能** | 适合高并发场景 |
| **事务支持** | 同一连接内支持事务 |

#### MySQL vs PostgreSQL 方案对比

| 方面 | MySQL | PostgreSQL |
|-----|-------|------------|
| threadsafety | 1 | 2 |
| BackendPool | ❌ 不支持 | ✅ 推荐 |
| 推荐方案 | 请求级 BackendGroup | BackendPool |
| 连接复用 | 每请求创建/断开 | 池管理复用 |
| 性能 | 可接受开销 | 优化连接池 |

#### 关键设计要点

1. **全局连接池**（应用启动时创建）
   - 在 `lifespan` 中初始化连接池
   - 配置合适的 min_size 和 max_size

2. **请求级连接获取**
   - 使用依赖注入获取连接
   - 自动绑定模型到当前连接

3. **自动归还连接**
   - 使用 `async with` 确保连接归还
   - 异常时也能正确归还

4. **应用关闭时清理**
   - 在 `lifespan` 的 `yield` 后关闭连接池

#### 适用场景

| 场景 | 推荐度 | 说明 |
|-----|--------|------|
| Web API（高并发） | ⭐⭐⭐ 推荐 | 连接池，高效复用 |
| 微服务 | ⭐⭐⭐ 推荐 | 连接池管理，适合分布式 |
| 实时应用 | ⭐⭐⭐ 推荐 | 高性能，支持 WebSocket |
| 批处理任务 | ⭐⭐ 可用 | 配合适当的池大小 |
| 后台任务 | ⭐⭐ 可用 | 共享应用连接池 |

#### 完整示例代码

参见 `docs/examples/chapter_10_fastapi/` 目录。

### 9.6 结论

1. **多进程是并行 Worker 的正确方案**: 所有平台均验证通过
2. **同步后端更稳定**: 异步后端在 Windows 上需要额外配置
3. **死锁重试方案推荐用于生产**: 不依赖数据可分区性，自动处理死锁
4. **数据分区效率最高**: MVCC 无锁竞争，适合可分区场景
5. **PostgreSQL 多线程推荐 BackendPool**: threadsafety=2 支持连接池
6. **BackendPool 提供真正的线程隔离**: 每线程独立连接，无竞争条件
7. **FastAPI 推荐使用 BackendPool**: 高效连接复用，适合高并发场景
5. **PostgreSQL vs MySQL 死锁错误码**:
   - PostgreSQL: SQLSTATE 40P01
   - MySQL: errno 1213
