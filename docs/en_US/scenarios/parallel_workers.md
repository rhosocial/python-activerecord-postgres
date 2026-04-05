# Correct Usage in Parallel Worker Scenarios (PostgreSQL)

In scenarios such as data processing, task queues, and batch imports, developers often want to use multiple workers to process tasks in parallel to improve throughput. This chapter focuses on parallel worker usage patterns for PostgreSQL databases, explains the fundamental differences between PostgreSQL, MySQL, and SQLite in concurrent processing, and provides verified safe solutions.

> **Design Principle Throughout This Chapter**: The synchronous class `BaseActiveRecord` and asynchronous class `AsyncBaseActiveRecord` in `rhosocial-activerecord` have **identical method names** - `configure()` / `backend()` / `transaction()` / `save()` etc. are all the same, the async version just needs `await` or `async with`. All examples in this chapter provide both versions.

## Table of Contents

1. [PostgreSQL Concurrency Overview](#1-postgresql-concurrency-overview)
2. [Multi-process: Recommended Approach](#2-multi-process-recommended-approach)
3. [PostgreSQL Async Backend Characteristics](#3-postgresql-async-backend-characteristics)
4. [Deadlock: PostgreSQL Automatic Detection and Prevention](#4-deadlock-postgresql-automatic-detection-and-prevention)
5. [Application Separation Principle](#5-application-separation-principle)
6. [asyncio Concurrency Behavior on PostgreSQL](#6-asyncio-concurrency-behavior-on-postgresql)
7. [Example Code](#7-example-code)

---

## 1. PostgreSQL Concurrency Overview

### 1.1 Differences Compared to MySQL and SQLite

`rhosocial-activerecord` follows the core design principle of **one ActiveRecord class bound to one connection**:

- **Sync**: `Post.configure(config, PostgreSQLBackend)` → writes to `Post.__backend__`
- **Async**: `await Post.configure(config, AsyncPostgreSQLBackend)` → writes to `Post.__backend__`

The configuration writes to a **class-level attribute**, which is still unsafe for multi-threading. PostgreSQL differs from other databases in the following ways:

| Feature | SQLite | MySQL | PostgreSQL |
| --- | --- | --- | --- |
| Lock Granularity | File-level lock | Row-level lock (InnoDB) | Row-level lock (MVCC) |
| Concurrent Writes | Requires WAL mode | Supported by default | Supported by default (MVCC) |
| Async Driver | `aiosqlite`: thread pool simulation | `mysql-connector-python` native async | `psycopg` native async |
| Deadlock Handling | Timeout wait | Auto detection, errno 1213 | Auto detection, SQLSTATE 40P01 |
| Connection Type | File path (local) | TCP network connection | TCP network connection |
| RETURNING | ❌ Not supported | ❌ Not supported | ✅ Natively supported |
| Free-threaded | ✅ Supported | ✅ Supported | ❌ Not supported (psycopg limitation) |

### 1.2 Immutability of Single Connection Model

Regardless of PostgreSQL's concurrency advantages, **the `__backend__` bound to a single `ActiveRecord` class is still one connection**. In multi-threaded environments, concurrent operations on the same `__backend__` will cause cursor state corruption. The `psycopg` documentation explicitly states that connections do not support multi-threaded concurrent use.

> **Do not share ActiveRecord configuration across multiple threads.** Multi-processing is the correct choice for parallel worker scenarios.

### 1.3 psycopg Free-threaded Limitation

`psycopg` version 3 does not support free-threaded builds of Python 3.13t/3.14t. If your project requires free-threaded Python, please consider using the MySQL or SQLite backend.

---

## 2. Multi-process: Recommended Approach

Multi-processing is the recommended approach for parallel worker scenarios. Each process has its own memory space, `configure()` executes independently within the process, establishing independent TCP connections.

### 2.1 Correct Lifecycle

**Sync (multiprocessing)**:

```python
import multiprocessing
from rhosocial.activerecord.backend.impl.postgres import PostgreSQLBackend, PostgreSQLConnectionConfig
from models import Comment, Post, User

def worker(post_ids: list[int]):
    # 1. After process starts, configure connection within process - each process establishes independent TCP connection
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
            author = post.author()          # BelongsTo association
            approved = len([c for c in post.comments() if c.is_approved])
            post.view_count = 1 + approved
            post.save()
    finally:
        # 2. Disconnect before process exits
        User.backend().disconnect()


if __name__ == "__main__":
    post_ids = list(range(1, 101))
    chunk_size = 25

    with multiprocessing.Pool(processes=4) as pool:
        chunks = [post_ids[i:i+chunk_size] for i in range(0, len(post_ids), chunk_size)]
        pool.map(worker, chunks)
```

**Async (asyncio + multi-process)**:

```python
import asyncio
import multiprocessing
from rhosocial.activerecord.backend.impl.postgres import AsyncPostgreSQLBackend, PostgreSQLConnectionConfig
from models import AsyncComment, AsyncPost, AsyncUser

async def async_worker_main(post_ids: list[int]):
    # 1. Configure async connection within process (psycopg native async, TCP network I/O)
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
            author = await post.author()        # Add await and parentheses
            approved = len([c for c in await post.comments() if c.is_approved])
            post.view_count = 1 + approved
            await post.save()

        # Single connection sequential execution (concurrency is between processes)
        for pid in post_ids:
            await process_post(pid)
    finally:
        await AsyncUser.backend().disconnect()


def run_async_worker(post_ids: list[int]):
    # Each process has its own event loop and independent PostgreSQL connection
    asyncio.run(async_worker_main(post_ids))


if __name__ == "__main__":
    post_ids = list(range(1, 101))
    chunk_size = 25

    with multiprocessing.Pool(processes=4) as pool:
        chunks = [post_ids[i:i+chunk_size] for i in range(0, len(post_ids), chunk_size)]
        pool.map(run_async_worker, chunks)
```

**Key Rules**:

- `configure()` must be called within child processes, not configured in parent process before `fork`
- PostgreSQL connections are TCP connections, inheriting file descriptors after `fork` is more dangerous than SQLite
- In async scenarios, coroutines within the same process naturally access the database sequentially (event loop single-threaded scheduling)

Runnable timing comparison demo at [exp1_basic_multiprocess.py](../../examples/chapter_08_scenarios/parallel_workers/exp1_basic_multiprocess.py).

---

## 3. PostgreSQL Async Backend Characteristics

### 3.1 Single Connection Model Constraints

`rhosocial-activerecord`'s async PostgreSQL backend (`AsyncPostgreSQLBackend`) is based on `psycopg` version 3's async interface, **each ActiveRecord class binds to one connection**. This differs from connection pool solutions:

| Feature | Single Connection ORM (This Project) | Connection Pool Solution |
| --- | --- | --- |
| In-process asyncio.gather | ❌ Not supported, raises runtime error | ✅ Supported, each coroutine uses different connection |
| Configuration Complexity | Low, one line `configure()` | High, manual connection pool management |
| Multi-process Concurrency | ✅ Each process has independent connection, true concurrency | ✅ Also supported |
| Applicable Scenarios | Batch processing, task queues, data pipelines | High-concurrency web services |

### 3.2 psycopg Async Advantages

`psycopg`'s async implementation is based on Python's native `asyncio`, truly releasing control while waiting for database responses:

```text
Single connection async execution path:
  Coroutine A → Send SQL → await → PostgreSQL response → Coroutine A resumes
  Coroutine B → Must wait for A to complete before sending (sequential execution)

Multi-process concurrent execution path:
  Process A → Independent connection → Send SQL → await (concurrent)
  Process B → Independent connection → Send SQL → await (concurrent)
  (Both truly concurrent at transport layer)
```

### 3.3 Multi-process is the Correct Approach for Concurrency

Although coroutines within a single process must execute sequentially, **between multiple processes** each process independently holds a connection, achieving true concurrency.

Runnable multi-process async vs sync performance comparison at [exp2_postgres_async_advantage.py](../../examples/chapter_08_scenarios/parallel_workers/exp2_postgres_async_advantage.py).

---

## 4. Deadlock: PostgreSQL Automatic Detection and Prevention

PostgreSQL has a built-in deadlock detection algorithm. When deadlock occurs, it automatically rolls back one of the transactions, allowing the other to continue. The rolled-back party receives an `OperationalError` (SQLSTATE 40P01).

### 4.1 Root Cause of Deadlock: Inconsistent Row Lock Order

```python
# ❌ Wrong: Different workers lock rows in opposite order
def worker_a():
    with Post.transaction():
        post1 = Post.find_one(1)  # Worker A locks id=1 first
        time.sleep(0.01)
        post2 = Post.find_one(2)  # Worker A requests id=2 (held by B)
        # → PostgreSQL detects deadlock, chooses to rollback A or B

def worker_b():
    with Post.transaction():
        post2 = Post.find_one(2)  # Worker B locks id=2 first
        time.sleep(0.01)
        post1 = Post.find_one(1)  # Worker B requests id=1 (held by A)
```

Anti-pattern example at [exp3_deadlock_wrong.py](../../examples/chapter_08_scenarios/parallel_workers/exp3_deadlock_wrong.py).

### 4.2 Prevention Method 1: Fixed Lock Order

```python
# ✅ Correct: Always lock resources in primary key ascending order
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

# Async version (same method names, add await)
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

### 4.3 Prevention Method 2: Atomic Claim (Query + Update in Transaction)

```python
# ✅ Correct: Atomic claim within transaction, PostgreSQL row-level lock guarantees no duplicates
def claim_posts(batch_size: int = 5) -> list:
    with Post.transaction():
        pending = (
            Post.query()
                .where(Post.c.status == "draft")
                .order_by(Post.c.id)
                .limit(batch_size)
                .for_update()  # PostgreSQL supports FOR UPDATE
                .all()
        )
        if not pending:
            return []
        for post in pending:
            post.status = "processing"
            post.save()
        return pending

# Async version (same method names, add await)
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

### 4.3.1 Cross-Database Compatible Capability Detection Pattern

When your code needs to be reused across multiple database backends, you should check backend capabilities before using FOR UPDATE:

```python
def claim_posts_portable(batch_size: int = 5) -> list:
    """Cross-database compatible atomic claim implementation"""
    # Get backend dialect and check capability
    dialect = Post.backend().dialect
    supports_for_update = dialect.supports_for_update()

    with Post.transaction():
        query = (
            Post.query()
                .where(Post.c.status == "draft")
                .order_by(Post.c.id)
                .limit(batch_size)
        )

        # Only use FOR UPDATE on supported backends
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

**Capability Detection Design Principles**:

| Principle | Description |
| --- | --- |
| **Don't make choices for users** | Dialect returns `False` by default; only backends that explicitly support it return `True` |
| **Dual-layer defense** | ActiveQuery layer detects when `for_update()` is called; Dialect layer checks again as safety net |
| **Explicit over implicit** | Raises `UnsupportedFeatureError` when unsupported, rather than silently ignoring |
| **User adapts** | Users choose alternative approaches after checking `supports_for_update()` |

> **PostgreSQL Note**: PostgreSQL has supported FOR UPDATE since early versions, so `PostgreSQLDialect.supports_for_update()` always returns `True`. SQLite does not support FOR UPDATE (uses file-level locks) and returns `False`.

### 4.4 PostgreSQL-specific Method: Catch Deadlock and Retry (Production Recommended)

PostgreSQL has automatic deadlock detection, so you can **not prevent deadlock, but catch and retry**. This is the recommended pattern for PostgreSQL production environments:

```python
import time

def _is_deadlock(exc: Exception) -> bool:
    """Check if exception is PostgreSQL deadlock (SQLSTATE 40P01)"""
    # psycopg 3 error objects have sqlstate attribute
    if hasattr(exc, 'sqlstate'):
        return exc.sqlstate == '40P01'
    msg = str(exc)
    return '40P01' in msg or 'deadlock' in msg.lower()


def claim_posts_with_retry(batch_size: int = 5, max_retry: int = 3) -> list:
    """Atomic claim + automatic deadlock retry"""
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
                time.sleep(0.05 * (attempt + 1))  # Exponential backoff
                continue
            raise
    return []
```

### 4.5 Five Prevention Principles (PostgreSQL-specific Notes)

| Principle | Description |
| --- | --- |
| **Data Partitioning** | Distribute data set by ID range or hash to workers, avoid multiple processes operating on same row |
| **Consistent Lock Order** | When involving multiple resources, request locks in fixed order (e.g., primary key ascending) |
| **Short Transactions** | Only do necessary operations in transaction, no cross I/O waits or time-consuming calculations |
| **Atomic Claim** | Query and update task status in transaction, use `FOR UPDATE` to lock |
| **Deadlock Retry** (PostgreSQL-specific) | Catch `OperationalError` (SQLSTATE 40P01) and retry |

> **PostgreSQL vs MySQL Difference**: MySQL deadlock error code is errno 1213, PostgreSQL is SQLSTATE 40P01. Both have automatic deadlock detection.

Runnable correct solution demo at [exp4_partition_correct.py](../../examples/chapter_08_scenarios/parallel_workers/exp4_partition_correct.py).

---

## 5. Application Separation Principle

When a system contains two distinctly different workloads, it is recommended to deploy them as separate applications.

### 5.1 Scenario Examples

| Workload Type | Characteristics | Suitable Deployment |
| --- | --- | --- |
| Web API Service | Short requests, high concurrency, response time sensitive | FastAPI / Django + asyncio + connection pool |
| Data Analysis Batch Processing | Long running, large data volume, CPU intensive | Standalone script + multiprocessing + PostgreSQLBackend |
| Task Queue Consumer | Regular polling, independent tasks, easy horizontal scaling | Standalone worker process pool |

### 5.2 Recommended Architecture

```text
User requests ──→ Web application (asyncio + connection pool)
                      │
                      └──→ Task queue (PostgreSQL database table / Redis)
                                  │
                                  └──→ Background worker process pool
                                        (Each process independent PostgreSQLBackend sync connection)
```

Web application receives requests and writes to task queue; worker process pool executes time-consuming tasks.

---

## 6. asyncio Concurrency Behavior on PostgreSQL

### 6.1 Single Connection Model Limitations

`rhosocial-activerecord` follows the **one ActiveRecord class bound to one connection** design principle. This means coroutines within the same process **cannot** concurrently access the database through `asyncio.gather` - a single connection can only handle one query at a time:

```python
# ❌ Wrong: Single connection does not support concurrent access
async def batch_update_wrong(post_ids: list[int]):
    await asyncio.gather(*[
        update_one(pid) for pid in post_ids  # Will raise runtime error
    ])

# ✅ Correct: Sequential execution within single connection
async def batch_update_correct(post_ids: list[int]):
    for pid in post_ids:
        await update_one(pid)
```

### 6.2 Multi-process is the Concurrency Carrier

The advantage of async lies **between multiple processes**: each process holds an independent PostgreSQL TCP connection, achieving true concurrency between processes.

### 6.3 PostgreSQL Async vs MySQL Async Comparison

| Scenario | PostgreSQL (psycopg 3) | MySQL (mysql-connector-python) |
| --- | --- | --- |
| In-process asyncio.gather | ❌ Single connection not supported | ❌ Single connection not supported |
| Multi-process concurrent writes | ✅ MVCC, no configuration needed | ✅ Row-level lock, no configuration needed |
| Deadlock handling | ✅ Auto detection, SQLSTATE 40P01 | ✅ Auto detection, errno 1213 |
| Free-threaded support | ❌ Not supported | ✅ Supported |

---

## 7. Example Code

Complete runnable examples for this chapter are located at [`docs/examples/chapter_08_scenarios/parallel_workers/`](../../examples/chapter_08_scenarios/parallel_workers/), containing the following experiments:

| File | Content | Corresponding Section |
| --- | --- | --- |
| [`config_loader.py`](../../examples/chapter_08_scenarios/parallel_workers/config_loader.py) | Connection configuration loading (YAML / environment variables / defaults) | — |
| [`models.py`](../../examples/chapter_08_scenarios/parallel_workers/models.py) | Shared model definitions (`User`, `Post`, `Comment` sync + async versions) | — |
| [`setup_db.py`](../../examples/chapter_08_scenarios/parallel_workers/setup_db.py) | Database initialization script (sync/async modes) | — |
| [`exp1_basic_multiprocess.py`](../../examples/chapter_08_scenarios/parallel_workers/exp1_basic_multiprocess.py) | Correct multi-process usage (serial/sync multi-process/async multi-process timing comparison) | §2.1 |
| [`exp2_postgres_async_advantage.py`](../../examples/chapter_08_scenarios/parallel_workers/exp2_postgres_async_advantage.py) | PostgreSQL async characteristics: single connection limitation + multi-process concurrent write comparison | §3 |
| [`exp3_deadlock_wrong.py`](../../examples/chapter_08_scenarios/parallel_workers/exp3_deadlock_wrong.py) | Row lock order conflict causing PostgreSQL deadlock (anti-pattern) | §4.1 |
| [`exp4_partition_correct.py`](../../examples/chapter_08_scenarios/parallel_workers/exp4_partition_correct.py) | Data partitioning + atomic claim + deadlock retry (sync/async solutions) | §4.2–4.4 |
| [`exp5_multithread_warning.py`](../../examples/chapter_08_scenarios/parallel_workers/exp5_multithread_warning.py) | Multi-threaded shared connection issues (anti-pattern) | §1.2 |

> **Note**: All example files use `rhosocial-activerecord` ORM directly, with model hierarchy `User → Post → Comment`, demonstrating sync and async equivalent usage of `HasMany` / `BelongsTo` associations.

Before running, execute the initialization script:

```bash
cd docs/examples/chapter_08_scenarios/parallel_workers
python setup_db.py
python exp1_basic_multiprocess.py   # Run any experiment
```

See `README.md` in that directory for complete descriptions and expected outputs of each experiment.

---

## 8. WorkerPool Testing Experience Summary

### 8.1 Known Limitations of Async Worker Testing

When running async tests in a `WorkerPool` multi-process environment, there are known limitations:

#### Event Loop Cross-Process Issue

When `WorkerPool` executes async tasks in child processes, each child process creates an independent event loop via `asyncio.run()`. However, the event loop created by the test framework (pytest-asyncio) in the main process is isolated from the child process event loops:

```text
Main Process (pytest):
  └── Event Loop A (created by pytest-asyncio)
      └── Fixture: async_user_class_for_worker
          └── Async backend instance bound to Loop A

Child Process (Worker):
  └── Event Loop B (created by asyncio.run())
      └── Task tries to use async backend bound to Loop A
          └── Error: Task got Future attached to a different loop
```

**Wrong Example**:

```python
# ❌ Wrong: Async backend created in main process fixture cannot be used in child process
async def async_worker_task(user_id, conn_params):
    # conn_params contains async backend instance bound to main process event loop
    # Child process trying to use it will fail
    backend = conn_params['backend']  # Bound to wrong loop
    user = await backend.find_one(user_id)  # RuntimeError!
```

**Correct Approach**:

```python
# ✅ Correct: Create new async backend instance inside child process
async def async_worker_task(user_id, conn_params):
    # Only pass connection parameters (serializable), create new instance in child process
    config = conn_params['config_kwargs']
    await Model.configure(config, AsyncPostgreSQLBackend)
    user = await Model.find_one(user_id)
    await Model.backend().disconnect()
```

#### Affected Test Scenarios

| Test Type | Sync Version | Async Version | Reason |
|----------|--------------|---------------|--------|
| Parallel reads | ✅ Pass | ❌ Fail | Async backend bound to main process loop |
| Parallel updates | ✅ Pass | ❌ Fail | Same as above |
| Parallel deletes | ✅ Pass | ❌ Fail | Same as above |
| Parallel queries | ✅ Pass | ❌ Fail | Same as above |
| Transaction isolation | ✅ Pass | ❌ Fail | Same as above + transaction state cross-process issue |
| Worker lifecycle | ✅ Pass | ✅ Pass | No cross-process async operations |
| Connection management | ✅ Pass | ✅ Pass | No cross-process async operations |

### 8.2 Test Coverage Notes

Worker test coverage contribution to PostgreSQL backend:

| Module | Coverage | Description |
|----------|----------|-------------|
| `backend.py` | ~32% | Sync backend core functionality |
| `async_backend.py` | ~35% | Async backend core functionality |
| `mixins/` | ~18% | DML operations |
| `dialect.py` | ~22% | SQL dialect handling |

Sync test coverage is higher; async test coverage has uncovered code paths due to event loop issues.

### 8.3 Production Recommendations

1. **Sync Workers Preferred**: In multi-process Worker scenarios, sync backend (`PostgreSQLBackend`) is the more stable choice
2. **Async for Single Process**: Async backend (`AsyncPostgreSQLBackend`) works well in single-process sequential execution scenarios
3. **Avoid Cross-Process Async Instance Passing**: Only pass serializable connection parameters, create new async backend instances in child processes

### 8.4 FOR UPDATE Capability Detection Experience

#### Problem Background

In WorkerPool multi-process testing, the `test_transaction_isolation.py` transfer test used the `FOR UPDATE` clause to lock rows. However, SQLite backend does not support `FOR UPDATE` (uses file-level locks), causing test failures.

#### Solution: Capability Detection Pattern

Following the "don't make choices for users" design principle, we implemented two-layer capability detection:

**1. Dialect-Level Capability Declaration**:

```python
# SQLDialectBase (default not supported)
def supports_for_update(self) -> bool:
    """Default returns False; only backends that support it override this method"""
    return False

# PostgreSQLDialect (explicitly supported)
def supports_for_update(self) -> bool:
    return True
```

**2. ActiveQuery-Level Early Detection**:

```python
# ActiveQuery.for_update() method
def for_update(self, ...):
    if not dialect.supports_for_update():
        raise UnsupportedFeatureError(
            dialect.name,
            "FOR UPDATE clause",
            "This backend does not support row-level locking with FOR UPDATE. "
            "Use dialect.supports_for_update() to check support before calling this method."
        )
```

**3. Dialect-Level Safety Net**:

```python
# SQLDialectBase.format_query_statement()
if expr.for_update:
    if not self.supports_for_update():
        raise UnsupportedFeatureError(...)
    # Generate FOR UPDATE SQL...
```

#### Test Code Adaptation

Use capability detection in test task functions to adapt to different backends:

```python
def transfer_task(from_id: int, to_id: int, amount: float, conn_params: dict):
    # Check if backend supports FOR UPDATE
    supports_for_update = backend.dialect.supports_for_update()

    with Model.transaction():
        if supports_for_update:
            # PostgreSQL/MySQL: Use FOR UPDATE to lock
            first = Model.query().where(Model.c.id == first_id).for_update().one()
        else:
            # SQLite: Use regular query (relies on file locks)
            first = Model.find_one({'id': first_id})
        # ... business logic
```

#### Design Experience Summary

| Experience | Description |
|------------|-------------|
| **Default deny principle** | Base class returns `False`, backends must explicitly declare support |
| **Don't make choices for users** | Raises error when unsupported, rather than silently ignoring (Django/Rails approach) |
| **Dual-layer defense** | ActiveQuery provides early failure, Dialect acts as safety net |
| **User adapts** | Users choose alternative approaches after checking `supports_for_update()` |

### 8.5 Related Files

- Test bridge files: `tests/rhosocial/activerecord_postgres_test/feature/basic/worker/`
- Provider implementations: `tests/providers/basic.py`, `tests/providers/query.py`
- WorkerPool implementation: `rhosocial.activerecord.worker.pool`
