# exp6_backend_group_pool.py - BackendGroup and BackendPool for PostgreSQL
# docs/examples/chapter_08_scenarios/parallel_workers/exp6_backend_group_pool.py
"""
Experiment objective: Compare BackendGroup + backend.context() vs BackendPool
for thread-safe PostgreSQL operations.

Key points:
- PostgreSQL (psycopg) has threadsafety=2 (connections can be shared across threads)
- BackendPool is SUITABLE for PostgreSQL - recommended for thread-safe operations
- BackendGroup + backend.context() also works, but has race condition limitations

PostgreSQL-specific notes:
- psycopg's connections are thread-safe (threadsafety=2)
- BackendPool can be used for connection pooling
- Each thread can acquire its own connection from the pool

How to run:
python setup_db.py  # Initialize database first
python exp6_backend_group_pool.py
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

_src = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "src"))
if _src not in sys.path:
    sys.path.insert(0, _src)

from rhosocial.activerecord.connection import BackendGroup
from rhosocial.activerecord.connection.pool import PoolConfig, BackendPool
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend, PostgresConnectionConfig

from config_loader import load_config
from models import Comment, Post, User

NUM_THREADS = 4


def init_test_data() -> None:
    """Create test tables and data if not exists"""
    config = load_config()
    backend = PostgresBackend(connection_config=config)
    backend.connect()
    
    backend.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(64) NOT NULL,
            email VARCHAR(255) NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    backend.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            title VARCHAR(255) NOT NULL,
            body TEXT,
            status VARCHAR(20) NOT NULL DEFAULT 'published',
            view_count INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    backend.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            id SERIAL PRIMARY KEY,
            post_id INTEGER NOT NULL REFERENCES posts(id),
            user_id INTEGER NOT NULL REFERENCES users(id),
            body TEXT,
            is_approved BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    result = backend.execute("SELECT COUNT(*) as cnt FROM users WHERE username = 'bot'")
    if result.data[0]['cnt'] == 0:
        backend.execute("""
            INSERT INTO users (username, email, is_active)
            VALUES ('bot', 'bot@example.com', TRUE)
        """)
    
    result = backend.execute("SELECT COUNT(*) as cnt FROM posts")
    if result.data[0]['cnt'] < 20:
        user_result = backend.execute("SELECT id FROM users LIMIT 1")
        user_id = user_result.data[0]['id'] if user_result.data else 1
        for i in range(20):
            backend.execute(f"""
                INSERT INTO posts (user_id, title, body, status)
                VALUES ({user_id}, 'Test Post {i}', 'Body of test post {i}', 'published')
            """)
    
    backend.disconnect()


def reset_exp_data() -> None:
    """Delete comments inserted by this experiment"""
    config = load_config()
    try:
        Comment.configure(config, PostgresBackend)
        to_delete = Comment.query().where(Comment.c.body.like("[exp6]%")).all()
        for c in to_delete:
            c.delete()
        Comment.backend().disconnect()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 1: BackendPool (✅ Thread-safe, RECOMMENDED for PostgreSQL)
# ─────────────────────────────────────────────────────────────────────────────

def thread_worker_pool(pool: BackendPool, thread_id: int, post_ids: list) -> dict:
    """
    Thread-safe worker using BackendPool.
    
    Each thread:
    1. Acquires a connection from the pool
    2. Performs database operations
    3. Releases connection back to pool
    """
    results = {"thread_id": thread_id, "success": 0, "errors": []}
    
    try:
        with pool.connection() as backend:
            User.__backend__ = backend
            Post.__backend__ = backend
            Comment.__backend__ = backend
            
            bot = User.query().order_by(User.c.id).one()
            if bot is None:
                results["errors"].append("No bot user found")
                return results
            
            for post_id in post_ids:
                try:
                    post = Post.find_one(post_id)
                    if post is None:
                        continue
                    
                    comment = Comment(
                        post_id=post.id,
                        user_id=bot.id,
                        body=f"[exp6-pool] Thread {thread_id} comment on post {post_id}",
                        is_approved=True
                    )
                    comment.save()
                    results["success"] += 1
                    
                except Exception as e:
                    results["errors"].append(f"Post {post_id}: {str(e)}")
    
    except Exception as e:
        results["errors"].append(f"Pool error: {str(e)}")
    
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 2: BackendGroup + backend.context() (⚠️ Has race conditions)
# ─────────────────────────────────────────────────────────────────────────────

def thread_worker_group(group: BackendGroup, thread_id: int, post_ids: list) -> dict:
    """
    Worker using BackendGroup + backend.context().
    
    Note: This has race conditions because all threads share the same backend instance,
    and context() modifies the same connection state.
    """
    backend = group.get_backend()
    results = {"thread_id": thread_id, "success": 0, "errors": []}
    
    try:
        with backend.context():
            bot = User.query().order_by(User.c.id).one()
            if bot is None:
                results["errors"].append("No bot user found")
                return results
            
            for post_id in post_ids:
                try:
                    post = Post.find_one(post_id)
                    if post is None:
                        continue
                    
                    comment = Comment(
                        post_id=post.id,
                        user_id=bot.id,
                        body=f"[exp6-group] Thread {thread_id} comment on post {post_id}",
                        is_approved=True
                    )
                    comment.save()
                    results["success"] += 1
                    
                except Exception as e:
                    results["errors"].append(f"Post {post_id}: {str(e)}")
    
    except Exception as e:
        results["errors"].append(f"Context error: {str(e)}")
    
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("BackendGroup vs BackendPool Thread Safety Demo (PostgreSQL)")
    print("=" * 60)
    
    print("\nInitializing test data...")
    init_test_data()
    
    config = load_config()
    
    print("Resetting experiment data...")
    reset_exp_data()
    
    # Get post IDs
    Post.configure(config, PostgresBackend)
    posts = Post.query().limit(20).all()
    all_post_ids = [p.id for p in posts if p.id is not None]
    Post.backend().disconnect()
    
    chunk_size = max(1, len(all_post_ids) // NUM_THREADS)
    chunks = [all_post_ids[i:i + chunk_size] for i in range(0, len(all_post_ids), chunk_size)]
    
    # ─────────────────────────────────────────────────────────────────────────
    # Scenario 1: BackendPool
    # ─────────────────────────────────────────────────────────────────────────
    
    print("\n" + "=" * 60)
    print("Scenario 1: BackendPool (✅ Thread-safe, RECOMMENDED)")
    print("=" * 60)
    
    pool_config = PoolConfig(
        min_size=2,
        max_size=10,
        backend_factory=lambda: PostgresBackend(connection_config=config)
    )
    pool = BackendPool(pool_config)
    
    start_time = time.time()
    results_pool = []
    
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for i, chunk in enumerate(chunks[:NUM_THREADS]):
            future = executor.submit(thread_worker_pool, pool, i, chunk)
            futures.append(future)
        
        for future in futures:
            results_pool.append(future.result())
    
    pool_time = time.time() - start_time
    pool.close()
    
    print(f"\nResults (BackendPool):")
    total_success = sum(r["success"] for r in results_pool)
    total_errors = sum(len(r["errors"]) for r in results_pool)
    for r in results_pool:
        if r["errors"]:
            print(f"  Thread {r['thread_id']}: {r['success']} success, {len(r['errors'])} errors")
        else:
            print(f"  Thread {r['thread_id']}: {r['success']} success ✓")
    
    print(f"\nTotal: {total_success} comments created, {total_errors} errors")
    print(f"Time: {pool_time:.3f}s")
    
    # Reset for next scenario
    reset_exp_data()
    
    # ─────────────────────────────────────────────────────────────────────────
    # Scenario 2: BackendGroup + backend.context()
    # ─────────────────────────────────────────────────────────────────────────
    
    print("\n" + "=" * 60)
    print("Scenario 2: BackendGroup + backend.context() (⚠️ Race conditions)")
    print("=" * 60)
    
    group = BackendGroup(
        name="exp6_group",
        models=[User, Post, Comment],
        config=config,
        backend_class=PostgresBackend
    )
    group.configure()
    
    start_time = time.time()
    results_group = []
    
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for i, chunk in enumerate(chunks[:NUM_THREADS]):
            future = executor.submit(thread_worker_group, group, i, chunk)
            futures.append(future)
        
        for future in futures:
            results_group.append(future.result())
    
    group_time = time.time() - start_time
    group.disconnect()
    
    print(f"\nResults (BackendGroup):")
    total_success = sum(r["success"] for r in results_group)
    total_errors = sum(len(r["errors"]) for r in results_group)
    for r in results_group:
        if r["errors"]:
            print(f"  Thread {r['thread_id']}: {r['success']} success, {len(r['errors'])} errors")
        else:
            print(f"  Thread {r['thread_id']}: {r['success']} success")
    
    print(f"\nTotal: {total_success} comments created, {total_errors} errors")
    print(f"Time: {group_time:.3f}s")
    
    # ─────────────────────────────────────────────────────────────────────────
    # Summary
    # ─────────────────────────────────────────────────────────────────────────
    
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"BackendPool:       {sum(r['success'] for r in results_pool)} success, {sum(len(r['errors']) for r in results_pool)} errors")
    print(f"BackendGroup:      {sum(r['success'] for r in results_group)} success, {sum(len(r['errors']) for r in results_group)} errors")
    
    print("\n" + "=" * 60)
    print("Conclusion:")
    print("=" * 60)
    print("""
PostgreSQL (psycopg) has threadsafety=2 - BackendPool IS RECOMMENDED:
- Each thread acquires its own connection from the pool
- Connection pool manages connection lifecycle efficiently
- True thread-safe concurrent database operations

BackendGroup + backend.context() has limitations:
- All threads share the same backend instance
- context() modifies the same connection state
- Race conditions occur when multiple threads call context() simultaneously

Key principle:
- For PostgreSQL: Use BackendPool for thread-safe operations
- For MySQL (threadsafety=1): Use multiprocessing, NOT threading
- '随用随连、用完即断' (Connect on demand, disconnect after use)
  - BackendPool: Pool manages connection lifecycle
  - BackendGroup: Each thread needs its own group instance for true isolation
""")


if __name__ == "__main__":
    main()
