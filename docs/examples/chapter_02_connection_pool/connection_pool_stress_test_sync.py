#!/usr/bin/env python3
"""
Example: PostgreSQL Connection Pool Stress Test (Synchronous)

This example demonstrates a stress test for the connection pool by having multiple
threads repeatedly acquire and release connections from the same pool.
It verifies the reliability of PooledBackend under concurrent usage.

Run with: .venv_postgres\Scripts\python connection_pool_stress_test_sync.py
Or in PostgreSQL virtual environment

Requirements:
    pip install psycopg
    pip install ..\\python-activerecord\\src
    pip install ..\\python-activerecord-testsuite\\src
    # Or use the virtual environment: .venv_postgres*
"""

import sys
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

os.environ.setdefault("PG_HOST", "")
os.environ.setdefault("PG_PORT", "")
os.environ.setdefault("PG_USER", "")
os.environ.setdefault("PG_PASSWORD", "")
os.environ.setdefault("PG_DATABASE", "")
os.environ.setdefault("PG_SSLMODE", "")

# Add the project paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", 
                             "python-activerecord", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", 
                             "python-activerecord-testsuite", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", 
                             "python-activerecord-postgres", "src"))

from rhosocial.activerecord.connection.pool import PoolConfig, BackendPool
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


def worker_thread(pool: BackendPool, worker_id: int, iterations: int, lock: threading.Lock):
    """Worker function that repeatedly acquires and releases connections."""
    success_count = 0
    error_count = 0
    
    for i in range(iterations):
        backend = None
        try:
            # Acquire connection from pool
            backend = pool.acquire(timeout=30.0)
            
            # Output backend info for verification
            with lock:
                print(f"  [Worker {worker_id}] Iteration {i + 1}/{iterations}")
                print(f"    threadsafety: {backend.threadsafety}")
                print(f"    mode: {pool.connection_mode}")
            
            # Execute a simple query to verify connection works
            options = ExecutionOptions(stmt_type=StatementType.DQL)
            result = backend.execute("SELECT 1 AS test", [], options=options)
            
            if result.data and result.data[0]["test"] == 1:
                success_count += 1
                with lock:
                    print(f"    [Worker {worker_id}] Query OK")
            else:
                error_count += 1
                with lock:
                    print(f"    [Worker {worker_id}] Query failed: unexpected result")
            
            # Small delay to simulate work
            time.sleep(0.01)
            
        except Exception as e:
            error_count += 1
            with lock:
                print(f"    [Worker {worker_id}] Error: {e}")
        finally:
            if backend is not None:
                pool.release(backend)
    
    return worker_id, success_count, error_count


def main():
    # Pre-set environment defaults to empty strings to prevent accidental leakage
    os.environ.setdefault("PG_HOST", "")
    os.environ.setdefault("PG_PORT", "")
    os.environ.setdefault("PG_USER", "")
    os.environ.setdefault("PG_PASSWORD", "")
    os.environ.setdefault("PG_DATABASE", "")
    os.environ.setdefault("PG_SSLMODE", "")
    os.environ.setdefault("PG_CONNECT_TIMEOUT", "")
    
    print("=" * 70)
    print("PostgreSQL Connection Pool Stress Test (Synchronous)")
    print("=" * 70)
    
    # PostgreSQL connection parameters - use environment variables
    pg_host = (os.environ.get("PG_HOST") or "").strip()
    pg_port = int((os.environ.get("PG_PORT") or "0").strip() or 0)
    pg_user = (os.environ.get("PG_USER") or "").strip()
    pg_password = (os.environ.get("PG_PASSWORD") or "").strip()
    pg_database = (os.environ.get("PG_DATABASE") or "").strip()
    pg_connect_timeout = int((os.environ.get("PG_CONNECT_TIMEOUT") or "30").strip() or 30)
    
    postgres_config = {
        "host": pg_host,
        "username": pg_user,
        "password": pg_password,
        "database": pg_database,
    }
    if pg_port > 0:
        postgres_config["port"] = pg_port
    if pg_connect_timeout > 0:
        postgres_config["connect_timeout"] = pg_connect_timeout
    
    print("Environment variables:")
    for key in ["PG_HOST", "PG_PORT", "PG_USER", "PG_DATABASE", "PG_CONNECT_TIMEOUT"]:
        print(f"  {key}={os.environ.get(key, 'NOT SET')}")
    print(f"\nConnection config: {postgres_config}")
    print()
    
    try:
        # Create backend to get threadsafety info
        test_backend = PostgresBackend(**postgres_config)
        test_backend.connect()
        print(f"Backend threadsafety: {test_backend.threadsafety}")
        print(f"  0 = None (not thread-safe)")
        print(f"  1 = driver (only supports SQL)")
        print(f"  2 = Full thread-safe (psycopg)")
        test_backend.disconnect()
        
        # Create connection pool with higher load
        config = PoolConfig(
            min_size=10,
            max_size=50,
            connection_mode="auto",  # auto-detect based on threadsafety
            validate_on_borrow=True,
            validation_query="SELECT 1",
            backend_factory=lambda: PostgresBackend(**postgres_config)
        )
        
        print(f"\nPool configuration:")
        print(f"  min_size: {config.min_size}")
        print(f"  max_size: {config.max_size}")
        print(f"  connection_mode: {config.connection_mode}")
        print(f"  validate_on_borrow: {config.validate_on_borrow}")
        print(f"  validation_query: {config.validation_query}")
        
        pool = BackendPool.create(config)
        
        print(f"\nEffective connection mode: {pool.connection_mode}")
        
        print(f"\nPool initial stats: {pool.get_stats()}")
        
        # -----------------------------------------------------------
        # Stress test with multiple threads
        # -----------------------------------------------------------
        print("\n" + "-" * 50)
        print("Starting stress test with 20 workers, 50 iterations each")
        print("-" * 50)
        
        num_workers = 20
        iterations = 50
        lock = threading.Lock()
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(worker_thread, pool, i, iterations, lock)
                for i in range(num_workers)
            ]
            
            for future in as_completed(futures):
                worker_id, success, errors = future.result()
                print(f"Worker {worker_id} completed: {success} success, {errors} errors")
        
        elapsed = time.time() - start_time
        
        # -----------------------------------------------------------
        # Results
        # -----------------------------------------------------------
        print("\n" + "-" * 50)
        print("Stress test results")
        print("-" * 50)
        
        stats = pool.get_stats()
        print(f"Total connections created: {stats.total_created}")
        print(f"Total acquired: {stats.total_acquired}")
        print(f"Total released: {stats.total_released}")
        print(f"Current available: {stats.current_available}")
        print(f"Current in use: {stats.current_in_use}")
        print(f"Elapsed time: {elapsed:.2f}s")
        
        # -----------------------------------------------------------
        # Cleanup
        # -----------------------------------------------------------
        print("\n" + "-" * 50)
        print("Cleanup")
        print("-" * 50)
        
        pool.close()
        print(f"Pool closed: {pool.is_closed}")
        
        print("\n" + "=" * 70)
        print("Stress test completed!")
        print("=" * 70)
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure PostgreSQL server is running and credentials are correct.")


if __name__ == "__main__":
    main()