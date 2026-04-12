# docs/examples/chapter_05_worker_isolation/config.py
"""
Configuration loader for Worker isolation experiment (PostgreSQL version).
"""

import os
import yaml
from typing import Optional

from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig


SCHEMA_SQL = {
    "users": """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) NOT NULL UNIQUE,
            email VARCHAR(255) NOT NULL UNIQUE,
            age INTEGER,
            balance DECIMAL(10, 2) NOT NULL DEFAULT 0.0,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            order_number VARCHAR(50) NOT NULL,
            total_amount DECIMAL(10, 2) NOT NULL DEFAULT 0,
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "posts": """
        CREATE TABLE IF NOT EXISTS posts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            title VARCHAR(200) NOT NULL,
            content TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "comments": """
        CREATE TABLE IF NOT EXISTS comments (
            id SERIAL PRIMARY KEY,
            post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            content TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """,
}


def load_scenario_config(scenario_name: str = None) -> PostgresConnectionConfig:
    """Load PostgreSQL connection config from the test scenarios YAML file."""
    env_config_path = os.getenv("POSTGRES_SCENARIOS_CONFIG_PATH")
    if env_config_path and os.path.exists(env_config_path):
        config_path = env_config_path
    else:
        config_path = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "..", "tests", "config", "postgres_scenarios.yaml"
        )

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"PostgreSQL scenarios config not found at {config_path}. "
            f"Please create the config file or set POSTGRES_SCENARIOS_CONFIG_PATH."
        )

    with open(config_path, "r") as f:
        config_data = yaml.safe_load(f)

    scenarios = config_data.get("scenarios", {})
    if not scenarios:
        raise ValueError("No scenarios found in config file")

    if scenario_name is None:
        scenario_name = os.getenv("POSTGRES_SCENARIO")
        if scenario_name is None:
            scenario_name = list(scenarios.keys())[0]

    if scenario_name not in scenarios:
        raise ValueError(
            f"Scenario '{scenario_name}' not found. "
            f"Available: {list(scenarios.keys())}"
        )

    scenario = scenarios[scenario_name]
    return PostgresConnectionConfig(
        host=scenario.get("host", "localhost"),
        port=scenario.get("port", 5432),
        database=scenario.get("database", "test_db"),
        username=scenario.get("username", "root"),
        password=scenario.get("password", ""),
        sslmode=scenario.get("sslmode", "prefer"),
    )


def get_backend_class():
    """Return the PostgreSQL backend class."""
    return PostgresBackend


def setup_database(backend: PostgresBackend, drop_existing: bool = True) -> None:
    """Set up database tables for PostgreSQL."""
    if drop_existing:
        for table in reversed(list(SCHEMA_SQL.keys())):
            backend.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    for table, sql in SCHEMA_SQL.items():
        backend.execute(sql)