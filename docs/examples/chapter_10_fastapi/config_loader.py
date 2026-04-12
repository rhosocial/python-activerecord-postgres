# config_loader.py - PostgreSQL Connection Configuration for FastAPI
# docs/examples/chapter_10_fastapi/config_loader.py

from __future__ import annotations

import os
import sys

_src = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "src"))
if _src not in sys.path:
    sys.path.insert(0, _src)

from rhosocial.activerecord.backend.impl.postgres import PostgresConnectionConfig


def load_config() -> PostgresConnectionConfig:
    """Load PostgreSQL connection configuration from environment or defaults."""
    return PostgresConnectionConfig(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DATABASE", "test_db"),
        username=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )
