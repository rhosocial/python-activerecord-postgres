# config_loader.py - PostgreSQL Connection Configuration Loader
# docs/examples/chapter_08_scenarios/parallel_workers/config_loader.py
"""
PostgreSQL connection configuration loading (three-level priority):

1. tests/config/postgres_scenarios.yaml (if exists, use postgres_16 or first scenario)
2. Environment variables PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD
3. Hardcoded defaults (localhost:5432/test_db/postgres/)

Public interface:
    load_config(scenario=None)  → PostgreSQLConnectionConfig
    list_scenarios()            → list[str]
"""

from __future__ import annotations

import os
import sys
from typing import List, Optional

# ─── sys.path patch (enables standalone execution) ─────────────────────────────────

_src = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "src"))
if _src not in sys.path:
    sys.path.insert(0, _src)

from rhosocial.activerecord.backend.impl.postgres import PostgresConnectionConfig  # noqa: E402

# ─── Default preferred scenarios (by priority) ─────────────────────────────────────

_PREFERRED_SCENARIOS = ["postgres_17", "postgres_16", "postgres_15", "postgres_14", "postgres_13"]


def _find_scenarios_yaml() -> Optional[str]:
    """Find postgres_scenarios.yaml in standard locations."""
    # Relative to this file: ../../../../tests/config/postgres_scenarios.yaml
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.abspath(os.path.join(here, "..", "..", "..", "..", "tests", "config", "postgres_scenarios.yaml")),
        os.environ.get("PG_ACTIVERECORD_CONFIG_PATH", ""),
    ]
    for path in candidates:
        if path and os.path.isfile(path):
            return path
    return None


def load_config(scenario: Optional[str] = None) -> PostgresConnectionConfig:
    """Load PostgreSQL connection configuration.

    Args:
        scenario: Scenario name in YAML (e.g., "postgres_16"). If None, auto-select by priority.

    Returns:
        PostgreSQLConnectionConfig instance.
    """
    yaml_path = _find_scenarios_yaml()
    if yaml_path:
        try:
            import yaml  # type: ignore[import]

            with open(yaml_path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            scenarios: dict = data.get("scenarios", {})
            if scenarios:
                if scenario and scenario in scenarios:
                    params = scenarios[scenario]
                else:
                    # Select scenario by priority
                    params = None
                    for preferred in _PREFERRED_SCENARIOS:
                        if preferred in scenarios:
                            params = scenarios[preferred]
                            break
                    if params is None:
                        params = next(iter(scenarios.values()))
                # Filter out None values to avoid dataclass field type conflicts
                return PostgresConnectionConfig(**{k: v for k, v in params.items() if v is not None})
        except ImportError:
            print("Warning: pyyaml not installed, skipping YAML config loading. Run pip install pyyaml", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Cannot read {yaml_path}: {e}, using environment variables/defaults", file=sys.stderr)

    # Build configuration from environment variables or hardcoded defaults
    # PostgreSQL environment variable naming follows libpq convention
    return PostgresConnectionConfig(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        database=os.environ.get("PGDATABASE", "test_db"),
        username=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
    )


def list_scenarios() -> List[str]:
    """Return all available scenario names in YAML, or empty list if config file not found."""
    yaml_path = _find_scenarios_yaml()
    if yaml_path:
        try:
            import yaml  # type: ignore[import]

            with open(yaml_path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            return list(data.get("scenarios", {}).keys())
        except Exception:
            pass
    return []


def show_active_config(config: PostgresConnectionConfig) -> None:
    """Print current connection parameters (password hidden)."""
    print(f"  Host: {config.host}:{config.port}")
    print(f"  Database: {config.database}")
    print(f"  User: {config.username}")
