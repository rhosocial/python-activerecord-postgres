> **Important**: This document is a supplement to the main **Testing Architecture and Execution Guide** ([local](../../python-activerecord/.claude/testing.md) | [github](https://github.com/Rhosocial/python-activerecord/blob/main/.claude/testing.md)). Please read the core documentation first to understand the overall testing architecture, provider patterns, and capability negotiation. This guide focuses only on details specific to the PostgreSQL backend.

# PostgreSQL Backend Testing Guide

## 1. Overview

This guide provides instructions for setting up and running tests for the `rhosocial-activerecord-postgres` backend, covering PostgreSQL-specific configuration, testing patterns, and troubleshooting.

## 2. PostgreSQL Test Environment Setup

### 2.1. Dependencies

In addition to core testing dependencies, the PostgreSQL backend requires `psycopg`. For easier installation, `psycopg-binary` is also included.

```toml
# pyproject.toml
[project]
dependencies = [
    "psycopg>=3.2.12",
    "psycopg-binary>=3.2.12"
]

[project.optional-dependencies]
test = [
    # ... other dependencies
    "rhosocial-activerecord-testsuite",
]
```

### 2.2. Test Scenario Configuration

All database configurations for testing are managed through **test scenarios**. The primary method for defining these scenarios is the `tests/config/postgres_scenarios.yaml` file. This file is git-ignored, so you must create it locally.

The test framework, specifically `config_manager.py` and `providers/scenarios.py`, reads this file to understand which database environments are available for testing.

**Example `tests/config/postgres_scenarios.yaml`:**

This example shows how to configure connections for different versions of PostgreSQL. You should adapt the host, port, and credential details to your local environment.

```yaml
scenarios:
  # Example for a PostgreSQL 16 server
  postgres_16:
    host: "localhost"
    port: 5432
    username: "test_user"
    password: "your_password" # Use ${PG_PASSWORD} to load from env var
    database: "test_activerecord_pg16"
    sslmode: "prefer"

  # Example for a PostgreSQL 15 server (commented out by default)
  # postgres_15:
  #   host: "localhost"
  #   port: 5433
  #   username: "test_user"
  #   password: "your_password"
  #   database: "test_activerecord_pg15"
```

Alternatively, for CI/CD environments, you can configure scenarios using environment variables, but the YAML file is recommended for local development.

### 2.3. Setting Up Test Databases with Docker

For a consistent and isolated testing environment, you can use Docker to run PostgreSQL instances.

```bash
# Start a PostgreSQL 16 container for the 'postgres_16' scenario
docker run -d \
  --name postgres-test-16 \
  -e POSTGRES_USER=test_user \
  -e POSTGRES_PASSWORD=your_password \
  -e POSTGRES_DB=test_activerecord_pg16 \
  -p 5432:5432 \
  postgres:16
```

## 3. Testing Against the Shared Test Suite

Integrating the PostgreSQL backend with `rhosocial-activerecord-testsuite` involves implementing a set of provider classes that adapt the generic tests to the PostgreSQL environment.

### 3.1. Implement Provider Interfaces by Feature

For each feature category defined in the test suite (e.g., `basic`, `query`), a corresponding provider must be implemented in the `tests/providers/` directory.

Each provider is responsible for:
1.  **Setting up the database connection** for its specific test scenario.
2.  **Creating the necessary schema**. This involves converting SQLite-based schemas from the test suite to PostgreSQL-compatible SQL (e.g., changing `AUTOINCREMENT` to `SERIAL` or `IDENTITY`).
3.  **Configuring and returning ActiveRecord models**.
4.  **Handling cleanup** (teardown).

The `providers/registry.py` file discovers and registers these provider implementations.

### 3.2. Import and Execute Shared Tests

With providers in place, tests are triggered by files in `tests/rhosocial/activerecord_postgres_test/feature/`.

To ensure maximum compatibility, **it is strongly recommended to import and run the entire suite of tests without omissions.** This provides a comprehensive validation of the backend's implementation.

These test files import test classes from `rhosocial-activerecord-testsuite`, and pytest fixtures invoke the providers to run the tests against the PostgreSQL backend.

```bash
# Ensure the test suite is installed
pip install rhosocial-activerecord-testsuite
```

## 4. Capability Declaration

Capabilities are declared once, during the **backend's instantiation phase**, not during testing. The `PostgreSQLBackend` class's `_initialize_capabilities` method is called on creation to build a `DatabaseCapabilities` object based on the database version.

**Example: Declaring Capabilities in `PostgreSQLBackend`:**
```python
# src/rhosocial/activerecord/backend/impl/postgres/backend.py
from rhosocial.activerecord.backend.capabilities import (
    DatabaseCapabilities,
    CTECapability,
    JSONCapability
)

class PostgreSQLBackend(StorageBackend):
    def _initialize_capabilities(self):
        """Initializes and returns the backend's capability descriptor."""
        capabilities = DatabaseCapabilities()
        version = self.get_server_version() # Returns a tuple like (16, 1, 0)

        # PostgreSQL has robust JSONB support
        if version >= (9, 4, 0):
            capabilities.add_json_operation([
                JSONCapability.BASIC_JSON,
                JSONCapability.JSON_EXTRACT,
                JSONCapability.JSONB, # PostgreSQL specific
            ])

        # RETURNING clause is well-supported
        capabilities.add_dml_returning(True)

        return capabilities
```
The test suite automatically uses this information to skip unsupported tests. For backend-specific tests, you are responsible for managing capability checks.

## 5. Writing PostgreSQL-Specific Tests

Dedicated tests are needed for PostgreSQL-specific features. These can be organized in a separate directory or interspersed within the `tests/rhosocial/activerecord_postgres_test/feature/` directories.

### Example: Testing JSONB Contains Operator

```python
# tests/feature/query/test_postgres_jsonb.py
def test_postgres_jsonb_contains_operator(pg_backend):
    """Tests PostgreSQL's JSONB '@>' contains operator."""
    pg_backend.execute("CREATE TABLE users (id SERIAL, data JSONB)")
    
    class User(ActiveRecord):
        # ...
    
    User(data={'tags': ['python', 'orm']}).save()
    
    # Use the @> operator for JSONB containment
    found_user = User.where("data @> %s", [{'tags': ['python']}]).first()
    
    assert found_user is not None
```

## 6. Troubleshooting

*   **Issue**: `psycopg.OperationalError: connection failed`
    *   **Solution**:
        1.  Check if your PostgreSQL service is running (e.g., via `docker ps`).
        2.  Confirm that the details in your `postgres_scenarios.yaml` match your running database instance.
        3.  Try connecting manually (e.g., with `psql`) to verify credentials and permissions.

*   **Issue**: PostgreSQL syntax errors during shared tests.
    *   **Solution**: Check the schema conversion logic in your providers. It may need to be updated to handle more SQLite-to-PostgreSQL syntax differences (e.g., `AUTOINCREMENT` to `SERIAL`).

*   **Issue**: Tests skipped due to unsupported capabilities.
    *   **Solution**: Check the `_initialize_capabilities` method in your backend implementation. Ensure it correctly declares all supported features based on the PostgreSQL version.

## 7. Quick Command Reference

**Crucial Prerequisite**: Before running any tests, `PYTHONPATH` **must** be set.

```bash
# Set PYTHONPATH (Linux/macOS)
export PYTHONPATH=src

# Set PYTHONPATH (Windows PowerShell)
$env:PYTHONPATH="src"

# ---

# --- Test Execution ---

# Run all PostgreSQL backend tests
pytest tests/

# Run tests for a specific feature
pytest tests/rhosocial/activerecord_postgres_test/feature/basic/

# Run specific tests by name
pytest -k "jsonb"

# View coverage
pytest --cov=rhosocial.activerecord.backend.impl.postgres tests/
```

## 8. CRITICAL: No Parallel Test Execution

**Tests MUST be executed serially. Do NOT use pytest-xdist or similar parallel execution tools.**

### Why Parallel Execution Is Not Supported

The test suite creates database tables with **fixed names** (e.g., `users`, `orders`, `posts`). Each test:

1. Drops existing tables: `DROP TABLE IF EXISTS ... CASCADE`
2. Creates fresh tables with the same names
3. Inserts test data
4. Cleans up after completion

If tests run in parallel, they will:
- Drop tables while other tests are using them → Errors
- Create conflicting data → Test failures
- Cleanup prematurely → Data loss

### What NOT To Do

```bash
# DO NOT use parallel execution
pytest -n auto          # ❌ WILL CAUSE FAILURES
pytest -n 4             # ❌ WILL CAUSE FAILURES
pytest --dist=loadfile  # ❌ WILL CAUSE FAILURES
```

### Correct Execution

```bash
# Always run tests serially (default behavior)
pytest                  # ✅ Correct - serial execution
```

For more details, see the main project's [Testing Documentation](https://github.com/Rhosocial/python-activerecord/blob/main/.claude/testing.md).
