# Local PostgreSQL Testing

## Overview

This section describes how to set up a local PostgreSQL testing environment.

## Running PostgreSQL with Docker

```bash
# Run PostgreSQL container
docker run -d \
  --name postgres-test \
  -e POSTGRES_USER=test_user \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=test \
  -p 5432:5432 \
  postgres:16

# Wait for PostgreSQL to start
docker exec postgres-test pg_isready -U test_user
```

## Using Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: test_user
      POSTGRES_PASSWORD: test
      POSTGRES_DB: test
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

```bash
docker-compose up -d
```

## Running Tests

```bash
# Set environment variables
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=test
export PG_USER=test_user
export PG_PASSWORD=test

# Run tests (serially - no parallel execution)
pytest tests/
```

## CRITICAL: No Parallel Test Execution

**Tests MUST be executed serially.** The test suite uses fixed table names, and parallel execution will cause conflicts and failures.

```bash
# DO NOT use parallel execution
pytest -n auto          # ❌ WILL CAUSE FAILURES
pytest -n 4             # ❌ WILL CAUSE FAILURES

# Always run tests serially (default behavior)
pytest                  # ✅ Correct
```

💡 *AI Prompt:* "What is the difference between Docker and Docker Compose?"
