# $\mathcal{B}_{\text{postgres}}^{\rho}$ - PostgreSQL Backend Quick Execution Tool

This `__main__.py` script provides a command-line interface to quickly execute SQL queries against a PostgreSQL database using the `rhosocial-activerecord` PostgreSQL backend implementation. It supports both synchronous and asynchronous execution modes.

## Purpose

This tool is designed for:
*   Rapid testing of PostgreSQL backend connectivity and query execution.
*   Debugging specific SQL queries or backend behaviors.
*   Performing quick database operations (DDL/DML) directly from the command line.

## Usage

To run the script, navigate to the root directory of the project (where the `src` folder is located). Then, execute the module using `python -m` followed by the module path.

The SQL query is a **positional argument** and should be the last argument after all optional flags.

```bash
python -m rhosocial.activerecord.backend.impl.postgres [OPTIONAL_FLAGS] "YOUR_SQL_QUERY;"
```

## Arguments

| Argument         | Default                                           | Description                                                                                                                                                             |
| :--------------- | :------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `--host`         | `POSTGRES_HOST` env var or `localhost`            | Database host.                                                                                                                                          |
| `--port`         | `POSTGRES_PORT` env var or `5432`                 | Database port.                                                                                                                                          |
| `--database`     | `POSTGRES_DATABASE` env var or _None_             | Database name to connect to. **Important**: If not provided, it defaults to the `username`. For example, a `root` user will attempt to connect to a `root` database. |
| `--user`         | `POSTGRES_USER` env var or `postgres`             | Database user.                                                                                                                                          |
| `--password`     | `POSTGRES_PASSWORD` env var or _empty string_     | Database password.                                                                                                                                      |
| `query`          | _Required_ (positional)                           | **SQL query to execute.** Must be enclosed in quotes.                                                                                                   |
| `--use-async`    | _False_                                           | Use the asynchronous backend (`AsyncPostgresBackend`). If omitted, the synchronous backend (`PostgresBackend`) will be used.                               |
| `--log-level`    | `INFO`                                            | Set the logging level (e.g., `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`).                                                                        |
| `--rich-ascii`   | _False_                                           | Use ASCII characters for table borders. Recommended for terminals that have trouble rendering Unicode box characters.                                |

## Pretty Output with `rich`

This tool integrates with the [rich](https://github.com/Textualize/rich) library to provide beautified, color-coded output and logging. This is an optional feature.

### Activation

To enable this feature, simply install `rich` in your Python environment:
```bash
pip install rich
```
The script will automatically detect its presence and enhance the output. If `rich` is not found, the script will fall back to standard plain text output.

### Rendering Issues in Terminals

Some terminals may not correctly render the box-drawing characters used in tables by default. To fix this, you can use the `--rich-ascii` flag.

```bash
python -m rhosocial.activerecord.backend.impl.postgres ... --rich-ascii "SELECT 1;"
```
This forces the table borders to be rendered using only ASCII characters, which is compatible with all terminals.

## Using Environment Variables for Connection Parameters

To simplify command-line execution, you can set connection parameters as environment variables. Command-line arguments will always take precedence over environment variables.

**Supported Environment Variables:**
*   `POSTGRES_HOST`
*   `POSTGRES_PORT`
*   `POSTGRES_DATABASE`
*   `POSTGRES_USER`
*   `POSTGRES_PASSWORD`

**Examples for setting environment variables and running commands:**

### Bash (Linux/macOS)

```bash
# Set environment variables for the current session
export POSTGRES_HOST="your_postgres_host"
export POSTGRES_PORT="5432"
export POSTGRES_USER="root"
export POSTGRES_PASSWORD="your_secure_password"
export POSTGRES_DATABASE="postgres" # Recommended to set a default DB

# Run a query using environment variables
python -m rhosocial.activerecord.backend.impl.postgres "SELECT NOW();" --use-async

# Override user via command-line
python -m rhosocial.activerecord.backend.impl.postgres "SELECT * FROM your_table;" --user guest
```

### PowerShell (Windows)

```powershell
# Set environment variables for the current session
$env:POSTGRES_HOST="your_postgres_host"
$env:POSTGRES_PORT="5432"
$env:POSTGRES_USER="root"
$env:POSTGRES_PASSWORD="your_secure_password"
$env:POSTGRES_DATABASE="postgres" # Recommended to set a default DB

# Run a query using environment variables
python -m rhosocial.activerecord.backend.impl.postgres "SELECT NOW();" --use-async
```

## Examples

This section provides a complete lifecycle example of database operations, from creating a database to cleaning it up.

**Note**:
*   The following examples assume you are running the commands from the root of this project.
*   For a multi-repository setup, you must set your `PYTHONPATH` to include the `src` directories of all relevant projects.
*   Replace placeholder credentials (`your_postgres_host`, etc.) with your actual PostgreSQL server credentials, or set the corresponding environment variables.

---

### System Information Queries

These queries help you retrieve basic information about the PostgreSQL server and your current connection.

#### Query PostgreSQL Version

```bash
python -m rhosocial.activerecord.backend.impl.postgres \
    --host your_postgres_host --user your_postgres_user --password your_secure_password \
    "SELECT version();"
```

#### Query Current User Details

```bash
python -m rhosocial.activerecord.backend.impl.postgres \
    --host your_postgres_host --user your_postgres_user --password your_secure_password \
    "SELECT usename, client_addr, application_name FROM pg_stat_activity WHERE pid = pg_backend_pid();"
```

#### Check Detailed TLS/SSL Status

```bash
python -m rhosocial.activerecord.backend.impl.postgres \
    --host your_postgres_host --user your_postgres_user --password your_secure_password \
    "SELECT * FROM pg_stat_ssl WHERE pid = pg_backend_pid();"
```

---

### Step 1: Create a Database

First, we'll create a new database named `test_db`. Note that we connect to the default `postgres` database to perform this action.

```bash
python -m rhosocial.activerecord.backend.impl.postgres \
    --host your_postgres_host --user your_postgres_user --password your_secure_password \
    --database postgres \
    "CREATE DATABASE test_db;"
```

### Step 2: Create a Table

Now, let's create a `users` table within our new database. We'll specify `--database test_db` for this and all subsequent table-level commands. Note the use of `SERIAL PRIMARY KEY` for auto-incrementing IDs in PostgreSQL.

```bash
python -m rhosocial.activerecord.backend.impl.postgres \
    --host your_postgres_host --user your_postgres_user --password your_secure_password \
    --database test_db \
    "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100));"
```

### Step 3: Insert Records

Let's add two users, 'Alice' and 'Bob', to our `users` table.

```bash
# Insert Alice
python -m rhosocial.activerecord.backend.impl.postgres \
    --host your_postgres_host --user your_postgres_user --password your_secure_password \
    --database test_db \
    "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');"

# Insert Bob
python -m rhosocial.activerecord.backend.impl.postgres \
    --host your_postgres_host --user your_postgres_user --password your_secure_password \
    --database test_db \
    "INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com');"
```

### Step 4: Query Records

Retrieve all records from the `users` table.

```bash
python -m rhosocial.activerecord.backend.impl.postgres \
    --host your_postgres_host --user your_postgres_user --password your_secure_password \
    --database test_db \
    "SELECT * FROM users;"
```

### Step 5: Update a Record

Let's update Alice's email address.

```bash
python -m rhosocial.activerecord.backend.impl.postgres \
    --host your_postgres_host --user your_postgres_user --password your_secure_password \
    --database test_db \
    "UPDATE users SET email = 'alice_updated@example.com' WHERE name = 'Alice';"
```

### Step 6: Delete a Record

Now, let's remove Bob from the table.

```bash
python -m rhosocial.activerecord.backend.impl.postgres \
    --host your_postgres_host --user your_postgres_user --password your_secure_password \
    --database test_db \
    "DELETE FROM users WHERE name = 'Bob';"
```

### Step 7: Clean Up by Dropping the Table

After our operations are complete, we can clean up by dropping the `users` table.

```bash
python -m rhosocial.activerecord.backend.impl.postgres \
    --host your_postgres_host --user your_postgres_user --password your_secure_password \
    --database test_db \
    "DROP TABLE users;"
```

### Step 8: Final Cleanup by Dropping the Database

Finally, we remove the test database itself.

```bash
python -m rhosocial.activerecord.backend.impl.postgres \
    --host your_postgres_host --user your_postgres_user --password your_secure_password \
    --database postgres \
    "DROP DATABASE test_db;"
```