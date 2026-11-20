# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/helpers.py
"""Small SQL helpers shared by the native type test modules."""

from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

#: INSERT ... RETURNING must ask for result processing explicitly: a DML
#: statement does not run the result pipeline by default.
RETURNING_OPTIONS = ExecutionOptions(
    stmt_type=StatementType.DML,
    process_result_set=True,
)


def insert_row(backend, table, values, casts=None):
    """Insert one row and return its ``id``.

    ``values`` keeps its insertion order, so callers can line the columns up
    with the SQL they assert against.  ``casts`` maps a column name to the
    PostgreSQL cast appended to its placeholder, for the few types whose text
    form is the only unambiguous input (``xid``, ``bit(n)``, ...).
    """
    casts = casts or {}
    columns = ", ".join(values)
    placeholders = ", ".join(
        "%s" + ("::" + casts[name] if name in casts else "") for name in values
    )
    sql = "INSERT INTO {} ({}) VALUES ({}) RETURNING id".format(
        table, columns, placeholders
    )
    result = backend.execute(sql, tuple(values.values()), options=RETURNING_OPTIONS)
    return result.data[0]["id"]


def insert_sql(backend, sql):
    """Run a native ``INSERT ... RETURNING id`` and return the new row id.

    Used by the S3 cases, which deliberately bypass the adapter.
    """
    result = backend.execute(sql, options=RETURNING_OPTIONS)
    return result.data[0]["id"]


def read_raw(backend, table, column, row_id, expression=None):
    """Read the server's own text for one column of one row."""
    if expression is None:
        expression = "{}::text".format(column)
    row = backend.fetch_one(
        "SELECT {} AS raw FROM {} WHERE id = %s".format(expression, table),
        (row_id,),
    )
    return row["raw"]


def read_value(backend, table, column, row_id):
    """Read the driver-decoded value for one column of one row."""
    row = backend.fetch_one(
        "SELECT {} AS value FROM {} WHERE id = %s".format(column, table),
        (row_id,),
    )
    return row["value"]


def update_cell(backend, table, column, row_id, value, cast=None):
    """Write one column, optionally through an explicit PostgreSQL cast."""
    suffix = "" if cast is None else "::" + cast
    backend.execute(
        "UPDATE {} SET {} = %s{} WHERE id = %s".format(table, column, suffix),
        (value, row_id),
    )
