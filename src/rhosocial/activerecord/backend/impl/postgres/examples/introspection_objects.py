# src/rhosocial/activerecord/backend/impl/postgres/examples/introspection_objects.py
"""
Schema Object Introspection: sequences and materialized views.

Materialized views and sequences are PostgreSQL objects the core
introspection surface does not model on its own, so this backend answers
for them with two dedicated entry points:

* ``introspector.list_materialized_views()`` /
  ``introspector.get_materialized_view_info()`` -- ``pg_matviews``.
* ``introspector.list_sequences()`` /
  ``introspector.get_sequence_info()`` -- ``information_schema.sequences``
  left-joined to ``pg_sequences`` for the runtime state (``cache_size``,
  ``last_value``) the standard view omits.

This example demonstrates:
1. Creating a sequence and a materialized view to read back
2. Listing every sequence in the schema and reading one in detail
3. Listing the materialized views and reading one in detail
4. What the introspection cache actually saves, measured: the questions
   asked of the server on a cold read, on a warm one, and the wall time
   for each

Requires a reachable PostgreSQL server (9.3+ for materialized views, 10+
for the ``pg_sequences`` columns). Configure the connection with the same
environment variables the test suite uses:

    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE,
    POSTGRES_USER, POSTGRES_PASSWORD
"""

import os
import sys
import time

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# The examples directory holds a ``types`` package (the PostgreSQL type
# examples). Running this file directly puts that directory first on
# ``sys.path``, where its ``types`` shadows the standard library module
# every ``import functools`` needs. Take the directory back off the path
# so the standard library wins; nothing here imports from it.
if _SCRIPT_DIR in sys.path:
    sys.path.remove(_SCRIPT_DIR)

from rhosocial.activerecord.backend.errors import ConnectionError
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

# Objects this example creates and owns; dropped again on the way out.
DEMO_TABLE = "arx_demo_sales"
DEMO_SEQUENCE = "arx_demo_order_id"
DEMO_VIEW = "arx_demo_sales_summary"


def _connect():
    """Connect to the server described by the environment."""
    config = PostgresConnectionConfig(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        database=os.getenv('POSTGRES_DATABASE', 'test'),
        username=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', ''),
    )
    backend = PostgresBackend(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()
    return backend


def _create_demo_objects(backend):
    """A sequence with non-default settings, and a populated view over a table."""
    backend.execute(f"DROP MATERIALIZED VIEW IF EXISTS {DEMO_VIEW}")
    backend.execute(f"DROP TABLE IF EXISTS {DEMO_TABLE}")
    backend.execute(f"DROP SEQUENCE IF EXISTS {DEMO_SEQUENCE}")
    backend.execute(
        f"CREATE SEQUENCE {DEMO_SEQUENCE} START 100 INCREMENT 5 "
        "MINVALUE 1 MAXVALUE 100000 CACHE 4 NO CYCLE"
    )
    backend.execute(f"SELECT nextval('{DEMO_SEQUENCE}')")
    backend.execute(
        f"CREATE TABLE {DEMO_TABLE} (id INTEGER PRIMARY KEY, amount INTEGER)"
    )
    backend.execute(
        f"INSERT INTO {DEMO_TABLE} (id, amount) VALUES (1, 250), (2, 900)"
    )
    backend.execute(
        f"CREATE MATERIALIZED VIEW {DEMO_VIEW} AS "
        f"SELECT id, amount FROM {DEMO_TABLE}"
    )


def _drop_demo_objects(backend):
    """Remove the objects this example created, in dependency order."""
    backend.execute(f"DROP MATERIALIZED VIEW IF EXISTS {DEMO_VIEW}")
    backend.execute(f"DROP TABLE IF EXISTS {DEMO_TABLE}")
    backend.execute(f"DROP SEQUENCE IF EXISTS {DEMO_SEQUENCE}")


def _print_sequences(backend):
    """Every sequence in the schema, then one read on its own."""
    introspector = backend.introspector
    introspector.clear_cache()

    sequences = introspector.list_sequences()
    print(f"Sequences in this schema: {len(sequences)}")
    for seq in sequences:
        print(
            f"  {seq.schema}.{seq.name} type={seq.data_type} "
            f"start={seq.start_value} increment={seq.increment} "
            f"cycle={seq.cycle} cache_size={seq.cache_size} "
            f"last_value={seq.last_value}"
        )

    mine = introspector.get_sequence_info(DEMO_SEQUENCE)
    if mine is None:
        # The example must not report a reading it did not take.
        raise RuntimeError(f"sequence {DEMO_SEQUENCE} disappeared before it could be read")
    print(f"Read back {mine.schema}.{mine.name}: start={mine.start_value} "
          f"increment={mine.increment} max={mine.maximum_value} "
          f"last_value={mine.last_value}")

    # A server older than 10 has no pg_sequences; the same question then
    # answers the parts of the row the standard view does carry.
    narrow = introspector.get_sequence_info(DEMO_SEQUENCE, include_pg_sequences=False)
    print(f"Without pg_sequences: cache_size={narrow.cache_size} "
          f"last_value={narrow.last_value}")

    missing = introspector.get_sequence_info("arx_no_such_sequence")
    print(f"Unknown sequence reads as: {missing}")


def _print_materialized_views(backend):
    """Every materialized view in the schema, then one read on its own."""
    introspector = backend.introspector
    introspector.clear_cache()

    views = introspector.list_materialized_views()
    print(f"Materialized views in this schema: {len(views)}")
    for view in views:
        print(
            f"  {view.schema}.{view.name} "
            f"populated={view.extra.get('is_populated')} "
            f"unique_index={view.extra.get('has_unique_index')}"
        )

    mine = introspector.get_materialized_view_info(DEMO_VIEW)
    if mine is None:
        raise RuntimeError(f"materialized view {DEMO_VIEW} disappeared before it could be read")
    definition = (mine.definition or "").strip().replace("\n", " ")
    print(f"Read back {mine.schema}.{mine.name}: {definition[:70]}")


class _QuestionCounter:
    """Wrap an introspector executor and count the questions it is asked.

    The introspector is the only object that asks the server catalog
    questions here, so counting its executor counts them exactly: one
    ``execute`` per question, and a cache hit asks none at all.
    """

    def __init__(self, executor):
        self._executor = executor
        self.questions = 0
        self.statements = []

    def execute(self, sql, params=()):
        self.questions += 1
        self.statements.append(sql)
        return self._executor.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._executor, name)


def _measure_cache(backend):
    """Cold versus warm: how many questions, and how long they took."""
    introspector = backend.introspector
    introspector.clear_cache()

    counter = _QuestionCounter(introspector._executor)
    introspector._executor = counter

    started = time.perf_counter()
    introspector.list_sequences()
    introspector.list_materialized_views()
    cold_seconds = time.perf_counter() - started
    cold_questions = counter.questions

    started = time.perf_counter()
    introspector.list_sequences()
    introspector.list_materialized_views()
    warm_seconds = time.perf_counter() - started
    warm_questions = counter.questions - cold_questions

    introspector._executor = counter._executor
    introspector.clear_cache()

    print("Cache effect, measured:")
    print(f"  cold: {cold_questions} question(s) in {cold_seconds * 1000:.1f} ms")
    print(f"  warm: {warm_questions} question(s) in {warm_seconds * 1000:.1f} ms")
    if warm_questions:
        # The warm read must be free: a question here means the cache missed.
        print("  the warm read still asked the server -- cache is not working")
    else:
        print("  the warm read cost zero questions, as a cache hit should")


def main():
    print("Connecting with POSTGRES_HOST/POSTGRES_PORT/POSTGRES_DATABASE/"
          "POSTGRES_USER/POSTGRES_PASSWORD ...")
    try:
        backend = _connect()
    except (ConnectionError, OSError) as exc:
        print()
        print("Could not reach a PostgreSQL server, so no readings are shown.")
        print(f"  reported: {type(exc).__name__}: {exc}")
        print()
        print("Set the connection environment variables and re-run:")
        print("  POSTGRES_HOST      server host      (now: "
              f"{os.getenv('POSTGRES_HOST', 'localhost')})")
        print("  POSTGRES_PORT      server port      (now: "
              f"{os.getenv('POSTGRES_PORT', '5432')})")
        print("  POSTGRES_DATABASE  database name    (now: "
              f"{os.getenv('POSTGRES_DATABASE', 'test')})")
        print("  POSTGRES_USER       user            (now: "
              f"{os.getenv('POSTGRES_USER', 'postgres')})")
        print("  POSTGRES_PASSWORD  password")
        return 1

    try:
        version = backend.get_server_version()
        print(f"Connected to PostgreSQL {version}")
        _create_demo_objects(backend)
        print()
        _print_sequences(backend)
        print()
        _print_materialized_views(backend)
        print()
        _measure_cache(backend)
    finally:
        try:
            _drop_demo_objects(backend)
        except Exception:
            pass
        backend.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
