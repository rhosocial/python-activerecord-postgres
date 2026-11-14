# tests/rhosocial/activerecord_postgres_test/feature/basic/core/conftest.py
"""The mapped-model fixture group this directory owns.

The directory runs the shared ``basic`` mapped-model tests; the models they
request (``MappedUser``, ``MappedPost``, ``MappedComment``) read and write the
topic's ``users``, ``posts`` and ``comments`` tables, so the group is exactly
those three.
"""

from rhosocial.activerecord.testsuite.feature.basic.fixtures import tables

GROUP_TABLES = ("users", "posts", "comments")


def ensure_database(environment):
    """Create the directory's database, or clear it when it already exists."""
    environment.reset_database()


def prepare(environment):
    """Create the group's tables before every test."""
    for name in GROUP_TABLES:
        environment.execute(tables.BASIC_TABLES[name](environment.dialect))


def teardown(environment):
    """Drop the group's tables after every test."""
    for name in reversed(GROUP_TABLES):
        environment.execute(tables.drop_table(environment.dialect, name))
