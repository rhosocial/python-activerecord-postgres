# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/conftest.py
"""Group the backend-owned execute tests by their directory-owned database.

The root ``backend/conftest.py`` declares ``parallel=False`` so pure-render
scopes stay ungrouped. Every directory under ``execute/`` however drives a
live server and owns its own database (named from the directory path), so its
tests must stay on one worker per scenario. Declaring ``parallel=True`` here
makes the testsuite plugin pin each such directory to one worker per scenario;
the group key is derived from the test file's own directory, so different
execute directories still run in parallel.
"""
from rhosocial.activerecord.testsuite.pytest_plugin import Declaration


DECLARATION = Declaration(
    parallel=True,
    purpose="PostgreSQL execute tests against each directory-owned database.",
    passes_means="the live-database contract of the execute directory holds on PostgreSQL",
    fails_means="the directory-owned database is corrupted or a backend behavior is wrong",
)
