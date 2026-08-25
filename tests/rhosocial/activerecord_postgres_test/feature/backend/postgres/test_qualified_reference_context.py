# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_qualified_reference_context.py
"""Column qualifier resolution under JOINs (regression tests).

History: the query layer used to force ``alias=alias or table_name`` on every
JOIN range while field accessors baked ``"schema"."table"."column"`` into
Column expressions at access time. PostgreSQL forbids schema-qualified
references to aliased ranges, so plain join queries failed with
``invalid reference to FROM-clause entry``.

The forced aliasing was removed (JoinQueryMixin / AsyncJoinQueryMixin); these
tests pin down the fixed behaviour:

* R1  plain field accessors under JOIN must execute
* R2a mixed aliased/plain accessors must execute
* R3  pre-built predicates remain reusable across plain/join contexts
* R4  dynamic ``__table_name__`` / ``__schema_name__``: the supported usage —
      change the override, then build expressions — produces fully consistent
      DQL across FROM / WHERE / SELECT (qualifiers are captured when column
      expressions are constructed; rebuild conditions after a change)
* Control: fully alias-consistent accessors keep working, and the generated
  FROM carries no implicit aliases.
"""

from typing import ClassVar, Optional

import pytest

from rhosocial.activerecord.backend.impl.postgres import (
    PostgresBackend,
    PostgresConnectionConfig,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.base.field_proxy import FieldProxy
from rhosocial.activerecord.model import ActiveRecord

RISK_SCHEMA = "tenant_qualified_ref"


class Author(ActiveRecord):
    __table_name__ = "authors"
    __schema_name__ = RISK_SCHEMA
    __primary_key__ = "id"
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    name: str
    active: Optional[bool] = None


class Book(ActiveRecord):
    __table_name__ = "books"
    __schema_name__ = RISK_SCHEMA
    __primary_key__ = "id"
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    title: str
    author_id: Optional[int] = None


@pytest.fixture
def risk_env(postgres_backend_single):
    """Provision schema/tables, bind models to the scenario config, capture SQL.

    Yields a namespace with:
        backend   -- the fixture backend (raw SQL setup/teardown)
        config    -- connection config shared by the models
        captured  -- list of SQL strings executed by the models' backends
    """
    backend = postgres_backend_single
    ddl = ExecutionOptions(stmt_type=StatementType.DDL)

    backend.execute(f'DROP SCHEMA IF EXISTS "{RISK_SCHEMA}" CASCADE', options=ddl)
    backend.execute(f'CREATE SCHEMA "{RISK_SCHEMA}"', options=ddl)
    for table in ("authors", "authors_archive", "books"):
        if table == "books":
            cols = "id SERIAL PRIMARY KEY, title TEXT, author_id INTEGER"
        else:
            cols = "id SERIAL PRIMARY KEY, name TEXT, active BOOLEAN DEFAULT TRUE"
        backend.execute(f'CREATE TABLE "{RISK_SCHEMA}".{table} ({cols})', options=ddl)
    backend.execute(
        f"INSERT INTO \"{RISK_SCHEMA}\".authors (name) VALUES ('a1'), ('a2')",
        options=ExecutionOptions(stmt_type=StatementType.DML),
    )
    backend.execute(
        f"INSERT INTO \"{RISK_SCHEMA}\".authors_archive (name) VALUES ('old1')",
        options=ExecutionOptions(stmt_type=StatementType.DML),
    )

    # A second schema for dynamic __schema_name__ switching tests.
    backend.execute(f'DROP SCHEMA IF EXISTS "{RISK_SCHEMA}_alt" CASCADE', options=ddl)
    backend.execute(f'CREATE SCHEMA "{RISK_SCHEMA}_alt"', options=ddl)
    backend.execute(
        f'CREATE TABLE "{RISK_SCHEMA}_alt".authors '
        f'(id SERIAL PRIMARY KEY, name TEXT, active BOOLEAN DEFAULT TRUE)',
        options=ddl,
    )
    backend.execute(
        f"INSERT INTO \"{RISK_SCHEMA}_alt\".authors (name) VALUES ('alt1')",
        options=ExecutionOptions(stmt_type=StatementType.DML),
    )

    config: PostgresConnectionConfig = backend.config
    captured: list = []
    original_execute = PostgresBackend.execute

    def spying_execute(self, sql, params=None, **kwargs):
        captured.append(sql)
        return original_execute(self, sql, params, **kwargs)

    PostgresBackend.execute = spying_execute
    try:
        Author.configure(config, PostgresBackend)
        Book.configure(config, PostgresBackend)
        yield type("Env", (), {"backend": backend, "config": config, "captured": captured})
    finally:
        PostgresBackend.execute = original_execute
        Author.__table_name__ = "authors"
        Author.__schema_name__ = RISK_SCHEMA
        backend.execute(f'DROP SCHEMA IF EXISTS "{RISK_SCHEMA}" CASCADE', options=ddl)
        backend.execute(f'DROP SCHEMA IF EXISTS "{RISK_SCHEMA}_alt" CASCADE', options=ddl)


class TestQualifiedReferenceContext:
    """See module docstring for the risk catalogue."""

    def test_join_with_plain_accessors(self, risk_env):
        """R1: natural join form with unaliased field accessors."""
        rows = (
            Author.query()
            .join(Book, on=Author.c.id == Book.c.author_id)
            .select(Author.c.name)
            .all()
        )
        assert isinstance(rows, list)

    def test_join_with_partial_aliases(self, risk_env):
        """R2a: mixing a table-aliased accessor with plain accessors."""
        rows = (
            Author.query()
            .join(Book, on=Author.c.id == Book.c.with_table_alias("books").author_id)
            .select(Author.c.name.as_("n"))
            .all()
        )
        assert isinstance(rows, list)

    def test_prebuilt_predicate_reuse_under_join(self, risk_env):
        """R3: predicates stay valid when reused across query contexts."""
        predicate = Author.c.active == True  # noqa: E712
        assert len(Author.query().where(predicate).all()) == 2
        rows = (
            Author.query()
            .join(Book, on=Author.c.id == Book.c.author_id)
            .where(predicate)
            .all()
        )
        assert isinstance(rows, list)

    def test_explicit_alias_join(self, risk_env):
        """Explicit join aliases keep working via matching accessors."""
        rows = (
            Author.query()
            .join(Book, on=Author.c.id == Book.c.with_table_alias("b").author_id, alias="b")
            .select(Author.c.name.as_("n"))
            .all()
        )
        assert isinstance(rows, list)

    def test_dynamic_table_name_full_dql_consistency(self, risk_env):
        """R4: dynamic __table_name__ used the supported way.

        Change the override first, then build expressions — field proxies
        pick up the new name and FROM / WHERE / SELECT all stay consistent.
        """
        Author.__table_name__ = "authors_archive"
        try:
            rows = Author.query().where(Author.c.active == True).all()  # noqa: E712
            assert len(rows) == 1
            select_sql = next(
                (s for s in risk_env.captured
                 if s.startswith("SELECT") and "authors_archive" in s),
                "",
            )
            assert 'FROM "tenant_qualified_ref"."authors_archive"' in select_sql
            assert '"authors_archive"."active"' in select_sql
        finally:
            Author.__table_name__ = "authors"

    def test_dynamic_schema_switch_full_dql_consistency(self, risk_env):
        """R4: same contract for dynamic __schema_name__ (tenant switching)."""
        Author.__schema_name__ = f"{RISK_SCHEMA}_alt"
        try:
            rows = Author.query().where(Author.c.name == "alt1").all()
            assert len(rows) == 1 and rows[0].name == "alt1"
            select_sql = next(
                (s for s in risk_env.captured if s.startswith("SELECT") and "alt" in s),
                "",
            )
            assert f'FROM "{RISK_SCHEMA}_alt"."authors"' in select_sql
        finally:
            Author.__schema_name__ = RISK_SCHEMA

    def test_baseline_plain_query_with_predicate(self, risk_env):
        """Sanity: prebuilt predicates work in their construction context."""
        predicate = Author.c.active == True  # noqa: E712
        rows = Author.query().where(predicate).all()
        assert len(rows) == 2

    def test_control_fully_alias_consistent(self, risk_env):
        """Control: FROM ranges carry no implicit aliases; references resolve."""
        rows = (
            Author.query()
            .join(Book, on=Author.c.with_table_alias("authors").id
                 == Book.c.with_table_alias("books").author_id)
            .select(Author.c.with_table_alias("authors").name.as_("n"))
            .all()
        )
        assert isinstance(rows, list)
        select_sql = next((s for s in risk_env.captured if s.startswith("SELECT")), "")
        # Schema-qualified FROM ranges, no forced AS aliases ...
        assert 'FROM "tenant_qualified_ref"."authors"' in select_sql
        assert 'JOIN "tenant_qualified_ref"."books"' in select_sql
        assert " AS " not in select_sql.split("FROM", 1)[1]
        # ... and table-name-qualified column references.
        assert '"authors"."id"' in select_sql and '"books"."author_id"' in select_sql
