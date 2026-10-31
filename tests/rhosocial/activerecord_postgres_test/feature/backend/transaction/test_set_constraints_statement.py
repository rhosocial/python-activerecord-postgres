# tests/rhosocial/activerecord_postgres_test/feature/backend/transaction/test_set_constraints_statement.py
"""The PostgreSQL ``SET CONSTRAINTS`` statement node (SQL generation only).

``defer_constraint`` used to spell the statement at the call site with an
f-string that interpolated the constraint name raw, so a name carrying a quote
either broke the statement or escaped it. The statement now belongs to
``PostgresSetConstraintsStatement``, and the name is rendered as the identifier
PostgreSQL parses it as.
"""

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.transaction import (
    PostgresSetConstraintsStatement,
)


def _dialect():
    return PostgresDialect(version=(16, 0, 0))


class TestSetConstraintsStatement:
    """The statement's rendered text and the safety of the name it carries."""

    def test_renders_the_deferred_clause_for_the_named_constraint(self):
        sql, params = PostgresSetConstraintsStatement(
            _dialect(), constraint_name="deferred_fk"
        ).to_sql()
        assert sql == 'SET CONSTRAINTS "deferred_fk" DEFERRED'
        assert params == ()

    def test_name_is_rendered_as_an_identifier_not_as_text(self):
        # The server parses the constraint name, so no bind parameter can stand
        # in for it; the name goes through identifier quoting instead of being
        # written into the statement as it stood.
        sql, _ = PostgresSetConstraintsStatement(
            _dialect(), constraint_name='de"fked fk'
        ).to_sql()
        assert sql == 'SET CONSTRAINTS "de""fked fk" DEFERRED'
