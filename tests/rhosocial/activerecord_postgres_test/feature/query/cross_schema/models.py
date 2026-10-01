# tests/rhosocial/activerecord_postgres_test/feature/query/cross_schema/models.py
"""PostgreSQL-local models for cross-schema tests.

Kept in this repository rather than the shared testsuite: schema qualification
on PostgreSQL has its own rules (three-part references for an unaliased range,
alias-only references once a range is aliased) that other backends do not share.
MySQL/MariaDB qualify by database, BigQuery never qualifies a column, Snowflake
nests schema inside a database. A shared contract would have to assert the
lowest common denominator and stop catching the dialect-specific mistakes.
"""

from rhosocial.activerecord.testsuite.feature.query.fixtures.async_models import (
    AsyncOrder,
)
from rhosocial.activerecord.testsuite.feature.query.fixtures.models import Order

# Non-default namespace provisioned by this repository's provider.
SCHEMA_A = "ar_crm"


class MixedSchemaOrder(Order):
    """The ``orders`` fixture, relocated into ``ar_crm``.

    The table DDL is identical to the default-schema ``orders`` -- the
    provider provisions the same shape in both namespaces -- so only
    ``__schema_name__`` differs. Overriding ``schema_name()`` would be
    equivalent; the attribute keeps the intent declarative.
    """

    __schema_name__ = SCHEMA_A


class AsyncMixedSchemaOrder(AsyncOrder):
    """Async variant of :class:`MixedSchemaOrder`."""

    __schema_name__ = SCHEMA_A
