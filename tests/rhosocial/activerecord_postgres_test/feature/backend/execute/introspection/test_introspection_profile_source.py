# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/introspection/test_introspection_profile_source.py
"""The provenance of a profile, and the default both twins offer.

``ProfileSource`` is core's closed set for where a profile number came from.
PostgreSQL is the engine that reads an estimate out of a catalogue, so it is
the one that has to name the members rather than spell their words: the same
``null_fraction`` means something different here than on an engine that counted
the rows, and the field is what carries that.

The second half is the value-sample limit. This introspector overrides core's
``sample_column_values`` so it can read ``pg_stats`` instead of scanning the
rows, and an override that dropped the parameter's default would make
``introspector.sample_column_values(table, column)`` fail here while it
succeeds on every backend that inherits core's. Both twins therefore keep the
default, and both point at core's one definition of it; the assertion is by
identity, because a second ``100`` compares equal and is still a second place
to move.
"""

import inspect

from rhosocial.activerecord.backend.introspection.defaults import VALUE_SAMPLE_LIMIT
from rhosocial.activerecord.backend.introspection.types import ProfileSource
from rhosocial.activerecord.backend.impl.postgres.introspection.introspector import (
    AsyncPostgreSQLIntrospector,
    PostgreSQLIntrospector,
    PostgreSQLIntrospectorMixin,
)


class _MixinOnly(PostgreSQLIntrospectorMixin):
    """The profile parser needs no backend; it reads the rows it is given.

    The parser is a method because it shares the static helpers below it in the
    mixin, not because it reads a connection.
    """


class TestTheProfileSourceIsAMember:
    def test_a_measured_vintage_is_an_estimate(self):
        profile = _MixinOnly().parse_column_data_profiles(
            [{"column_name": "status", "last_analyze": "2026-01-01 00:00:00"}],
            "public",
            "orders",
        )[0]

        assert profile.source is ProfileSource.ESTIMATE
        assert profile.source == "estimate"

    def test_an_unknown_vintage_is_unknown(self):
        profile = _MixinOnly().parse_column_data_profiles(
            [{"column_name": "status"}],
            "public",
            "orders",
        )[0]

        assert profile.source is ProfileSource.UNKNOWN
        assert profile.source == "unknown"


class TestTheValueSampleLimitHasOneOwner:
    def test_both_twins_offer_core_default(self):
        sync_default = (
            inspect.signature(PostgreSQLIntrospector.sample_column_values)
            .parameters["limit"]
            .default
        )
        async_default = (
            inspect.signature(AsyncPostgreSQLIntrospector.sample_column_values)
            .parameters["limit"]
            .default
        )

        assert sync_default is VALUE_SAMPLE_LIMIT
        assert async_default is VALUE_SAMPLE_LIMIT
