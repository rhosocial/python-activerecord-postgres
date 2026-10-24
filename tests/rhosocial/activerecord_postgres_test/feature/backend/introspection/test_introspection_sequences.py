# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_sequences.py
"""
Tests for PostgreSQL sequence, materialized view and extension introspection.

The sequences and the materialized view these tests read are created by the
``backend_with_sequence`` fixture (see conftest.py), so every assertion here is
taken against a live server rather than a fabricated row.
"""

import pytest

from rhosocial.activerecord.backend.impl.postgres.introspection.types import SequenceInfo
from rhosocial.activerecord.backend.introspection.status import ExtensionInfo
from rhosocial.activerecord.backend.introspection.types import IntrospectionViewInfo


class TestListSequences:
    """Tests for list_sequences method."""

    def test_list_sequences_returns_list_of_typed_rows(self, backend_with_sequence):
        """Every row is a SequenceInfo, and the live schema is not empty."""
        sequences = backend_with_sequence.introspector.list_sequences()

        assert isinstance(sequences, list)
        assert len(sequences) > 0
        for seq in sequences:
            assert isinstance(seq, SequenceInfo)
            assert isinstance(seq.name, str) and seq.name
            assert isinstance(seq.schema, str) and seq.schema

    def test_list_sequences_includes_the_created_sequence(self, backend_with_sequence):
        sequences = backend_with_sequence.introspector.list_sequences()
        names = [s.name for s in sequences]

        assert "seq_demo_id" in names

    def test_list_sequences_reports_created_attributes(self, backend_with_sequence):
        """The row mirrors the CREATE SEQUENCE the fixture issued."""
        introspector = backend_with_sequence.introspector
        sequences = introspector.list_sequences()
        demo = next(s for s in sequences if s.name == "seq_demo_id")

        assert demo.schema == "public"
        assert demo.start_value == 7
        assert demo.increment == 3
        assert demo.minimum_value == 2
        assert demo.maximum_value == 1000
        assert demo.cycle is False
        # Drawn once by the fixture, so a value exists. With CACHE 5 the
        # server writes the end of the block it reserved ahead of what it
        # has handed out, which is why the assertion is "at least" and not
        # an equality -- the value returned by nextval was 7.
        #
        # ``pg_sequences`` (PostgreSQL 10+) is where last_value is read from,
        # so on an older server the value is absent rather than present.
        if introspector._server_has_pg_sequences():
            assert demo.last_value is not None
            assert demo.last_value >= 7

    def test_list_sequences_is_sorted_by_name(self, backend_with_sequence):
        sequences = backend_with_sequence.introspector.list_sequences()
        names = [s.name for s in sequences]

        assert names == sorted(names)

    def test_list_sequences_honors_schema_argument(self, backend_with_sequence):
        sequences = backend_with_sequence.introspector.list_sequences(schema="public")

        assert [s.schema for s in sequences] == ["public"] * len(sequences)
        assert "seq_demo_id" in [s.name for s in sequences]

    def test_list_sequences_unknown_schema_is_empty(self, backend_with_sequence):
        sequences = backend_with_sequence.introspector.list_sequences(
            schema="arx_no_such_schema"
        )

        assert sequences == []

    def test_list_sequences_caching(self, backend_with_sequence):
        first = backend_with_sequence.introspector.list_sequences()
        second = backend_with_sequence.introspector.list_sequences()

        assert first is second


class TestGetSequenceInfo:
    """Tests for get_sequence_info method."""

    def test_get_sequence_info_existing(self, backend_with_sequence):
        introspector = backend_with_sequence.introspector
        info = introspector.get_sequence_info("seq_demo_id")

        assert isinstance(info, SequenceInfo)
        assert info.name == "seq_demo_id"
        assert info.schema == "public"
        assert info.increment == 3
        # ``cache_size`` and ``last_value`` come from ``pg_sequences``, which
        # arrived in PostgreSQL 10. On an older server the introspector asks
        # the narrower question and those two columns are legitimately absent,
        # so the assertion rides the same capability gate the code rides
        # instead of assuming every server has the view.
        if introspector._server_has_pg_sequences():
            assert info.cache_size == 5
        else:
            assert info.cache_size is None

    def test_get_sequence_info_nonexistent(self, backend_with_sequence):
        assert backend_with_sequence.introspector.get_sequence_info("arx_no_seq") is None

    def test_get_sequence_info_unused_sequence_has_no_last_value(
        self, backend_with_sequence
    ):
        """A sequence nobody drew from reports last_value NULL, not an error."""
        introspector = backend_with_sequence.introspector
        info = introspector.get_sequence_info("seq_demo_untouched")

        assert info is not None
        if introspector._server_has_pg_sequences():
            assert info.last_value is None

    def test_get_sequence_info_narrow_query_omits_pg_columns(
        self, backend_with_sequence
    ):
        """Without pg_sequences the two runtime columns are absent, not wrong."""
        narrow = backend_with_sequence.introspector.get_sequence_info(
            "seq_demo_id", include_pg_sequences=False
        )

        assert narrow is not None
        assert narrow.increment == 3
        assert narrow.cache_size is None
        assert narrow.last_value is None

    def test_get_sequence_info_caching(self, backend_with_sequence):
        first = backend_with_sequence.introspector.get_sequence_info("seq_demo_id")
        second = backend_with_sequence.introspector.get_sequence_info("seq_demo_id")

        assert first is second

    def test_sequence_exists(self, backend_with_sequence):
        introspector = backend_with_sequence.introspector

        assert introspector.sequence_exists("seq_demo_id") is True
        assert introspector.sequence_exists("arx_no_seq") is False


class TestMaterializedViewsLive:
    """Live coverage for the typed materialized view introspection surface."""

    def test_list_materialized_views_includes_the_created_view(self, backend_with_sequence):
        views = backend_with_sequence.introspector.list_materialized_views()
        names = [v.name for v in views]

        assert isinstance(views, list)
        assert "seq_demo_mv" in names

    def test_materialized_view_reports_population_state(self, backend_with_sequence):
        views = backend_with_sequence.introspector.list_materialized_views()
        view = next(v for v in views if v.name == "seq_demo_mv")

        assert isinstance(view, IntrospectionViewInfo)
        assert view.extra["is_materialized"] is True
        assert view.extra["is_populated"] is True
        # No unique index was created, so a CONCURRENTLY refresh is refused.
        assert view.extra["has_unique_index"] is False

    def test_get_materialized_view_info_existing(self, backend_with_sequence):
        info = backend_with_sequence.introspector.get_materialized_view_info("seq_demo_mv")

        assert info is not None
        assert info.name == "seq_demo_mv"
        assert "SELECT" in (info.definition or "").upper()

    def test_get_materialized_view_info_nonexistent(self, backend_with_sequence):
        assert (
            backend_with_sequence.introspector.get_materialized_view_info("arx_no_mv")
            is None
        )

    def test_materialized_views_are_not_listed_as_views(self, backend_with_sequence):
        """The two families answer different questions."""
        views = backend_with_sequence.introspector.list_views()
        assert "seq_demo_mv" not in [v.name for v in views]


class TestExtensionIntrospection:
    """Tests for the schema-level extension family."""

    def test_list_extensions_returns_typed_rows(self, postgres_backend_single):
        extensions = postgres_backend_single.introspector.list_extensions()

        assert isinstance(extensions, list)
        assert len(extensions) > 0
        for ext in extensions:
            assert isinstance(ext, ExtensionInfo)
            assert ext.name
        # plpgsql is created by initdb, so a real PostgreSQL has it.
        assert "plpgsql" in [e.name for e in extensions]

    def test_list_extensions_is_sorted(self, postgres_backend_single):
        names = [e.name for e in postgres_backend_single.introspector.list_extensions()]

        assert names == sorted(names)

    def test_get_extension_info_existing(self, postgres_backend_single):
        info = postgres_backend_single.introspector.get_extension_info("plpgsql")

        assert isinstance(info, ExtensionInfo)
        assert info.name == "plpgsql"
        assert info.version is not None
        assert info.schema

    def test_get_extension_info_nonexistent(self, postgres_backend_single):
        assert (
            postgres_backend_single.introspector.get_extension_info("arx_no_extension")
            is None
        )

    def test_extension_exists(self, postgres_backend_single):
        introspector = postgres_backend_single.introspector

        assert introspector.extension_exists("plpgsql") is True
        assert introspector.extension_exists("arx_no_extension") is False

    def test_extension_list_is_cached(self, postgres_backend_single):
        first = postgres_backend_single.introspector.list_extensions()
        second = postgres_backend_single.introspector.list_extensions()

        assert first is second


class TestAsyncSequenceIntrospection:
    """Async tests for sequence introspection."""

    @pytest.mark.asyncio
    async def test_async_list_sequences(self, async_backend_with_sequence):
        sequences = await async_backend_with_sequence.introspector.list_sequences()

        assert isinstance(sequences, list)
        assert "seq_demo_id" in [s.name for s in sequences]

    @pytest.mark.asyncio
    async def test_async_get_sequence_info(self, async_backend_with_sequence):
        introspector = async_backend_with_sequence.introspector
        info = await introspector.get_sequence_info("seq_demo_id")

        assert isinstance(info, SequenceInfo)
        assert info.increment == 3
        if introspector._server_has_pg_sequences():
            assert info.cache_size == 5
        else:
            assert info.cache_size is None

    @pytest.mark.asyncio
    async def test_async_sequence_exists(self, async_backend_with_sequence):
        introspector = async_backend_with_sequence.introspector

        assert await introspector.sequence_exists("seq_demo_id") is True
        assert await introspector.sequence_exists("arx_no_seq") is False

    @pytest.mark.asyncio
    async def test_async_materialized_views(self, async_backend_with_sequence):
        views = await async_backend_with_sequence.introspector.list_materialized_views()
        info = await async_backend_with_sequence.introspector.get_materialized_view_info(
            "seq_demo_mv"
        )

        assert "seq_demo_mv" in [v.name for v in views]
        assert info is not None
        assert info.extra["is_populated"] is True

    @pytest.mark.asyncio
    async def test_async_extensions(self, async_postgres_backend_single):
        extensions = await async_postgres_backend_single.introspector.list_extensions()
        info = await async_postgres_backend_single.introspector.get_extension_info("plpgsql")

        assert isinstance(extensions, list)
        assert "plpgsql" in [e.name for e in extensions]
        assert isinstance(info, ExtensionInfo)
