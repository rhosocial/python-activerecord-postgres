# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/introspection/test_introspection_data_profile.py
"""The data-profile family, measured against a real server.

Rendering these statements proves only that they are well-formed. What needs
checking is whether PostgreSQL accepts them, and whether the conversions the
parser performs -- negative ``n_distinct``, the ``-1`` sentinels, min/max
merged out of the histogram and the most-common list -- land on values a hand
count agrees with. That is only answerable against a server.

The table is a thousand rows, built so every figure is arithmetic rather than
guesswork:

    id      integer primary key   i, unique             -> 1000 distinct, no nulls
    code    text                  "a"/"b"/"c" by i % 3  -> 3 distinct, no nulls
    amount  numeric               i, unique             -> 1000 distinct, no nulls
    note    text                  "x" when i % 5 == 0, "y" when i % 5 == 1,
                                  NULL otherwise          -> 2 distinct, 60% null

``ANALYZE`` is called explicitly because the family reads ``pg_stats``, and a
table with no statistics is precisely the case where the answer must be empty
rather than zero-filled.

The size matters: with a handful of rows PostgreSQL may keep every value as a
most-common value and no histogram at all, so a five-row table would test one
branch of the min/max merge. A thousand rows guarantees the histogram exists
for the unique columns and the MCV list exists for the low-cardinality one, so
both branches are exercised.

Coverage is measured against two small tables instead. Unlike the profile it
reads no statistics at all -- it counts the rows -- so there is nothing to
``ANALYZE`` and nothing to sample, and hand-sized domains are exactly what
makes the expected fractions checkable by eye: left ``{1,2,3,4}`` against right
``{2,3,4,5,6}`` must answer three quarters and three fifths.
"""

import pytest


TABLE = "intro_profile_orders"
CODES = ("a", "b", "c")


def _rows():
    rows = []
    for i in range(1, 1001):
        if i % 5 == 0:
            note = "x"
        elif i % 5 == 1:
            note = "y"
        else:
            note = None
        rows.append((i, CODES[i % 3], i, note))
    return rows


def _setup(backend):
    backend.execute(f"DROP TABLE IF EXISTS {TABLE}")
    backend.execute(
        f"CREATE TABLE {TABLE} ("
        "id integer primary key, code text, amount numeric, note text)"
    )
    backend.execute_many(
        f"INSERT INTO {TABLE} (id, code, amount, note) VALUES (%s, %s, %s, %s)",
        _rows(),
    )
    backend.execute(f"ANALYZE {TABLE}")


def _teardown(backend):
    backend.execute(f"DROP TABLE IF EXISTS {TABLE}")


class TestColumnDataProfile:
    """What the catalog says the rows hold."""

    def test_profile_matches_a_hand_count(self, postgres_backend):
        _setup(postgres_backend)
        try:
            profiles = {
                profile.column_name: profile
                for profile in postgres_backend.introspector.list_column_data_profiles(
                    TABLE
                )
            }
            assert set(profiles) == {"id", "code", "amount", "note"}

            # A unique key. ANALYZE stores this as n_distinct = -1 -- the
            # fraction form -- so a count of 1000 is the conversion working
            # rather than the raw value happening to read right.
            assert profiles["id"].null_fraction == 0.0
            assert profiles["id"].distinct_count == 1000
            assert int(profiles["id"].min_value) == 1
            assert int(profiles["id"].max_value) == 1000

            # Three values, none null: the low-cardinality case, where the
            # histogram is absent and the extremes can only come from the
            # most-common list.
            assert profiles["code"].null_fraction == 0.0
            assert profiles["code"].distinct_count == 3
            assert profiles["code"].min_value == "a"
            assert profiles["code"].max_value == "c"

            assert profiles["amount"].null_fraction == 0.0
            assert profiles["amount"].distinct_count == 1000

            # 600 of 1000 rows are NULL, so a profile that dropped or clamped
            # the figure would still look plausible.
            assert profiles["note"].null_fraction == pytest.approx(0.6)
            assert profiles["note"].distinct_count == 2
            assert profiles["note"].min_value == "x"
            assert profiles["note"].max_value == "y"
        finally:
            _teardown(postgres_backend)

    def test_every_profile_carries_its_provenance(self, postgres_backend):
        """An estimate carries its vintage, and an unknown one admits it.

        The invariant is the pair, not one value: ``ESTIMATE`` means the age is
        known and recorded, ``UNKNOWN`` means the figures are there but their
        age is not. Right after ``ANALYZE`` the second is what answers -- the
        statistics collector records ``last_analyze`` asynchronously, so the
        timestamp is usually still empty when the profile is read.
        """
        _setup(postgres_backend)
        try:
            profiles = postgres_backend.introspector.list_column_data_profiles(TABLE)
            assert profiles
            for profile in profiles:
                assert profile.source in ("estimate", "unknown")
                assert (profile.source == "estimate") == (
                    profile.collected_at is not None
                ), "ESTIMATE must carry a vintage, UNKNOWN must not claim one"
                assert profile.row_count == 1000
        finally:
            _teardown(postgres_backend)

    def test_unanalysed_table_answers_with_nothing(self, postgres_backend):
        """No statistics row is "not measured", never a zero-filled profile."""
        postgres_backend.execute(f"DROP TABLE IF EXISTS {TABLE}")
        postgres_backend.execute(f"CREATE TABLE {TABLE} (id integer)")
        try:
            assert (
                postgres_backend.introspector.list_column_data_profiles(TABLE) == []
            )
        finally:
            _teardown(postgres_backend)


class TestColumnValueSample:
    """The kept most-common list, paired with its frequencies."""

    def test_listing_is_ordered_by_frequency(self, postgres_backend):
        _setup(postgres_backend)
        try:
            samples = postgres_backend.introspector.sample_column_values(
                TABLE, "code", limit=10
            )
            # Three values with near-identical shares: ANALYZE orders the list
            # by frequency, so which one comes first is a tie-break detail.
            assert {item.value for item in samples} == set(CODES)
            for item in samples:
                assert item.frequency == pytest.approx(1 / 3, abs=0.02)
        finally:
            _teardown(postgres_backend)

    def test_limit_bounds_the_listing(self, postgres_backend):
        _setup(postgres_backend)
        try:
            assert (
                len(
                    postgres_backend.introspector.sample_column_values(
                        TABLE, "code", limit=2
                    )
                )
                == 2
            )
        finally:
            _teardown(postgres_backend)

    def test_null_is_not_a_most_common_value(self, postgres_backend):
        """PostgreSQL tracks NULLs in ``null_frac``, never in the value list.

        Worth pinning because the other implementation of this family does the
        opposite: SQLite aggregates the column, so its listing includes a NULL
        row. Same question, two engines, two shapes -- which is exactly why the
        caller reads ``null_fraction`` rather than looking for a NULL entry.
        """
        _setup(postgres_backend)
        try:
            samples = postgres_backend.introspector.sample_column_values(
                TABLE, "note", limit=10
            )
            assert {item.value for item in samples} == {"x", "y"}
            assert all(item.value is not None for item in samples)
        finally:
            _teardown(postgres_backend)


LEFT_TABLE = "intro_coverage_left"
RIGHT_TABLE = "intro_coverage_right"


def _coverage_setup(backend, left_values, right_values):
    """Two one-column tables, filled with the values the case was written for."""
    for table in (LEFT_TABLE, RIGHT_TABLE):
        backend.execute(f"DROP TABLE IF EXISTS {table}")
    backend.execute(f"CREATE TABLE {LEFT_TABLE} (code integer)")
    backend.execute(f"CREATE TABLE {RIGHT_TABLE} (code integer)")
    backend.execute_many(
        f"INSERT INTO {LEFT_TABLE} (code) VALUES (%s)",
        [(value,) for value in left_values],
    )
    backend.execute_many(
        f"INSERT INTO {RIGHT_TABLE} (code) VALUES (%s)",
        [(value,) for value in right_values],
    )


def _coverage_teardown(backend):
    for table in (LEFT_TABLE, RIGHT_TABLE):
        backend.execute(f"DROP TABLE IF EXISTS {table}")


def _coverage(backend):
    return backend.introspector.estimate_join_coverage(
        LEFT_TABLE, "code", RIGHT_TABLE, "code"
    )


class TestJoinCoverage:
    """How far two columns' distinct value domains overlap.

    Every expectation is arithmetic rather than a recorded run: ``left`` has
    ``{1,2,3,4}`` and ``right`` has ``{2,3,4,5,6}``, so three of the four left
    values are matched and three of the five right ones are. The two figures
    are different on purpose -- an implementation that answered the same number
    twice, or averaged them, would still look plausible.
    """

    def test_both_fractions_match_a_hand_count(self, postgres_backend):
        _coverage_setup(postgres_backend, range(1, 5), range(2, 7))
        try:
            coverage = _coverage(postgres_backend)
            assert coverage is not None
            assert coverage.left_coverage == pytest.approx(3 / 4)
            assert coverage.right_coverage == pytest.approx(3 / 5)
        finally:
            _coverage_teardown(postgres_backend)

    def test_values_are_counted_not_rows(self, postgres_backend):
        """A repeated key that matches once is one covered value, not three.

        The left table holds four rows but only two distinct values, and the
        repeated value is the matched one: counting rows would answer 3/4 where
        the share of values is 1/2.
        """
        _coverage_setup(postgres_backend, (2, 2, 2, 1), (2, 3))
        try:
            coverage = _coverage(postgres_backend)
            assert coverage is not None
            assert coverage.left_coverage == pytest.approx(1 / 2)
            assert coverage.right_coverage == pytest.approx(1 / 2)
        finally:
            _coverage_teardown(postgres_backend)

    def test_no_overlap_is_zero_not_none(self, postgres_backend):
        """Disjoint domains are a measurement: both sides really are uncovered.

        ``None`` means "the engine could not measure", which is a different
        answer from "nothing matched", and a caller deciding whether a join is
        worth writing has to be able to tell them apart.
        """
        _coverage_setup(postgres_backend, (1, 2, 3), (10, 11))
        try:
            coverage = _coverage(postgres_backend)
            assert coverage is not None
            assert coverage.left_coverage == 0.0
            assert coverage.right_coverage == 0.0
        finally:
            _coverage_teardown(postgres_backend)

    def test_a_side_with_no_value_answers_no_fraction(self, postgres_backend):
        """A column of nothing but NULLs has no share to report.

        NULLs are excluded from the divisors as well as the numerators, and
        ``NULLIF`` turns the empty denominator into NULL. So the left side --
        which has no value at all -- answers ``None``, while the right side,
        which has values that simply did not match, answers a real zero.
        """
        _coverage_setup(postgres_backend, (None, None), (1, 2))
        try:
            coverage = _coverage(postgres_backend)
            assert coverage is not None
            assert coverage.left_coverage is None
            assert coverage.right_coverage == 0.0
        finally:
            _coverage_teardown(postgres_backend)

    def test_neither_side_having_a_value_is_no_measurement(self, postgres_backend):
        """When both fractions are NULL the parser reports nothing measured.

        That is what keeps an all-NULL pair from reading as "zero overlap":
        there was no overlap to measure, and the answer says exactly that.
        """
        _coverage_setup(postgres_backend, (None,), (None,))
        try:
            assert _coverage(postgres_backend) is None
        finally:
            _coverage_teardown(postgres_backend)
