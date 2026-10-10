# tests/rhosocial/activerecord_postgres_test/feature/backend/types/test_column_type_suggestions.py
"""PostgreSQL's column-type table, and the one entry it refuses.

The protocol's obligation on a backend is three things
(``backend/dialect/mixins/column_type.py``, the contract list in
``testsuite/feature/query/typed_column/column_helpers.COMMON_TYPES``):

1. **Answer every entry.** All eighteen of the closed list, none of them
   silently absent. A hole is a lie told to the model layer.
2. **Say so where this backend differs.** Here: native arrays for the four
   sequence entries, where seven backends carry a JSON document.
3. **Refuse only what it truly cannot express** -- with ``None``, which reaches
   the caller as a resolution error rather than as a silent pick. Here: the one
   entry, ``datetime.timedelta``, whose native ``interval`` this server carries
   but whose operations no core column class names.

No database is needed for any of it: the question is which class an annotation
resolves to, and the dialect already knows the server version.
"""

import datetime
import decimal
import enum
import uuid
from typing import Optional

import pytest

from rhosocial.activerecord.backend.dialect.mixins import ColumnTypeMixin
from rhosocial.activerecord.backend.expression.column_types import (
    ArrayColumn,
    BinaryColumn,
    BooleanColumn,
    ColumnBase,
    DateTimeColumn,
    IntegerColumn,
    JSONColumn,
    NumericColumn,
    StringColumn,
    UUIDColumn,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.mixins.column_type import (
    POSTGRES_COLUMN_TYPES,
    PostgresColumnTypeMixin,
)
from rhosocial.activerecord.base.field_proxy import ColumnTypeResolutionError
from rhosocial.activerecord.testsuite.feature.query.typed_column.column_helpers import (
    COMMON_TYPES,
    resolve_column_class,
)


@pytest.fixture
def dialect():
    return PostgresDialect(version=(16, 0, 0))


def resolve(dialect, annotation):
    """The class the field accessor's own selection picks on PostgreSQL."""
    return resolve_column_class(dialect, annotation)


_ENTRY_IDS = [getattr(entry, "__name__", str(entry)) for entry in COMMON_TYPES]


# ---------------------------------------------------------------------------
# 1. The table answers the whole protocol, and the dialect really answers it
# ---------------------------------------------------------------------------


def test_the_dialect_carries_the_protocol_mixin(dialect):
    """PostgreSQL has to *have* the mixin; a table on a class nothing reaches is
    documentation. ``Model.c.<field>`` asks the dialect's table on every column
    access, so this is also what makes a model's fields work here."""
    assert isinstance(dialect, ColumnTypeMixin)
    assert isinstance(dialect, PostgresColumnTypeMixin)


@pytest.mark.parametrize("entry", COMMON_TYPES, ids=_ENTRY_IDS)
def test_every_entry_is_answered(dialect, entry):
    """Eighteen present, every value a column class or a deliberate refusal.

    A missing key and a ``None`` have to fail differently from each other, hence
    two assertions rather than one ``.get()``: a forgotten entry is a hole in
    the contract, while a ``None`` is this backend stating that it has no class
    for a value family -- and the protocol wants that stated, not guessed.
    """
    table = dialect.suggested_column_types()
    assert entry in table, f"PostgreSQL does not answer {entry!r}"

    suggested = table[entry]
    assert suggested is None or (
        isinstance(suggested, type) and issubclass(suggested, ColumnBase)
    ), f"{entry!r} answered with {suggested!r}, which is neither a column class nor None"


def test_the_table_has_no_entry_beyond_the_protocol(dialect):
    """A key of the backend's own is allowed by the protocol, and is reported
    separately through ``suggested_extra_column_types()`` so "the framework has
    an answer" stays distinguishable from "this backend has one of its own".
    PostgreSQL has neither, and a stray key in the common table would be
    indistinguishable from a framework entry at the call site."""
    extra = set(dialect.suggested_column_types()) - set(COMMON_TYPES)
    assert extra == set()


def test_the_backend_declares_no_extra_python_types(dialect):
    """The framework's eighteen are the whole of what PostgreSQL answers for.

    Its *server* types beyond them (``interval``, ``xml``, the multirange
    family) are reached through their ``DataType`` spelling, not through a
    Python-annotation entry of their own: annotating a field with an
    unregistered custom class is what ``UseColumnType`` is for.
    """
    assert dialect.suggested_extra_column_types() == {}


def test_the_table_agrees_with_the_module_level_constant(dialect):
    """One table, one source: the mixin's answer is the documented table.

    Written as an equality rather than an import-and-call so that a future edit
    which makes the mixin compute something else fails here, naming the drift.
    """
    assert dialect.suggested_column_types() == POSTGRES_COLUMN_TYPES
    assert dialect.suggested_column_types() is not POSTGRES_COLUMN_TYPES


# ---------------------------------------------------------------------------
# 2. Every answer is the measured one
# ---------------------------------------------------------------------------


def test_resolution_is_the_baseline_for_the_scalar_and_value_families(dialect):
    """The ten-backend baseline, written out so a change to one of these is a
    test failure rather than a silent redefinition of what a ``str`` means."""
    assert resolve(dialect, bool) is BooleanColumn
    assert resolve(dialect, int) is IntegerColumn
    assert resolve(dialect, str) is StringColumn
    assert resolve(dialect, bytes) is BinaryColumn
    assert resolve(dialect, bytearray) is BinaryColumn
    assert resolve(dialect, uuid.UUID) is UUIDColumn
    assert resolve(dialect, dict) is JSONColumn
    assert resolve(dialect, datetime.date) is DateTimeColumn
    assert resolve(dialect, datetime.time) is DateTimeColumn
    assert resolve(dialect, datetime.datetime) is DateTimeColumn
    assert resolve(dialect, enum.Enum) is StringColumn


def test_the_numeric_entries_answer_the_one_numeric_class(dialect):
    """``float`` / ``decimal.Decimal`` answer ``NumericColumn``, and why.

    This table used to answer them with the dedicated ``FloatColumn`` /
    ``DecimalColumn`` core then had (secondary-gaps 附录 C: "real/double
    precision 真实分界"), on the measured strength of PostgreSQL spelling
    ``real`` and ``double precision`` as distinct widths. The rebuild removed
    those classes -- the numeric family is deliberately one class -- so the
    distinction now lives where it was always measurable in storage anyway: the
    ``DataType`` layer, not the operation surface. The answer changed name, not
    capability, and this is the test that says so.
    """
    assert resolve(dialect, float) is NumericColumn
    assert resolve(dialect, decimal.Decimal) is NumericColumn


def test_the_sequence_entries_answer_array_column_not_json(dialect):
    """**This backend's one real difference from the seven-backend majority.**

    Seven backends carry ``list`` as a JSON document and route array operations
    through JSON functions; PostgreSQL has a real array type, and
    ``suggested-pairing-array.md`` §A measured 9.6 / 12 / 18 with every row of
    the array guarantee set green (DDL, direct list binding, ``array_length``,
    1-based subscript, ``@>``, ``||``, ``v = ANY(x)``, whole-column equality,
    ``unnest``). Same Python type, different storage, different answer.

    ``tuple`` / ``set`` / ``frozenset`` come along for the same reason the
    baseline groups them: to the server they are sequences, and order is the one
    thing a set does not promise.
    """
    for entry in (list, tuple, set, frozenset):
        assert resolve(dialect, entry) is ArrayColumn, entry

    # And the class really is the array one -- the deviation has to be a class
    # that can answer, not a rename.
    for op in ("array_length", "unnest"):
        assert hasattr(ArrayColumn, op), f"ArrayColumn lost {op}"


def test_dict_answers_the_json_column_while_the_ddl_layer_chooses_jsonb(dialect):
    """The two halves stay apart, and the column half is the operations.

    ``.claude/plan/2026-10-08/column-suggestion-protocol.md`` §0.2 requires
    ``jsonb`` for a ``dict`` here because ``json`` has no equality operator, no
    ``?`` and no ``@>``. That is a *spelling* decision belonging to the
    ``DataType`` layer, and it is not the column class's to carry: the operation
    surface -- ``json_path``, ``json_value``, has-key, array length, validity --
    is the same on ``json`` and ``jsonb``, and ``jsonb`` simply adds the
    containment operators on top.
    """
    assert resolve(dialect, dict) is JSONColumn
    assert hasattr(JSONColumn, "json_path")
    assert hasattr(JSONColumn, "json_value")

    from rhosocial.activerecord.backend.impl.postgres.mixins.types.data_type_formatting import (
        PostgresTypeFormatSupportMixin,
    )

    # The DDL side is real and independent of the answer above, which is what
    # "neither layer infers the other" means in practice.
    assert hasattr(PostgresTypeFormatSupportMixin, "format_data_type_jsonb")


def test_the_enum_entry_answers_string_column(dialect):
    """PostgreSQL does have enums, but as a *named* type created once by
    ``CREATE TYPE ... AS ENUM`` and referred to by name -- a DDL fact carried by
    ``PostgresEnumColumnType``. The labels compare as strings, so the value
    surface is the string one; putting the label set in the column class would
    move storage into the operation layer.
    """

    class Mood(enum.Enum):
        SAD = "sad"
        OK = "ok"

    assert resolve(dialect, Mood) is StringColumn


def test_the_interval_entry_is_refused_until_core_grows_an_interval_column(dialect):
    """**The one deliberate refusal**, and the reasons it is not ``None``'s
    opposite either.

    PostgreSQL has a real ``interval`` type where eight backends have no answer
    at all (the protocol's own survey, §1), so there is no "this backend cannot
    carry a duration" story here: psycopg binds a ``timedelta`` and reads one
    back, and the ``DataType`` layer spells the storage
    (``format_data_type_interval``). What no core column class offers is the
    *operation* surface -- ``justify_hours``, ``EXTRACT(... FROM interval)``,
    the field qualifiers -- and naming a number class would offer
    transcendentals the server refuses: ``sqrt(interval '1 day')`` is an error.

    ``NumericColumn`` was the answer the last version of this table gave, and
    it was a *declared* over-claim kept visible only because ``FloatColumn``
    and ``DecimalColumn`` existed to differ from. With the numeric family merged
    into one class that visibility is gone, so the honest answer under the
    rebuilt protocol is ``None``: the gap reaches the model layer as a
    resolution error naming the way out, instead of as a number class that
    cannot keep its promise.

    When core grows an ``IntervalColumn``, this entry is where it goes -- so
    this test fails then, on purpose.
    """
    assert dialect.suggested_column_types()[datetime.timedelta] is None

    with pytest.raises(ColumnTypeResolutionError) as excinfo:
        resolve(dialect, datetime.timedelta)
    assert "UseColumnType" in str(excinfo.value)

    # The refusal is this table's, not the framework's: the generic numeric
    # answer is what the portable baseline would have said.
    from rhosocial.activerecord.backend.impl.dummy.column_type import DUMMY_COLUMN_TYPES

    assert DUMMY_COLUMN_TYPES[datetime.timedelta] is NumericColumn


# ---------------------------------------------------------------------------
# 3. Normalisation: the protocol's rules, on this backend
# ---------------------------------------------------------------------------


def test_optional_is_transparent(dialect):
    """``Optional[T]`` must resolve exactly as ``T`` does -- compared against
    this dialect's own answer rather than a fixed class, since the answer may
    legitimately vary by backend."""
    assert resolve(dialect, Optional[dict]) is resolve(dialect, dict)
    assert resolve(dialect, Optional[str]) is resolve(dialect, str)
    assert resolve(dialect, Optional[list]) is resolve(dialect, list)


def test_a_subclass_walks_to_its_entry(dialect):
    """``class Code(str)`` answers the ``str`` entry. The walk runs over *this
    backend's* key space, so a backend that extended the list would get the
    behaviour for free."""

    class Code(str):
        pass

    assert resolve(dialect, Code) is StringColumn


def test_bool_is_its_own_entry_not_an_integer(dialect):
    """``bool`` is an ``int`` subclass and the entry order is the only thing that
    keeps them apart. Without it a truth-value field would offer integer
    arithmetic -- which MySQL and SQLite would happily execute on 0/1."""
    assert resolve(dialect, bool) is not resolve(dialect, int)
    assert list(dialect.suggested_column_types()).index(bool) < list(
        dialect.suggested_column_types()
    ).index(int)


def test_an_unknown_annotation_fails_rather_than_becoming_a_universal_column(dialect):
    """No permissive fallback: the error names the way out. The old universal
    column offered ``.like()`` to an integer and only found out at the
    database."""
    with pytest.raises(ColumnTypeResolutionError) as excinfo:
        resolve(dialect, object())
    assert "UseColumnType" in str(excinfo.value)


def test_the_operation_names_the_contract_uses_are_real_attributes(dialect):
    """Operation names are *method names on the column class* so the contract
    tests can cross-check with ``hasattr``. If a name here were a label rather
    than an attribute, the contract would be checking a string.

    ``is_true`` / ``is_false`` come from ``BooleanLogicMixin``; the rest from the
    mixins each class is composed from.
    """
    expected = {
        "StringColumn": ("like", "ilike"),
        "JSONColumn": ("json_path", "json_value"),
        "ArrayColumn": ("array_length", "unnest"),
        "DateTimeColumn": ("date_trunc", "extract"),
        "BooleanColumn": ("is_true", "is_false"),
        "NumericColumn": ("__add__", "__mul__"),
    }
    # Resolved by *name* out of this backend's own table, so the cross-check runs
    # against the class PostgreSQL actually suggests rather than against a
    # hard-coded import that could stop being the answer. A refused entry has no
    # class to name, which is the point of refusing it.
    by_name = {
        cls.__name__: cls for cls in dialect.suggested_column_types().values() if cls is not None
    }

    for column_name, ops in expected.items():
        cls = by_name[column_name]
        for op in ops:
            assert hasattr(cls, op), (
                f"{column_name} has no attribute {op!r}, so declaring it "
                f"available would be checking a string"
            )
