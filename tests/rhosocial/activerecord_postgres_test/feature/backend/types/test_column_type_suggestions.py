# tests/rhosocial/activerecord_postgres_test/feature/backend/types/test_column_type_suggestions.py
"""PostgreSQL's column-type suggestion table, and its declared lack of narrowing.

The protocol's obligation on a backend is three things (protocol §2, §12):

1. **Answer every entry.** All eighteen of the closed list, none of them empty
   and none of them silently absent. A hole is a lie told to the model layer.
2. **Say so where this backend differs.** Here: native arrays for the four
   sequence entries, ``FloatColumn`` / ``DecimalColumn`` where core answers the
   generic ``NumericColumn``, and no ``UNSUPPORTED`` anywhere.
3. **Narrow only with evidence.** PostgreSQL narrows nothing, which is asserted
   rather than assumed -- it is the series' control backend, eight others have to
   take ``ilike`` away, so "nothing was narrowed" and "nobody looked" must not
   look the same at the call site.

No database is needed for any of it: the question is which class an annotation
resolves to, and the dialect already knows the server version.
"""

import datetime
import decimal
import enum
import uuid
from typing import Optional

import pytest

from rhosocial.activerecord.backend.dialect.mixins import ColumnSuggestionMixin
from rhosocial.activerecord.backend.expression.column_suggestions import (
    COLUMN_TYPE_ENTRIES,
    UNSUPPORTED,
    ColumnTypeResolutionError,
)
from rhosocial.activerecord.backend.expression.column_types import (
    ArrayColumn,
    BinaryColumn,
    BooleanColumn,
    ColumnBase,
    DateTimeColumn,
    DecimalColumn,
    FloatColumn,
    IntegerColumn,
    JSONColumn,
    NumericColumn,
    StringColumn,
    UUIDColumn,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.mixins.column_suggestion import (
    PostgresColumnSuggestionMixin,
)


@pytest.fixture
def dialect():
    return PostgresDialect(version=(16, 0, 0))


_ENTRY_IDS = [getattr(entry, "__name__", str(entry)) for entry in COLUMN_TYPE_ENTRIES]


# ---------------------------------------------------------------------------
# 1. The table answers the whole protocol, and the dialect really answers it
# ---------------------------------------------------------------------------


def test_the_dialect_carries_the_protocol_mixin(dialect):
    """PostgreSQL has to *have* the mixin; a table on a class nothing reaches is
    documentation. ``field_proxy`` calls ``dialect.column_class_for`` on every
    column access, so this is also what makes ``Model.c.<field>`` work here."""
    assert isinstance(dialect, ColumnSuggestionMixin)
    assert isinstance(dialect, PostgresColumnSuggestionMixin)


@pytest.mark.parametrize("entry", COLUMN_TYPE_ENTRIES, ids=_ENTRY_IDS)
def test_every_entry_is_answered(dialect, entry):
    """Eighteen present, every value a real column class.

    ``None`` is the one answer the protocol forbids and it is not detectable by
    ``suggested_column_types().get(entry, None)``, hence the explicit check: a
    forgotten entry and a ``None`` have to fail differently from a refusal.
    """
    table = dialect.suggested_column_types()
    assert entry in table, f"PostgreSQL does not answer {entry!r}"

    suggested = table[entry]
    assert suggested is not None, f"{entry!r} answered with None, which is silence"
    assert suggested is not UNSUPPORTED, f"{entry!r} is refused; PostgreSQL can express it"
    assert isinstance(suggested, type)
    assert issubclass(suggested, ColumnBase)


def test_the_table_has_no_entry_beyond_the_protocol(dialect):
    """A key of the backend's own is allowed by §2, and is registered separately
    so "the framework has an answer" stays distinguishable from "this backend has
    one of its own". PostgreSQL has none, and a stray key would be indistinguishable
    from the core half at the call site."""
    extra = set(dialect.suggested_column_types()) - set(COLUMN_TYPE_ENTRIES)
    assert extra == set()


def test_the_table_is_a_copy_not_the_shared_class_attribute(dialect):
    """Mutating what a caller received must not reach the next resolution."""
    table = dialect.suggested_column_types()
    table[str] = BinaryColumn
    assert dialect.column_class_for(str) is StringColumn


# ---------------------------------------------------------------------------
# 2. Every entry resolves to what the table says
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entry", COLUMN_TYPE_ENTRIES, ids=_ENTRY_IDS)
def test_every_entry_resolves_to_its_own_table_answer(dialect, entry):
    """Model and table must agree, or the table is a comment rather than the
    answer. Table-relative on purpose: the class may legitimately differ between
    backends, and what must not differ is what a model builds."""
    assert dialect.column_class_for(entry) is dialect.suggested_column_types()[entry]


def test_resolution_is_the_baseline_for_the_scalar_and_value_families(dialect):
    """The ten-backend baseline, written out so a change to one of these is a
    test failure rather than a silent redefinition of what a ``str`` means."""
    assert dialect.column_class_for(bool) is BooleanColumn
    assert dialect.column_class_for(int) is IntegerColumn
    assert dialect.column_class_for(str) is StringColumn
    assert dialect.column_class_for(bytes) is BinaryColumn
    assert dialect.column_class_for(bytearray) is BinaryColumn
    assert dialect.column_class_for(uuid.UUID) is UUIDColumn
    assert dialect.column_class_for(dict) is JSONColumn
    assert dialect.column_class_for(datetime.date) is DateTimeColumn
    assert dialect.column_class_for(datetime.time) is DateTimeColumn
    assert dialect.column_class_for(datetime.datetime) is DateTimeColumn


def test_float_and_decimal_answer_their_own_classes_not_the_generic_numeric(dialect):
    """Two deviations from core's neutral table, both §12's baseline.

    ``FloatColumn`` and ``DecimalColumn`` offer exactly the operations
    ``NumericColumn`` does, so this is about the name a reader sees rather than
    about capability -- which is also why nothing is narrowed for them. PostgreSQL
    is the backend where the distinction is real storage, though: ``numeric`` is
    its exact family and ``real`` / ``double precision`` are distinct widths.
    """
    assert dialect.column_class_for(float) is FloatColumn
    assert dialect.column_class_for(decimal.Decimal) is DecimalColumn
    assert dialect.column_class_for(float) is not NumericColumn
    assert dialect.column_class_for(decimal.Decimal) is not NumericColumn


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
        assert dialect.column_class_for(entry) is ArrayColumn, entry

    # And the class really is the array one -- the deviation has to be a class
    # that can answer, not a rename.
    for op in ("array_length", "unnest"):
        assert hasattr(ArrayColumn, op), f"ArrayColumn lost {op}"


def test_dict_answers_the_json_column_while_the_ddl_layer_chooses_jsonb(dialect):
    """The two halves stay apart, and the column half is the operations.

    §0.2 requires ``jsonb`` for a ``dict`` here because ``json`` has no equality
    operator, no ``?`` and no ``@>``. That is a *spelling* decision belonging to
    the ``DataType`` layer, and it is not the column class's to carry: the
    operation surface -- ``json_path``, ``json_value``, has-key, array length,
    validity -- is the same on ``json`` and ``jsonb``, and ``jsonb`` simply adds
    the containment operators on top.
    """
    assert dialect.column_class_for(dict) is JSONColumn
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

    assert dialect.column_class_for(Mood) is StringColumn


def test_timedelta_is_the_numeric_gap_until_core_grows_an_interval_column(dialect):
    """§12's ``IntervalColumn`` fallback, taken deliberately and pinned.

    PostgreSQL has a real ``interval`` type where eight backends have no answer at
    all, so ``NumericColumn`` under-claims nothing and over-claims exactly one
    thing: ``interval`` is not a number, and ``sqrt(interval '1 day')`` is an
    error on the server while ``NumericColumn`` offers transcendentals. That gap
    is inherited from core's neutral table, not chosen here, and it closes when
    core grows an ``IntervalColumn`` -- so this test fails then, on purpose.
    """
    assert dialect.column_class_for(datetime.timedelta) is NumericColumn

    from rhosocial.activerecord.backend.expression import column_types

    assert not hasattr(column_types, "IntervalColumn"), (
        "Core now has an IntervalColumn; PostgreSQL's native interval should "
        "answer with it (protocol §12, PostgreSQL row) rather than the numeric "
        "fallback. This test is the reminder to change the table."
    )


# ---------------------------------------------------------------------------
# 3. Normalisation: the protocol's rules, on this backend
# ---------------------------------------------------------------------------


def test_optional_is_transparent(dialect):
    """``Optional[T]`` must resolve exactly as ``T`` does -- compared against
    this dialect's own answer rather than a fixed class, since the answer may
    legitimately vary by backend."""
    assert dialect.column_class_for(Optional[dict]) is dialect.column_class_for(dict)
    assert dialect.column_class_for(Optional[str]) is dialect.column_class_for(str)
    assert dialect.column_class_for(Optional[list]) is dialect.column_class_for(list)


def test_a_subclass_walks_to_its_entry(dialect):
    """``class Code(str)`` answers the ``str`` entry. The walk runs over *this
    backend's* key space, so a backend that extended the list would get the
    behaviour for free."""

    class Code(str):
        pass

    assert dialect.column_class_for(Code) is StringColumn


def test_bool_is_its_own_entry_not_an_integer(dialect):
    """``bool`` is an ``int`` subclass and the entry order is the only thing that
    keeps them apart. Without it a truth-value field would offer integer
    arithmetic -- which MySQL and SQLite would happily execute on 0/1."""
    assert dialect.column_class_for(bool) is not dialect.column_class_for(int)
    assert COLUMN_TYPE_ENTRIES.index(bool) < COLUMN_TYPE_ENTRIES.index(int)


def test_an_unknown_annotation_fails_rather_than_becoming_a_universal_column(dialect):
    """No permissive fallback: ``Any`` is a definition-time failure naming the way
    out. The old universal column offered ``.like()`` to an integer and only
    found out at the database."""
    with pytest.raises(ColumnTypeResolutionError) as excinfo:
        dialect.column_class_for(object())
    assert "UseColumnType" in str(excinfo.value)


# ---------------------------------------------------------------------------
# 4. Narrowing: declared as none, and checked to be none
# ---------------------------------------------------------------------------


def test_nothing_is_narrowed_on_this_backend(dialect):
    """The declared verdict. Eight of the ten backends have to take ``ilike``
    away; PostgreSQL is the control that proves the mechanism discriminates
    rather than always answering True.

    The pairs are the ones §8.1/§8.4 measured here: ``ILIKE`` native (§8.4
    "ILIKE 仅 PG/CH"), JSON path access native on ``json`` and ``jsonb``, array
    length and ``unnest`` native on 9.6/12/18, tz ``AT TIME ZONE`` native.
    """
    assert dialect.supports_column_operation("StringColumn", "ilike") is True
    assert dialect.supports_column_operation("StringColumn", "like") is True
    assert dialect.supports_column_operation("JSONColumn", "json_path") is True
    assert dialect.supports_column_operation("JSONColumn", "json_value") is True
    assert dialect.supports_column_operation("ArrayColumn", "array_length") is True
    assert dialect.supports_column_operation("ArrayColumn", "unnest") is True
    assert dialect.supports_column_operation("DateTimeColumn", "date_trunc") is True
    assert dialect.supports_column_operation("BooleanColumn", "is_true") is True
    assert dialect.supports_column_operation("UUIDColumn", "eq") is True
    assert dialect.supports_column_operation("BinaryColumn", "eq") is True


def test_no_narrowing_holds_for_any_operation_on_any_column_class(dialect):
    """The whole surface, not a hand-picked sample of it.

    Driven off the table itself so a new entry cannot introduce a narrowing
    without this noticing, and off ``dir()`` of each class so a new operation
    method is covered too.
    """
    narrowed = []
    for column_class in dialect.suggested_column_types().values():
        name = column_class.__name__
        for op in dir(column_class):
            if op.startswith("_"):
                continue
            if not dialect.supports_column_operation(name, op):
                narrowed.append(f"{name}.{op}")
    assert narrowed == []


def test_the_operation_names_the_contract_uses_are_real_attributes(dialect):
    """Operation names are *method names on the column class* so the contract
    tests can cross-check with ``hasattr`` (protocol §5). If a name here were a
    label rather than an attribute, the contract would be checking a string.

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
    # hard-coded import that could stop being the answer.
    by_name = {cls.__name__: cls for cls in dialect.suggested_column_types().values()}

    for column_name, ops in expected.items():
        assert column_name in by_name, f"{column_name} is not in the table"
        for op in ops:
            assert dialect.supports_column_operation(column_name, op) is True
            assert hasattr(by_name[column_name], op), (
                f"{column_name} has no attribute {op!r}, so declaring it "
                f"available would be checking a string"
            )
