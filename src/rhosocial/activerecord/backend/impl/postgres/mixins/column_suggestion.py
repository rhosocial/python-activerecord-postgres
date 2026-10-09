# src/rhosocial/activerecord/backend/impl/postgres/mixins/column_suggestion.py
"""PostgreSQL's answer to "which column class does this Python type mean here".

The column half of the suggestion protocol (`.claude/plan/2026-10-08/
column-suggestion-protocol.md` §1–§2, §12's PostgreSQL row). A column class
says what a value can **do** on this server; how it is **stored** is the
``DataType`` layer's separate decision and never read from here. Keeping the two
apart is why this table can answer ``dict`` with a JSON column class while the
DDL side reaches for ``jsonb`` (§0.2) — the two are answers to different
questions, and neither infers the other.

Every answer below is written out rather than inherited from core's neutral
table, because a backend that owns its answer should be readable without a
second file. The eighteen entries are the protocol's closed list, all of them
answered, and none of them refused: :data:`UNSUPPORTED` is for a backend that
*cannot* express an entry, and there is no such entry here — see
``UNSUPPORTED`` below.

**What the answers are measured against.** The live probes in
``.claude/plan/2026-10-08/suggested-mappings.md`` §8 (≈90 server instances
across ten backends), the per-family detail in ``suggested-pairing-array.md``,
``-json``, ``-datetime``, ``-string-enum``, ``-uuid``, ``-numeric``, and this
backend's own ``.claude/plan/2026-10-08/secondary-gaps-investigation.md``
(附录 C mapping, 附录 D gap inventory, 附录 F catalog round-trip over eleven
servers). 附录 F is the discipline this table follows rather than a source of
answers: its single finding F1 (``bit varying(n)`` parsed as ``PostgresBitType``
instead of ``PostgresVarBitType``) was invisible to render/parse round-trips
and only appeared once the **server's own catalog words** were fed in — four of
thirty-six words were wrong, and the declared spelling was wrong the same way,
so both sides of the comparison agreed with each other and both were wrong. Fixed
in ``04ec5e5``. An answer written from documentation rather than from a server
is exactly the shape of that bug, which is why the array and JSON rows below
quote the probe tables instead of the manual.
"""

import datetime
import decimal
import enum
import uuid
from typing import Any, Dict, Type

from rhosocial.activerecord.backend.dialect.mixins import ColumnSuggestionMixin
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


class PostgresColumnSuggestionMixin(ColumnSuggestionMixin):
    """The full common-Python-type → column-class table for PostgreSQL.

    Composed into :class:`~rhosocial.activerecord.backend.impl.postgres.dialect.PostgresDialect`
    ahead of this backend's own mixins, and after core's
    :class:`~rhosocial.activerecord.backend.dialect.mixins.column_suggestion.ColumnSuggestionMixin`
    supplies the resolution path it is queried through.

    **Two entries differ from core's neutral table** — ``float`` and
    ``decimal.Decimal`` — and the other sixteen are the baseline all ten backends
    agree on. The sixteen are still written here rather than spread-copied, so
    that "this backend agrees" is a stated fact a test can check instead of an
    accident of what core happened to publish.

    **No entry is ``UNSUPPORTED``.** The protocol reserves the sentinel for a
    backend with no column class for an entry, and the two backends that need it
    need it for ``dict`` — MySQL below 5.7 and Firebird 5/6, which have no JSON
    functions at all (protocol §12). PostgreSQL has had ``json`` since 9.2 and
    ``jsonb`` since 9.4, plus the path operators on both, so the refusal would
    be false. Every measured PG version behind these answers (9.6, 12, 13, 16,
    18, 19beta4 — 附录 F) answers all eighteen, and the table has no version
    branch because nothing in it moves with the version: the one thing that does
    (``jsonb`` becoming available in 9.4) changes the DDL spelling, not which
    column class a ``dict`` is.
    """

    #: All eighteen protocol entries, in the protocol's reading order. Commented
    #: per group because the reasons are the content: the answer is a claim about
    #: this server, and a claim without its evidence is decoration.
    COLUMN_TYPE_SUGGESTIONS: Dict[Any, Type[ColumnBase]] = {
        # -- Scalars ---------------------------------------------------------
        # Native `boolean`, and the three-valued logic that comes with it, so
        # `is_true()` / `is_false()` render as `IS TRUE` / `IS FALSE` rather
        # than `= 1`. That spelling is not a stylistic choice: PostgreSQL rejects
        # `boolean = integer` outright (suggested-mappings §8.1, `bool` row),
        # which is why `BooleanColumn` exists rather than a numeric column.
        bool: BooleanColumn,
        int: IntegerColumn,
        # DEVIATION 1 of 2. Core's neutral table answers `NumericColumn` for
        # `float`; §12's ten-backend baseline says `FloatColumn`, and this
        # backend's 附录 C row agrees ("核心默认 NumericColumn 偏泛；本后端
        # real/double precision 真实分界"). The two classes offer the same
        # operations — `FloatColumn`, `RealColumn` and `DoubleColumn` differ in
        # name, not surface — so this is a readability gain, not a capability
        # claim, and it is why no operation is narrowed for it below.
        float: FloatColumn,
        # DEVIATION 2 of 2, same shape: core answers `NumericColumn`, §12 says
        # `DecimalColumn`, and 附录 C gives the reason for this backend
        # specifically — `numeric` is the exact family here, so a `Decimal` is
        # not "some number" but a number whose sum has to come out right.
        decimal.Decimal: DecimalColumn,
        str: StringColumn,
        # `bytea`. Binary comparison, length, substring and hex are all
        # available (suggested-mappings §8.1, `bytes` row, where PostgreSQL is
        # in the unqualified set).
        bytes: BinaryColumn,
        bytearray: BinaryColumn,
        # -- Temporal --------------------------------------------------------
        # `date` / `time` / `datetime` are three entries and one answer, and the
        # answer is the baseline: core has no `DateColumn` / `TimeColumn` to give
        # (secondary-gaps 附录 D ① records both as core gaps), so splitting
        # `DateTimeColumn` by operation set is a core modelling change and not
        # this backend's to make in this table. A tz-aware `datetime` lands on
        # the same class and is where this backend is strongest: `AT TIME ZONE`
        # is native and named zones work (only the *read-back* follows the
        # session time zone — §8.1 `datetime tz` row).
        datetime.date: DateTimeColumn,
        datetime.time: DateTimeColumn,
        datetime.datetime: DateTimeColumn,
        # `timedelta` → `NumericColumn`, which is the §12 fallback and NOT what
        # this server can do. PostgreSQL has a real `interval` type — §1 spells
        # it `INTERVAL` where eight backends have no answer at all, and
        # 附录 D ① lists `IntervalColumn` as a class this backend would use and
        # core does not have.
        #
        # It is kept at `NumericColumn` because `interval` is not a number: the
        # server answers `justify_hours`, `EXTRACT(... FROM interval)` and field
        # qualifiers for it, and refuses `sqrt(interval '1 day')` outright.
        # `NumericColumn` offers transcendentals, so naming it here over-claims
        # exactly that much — a real, and deliberately *declared*, gap rather
        # than a silent one: it is inherited from core's neutral table, not
        # chosen here, and the fix is core growing an `IntervalColumn` (pinned by
        # `test_timedelta_is_the_numeric_gap_until_core_grows_an_interval_column`).
        datetime.timedelta: NumericColumn,
        # -- Value families --------------------------------------------------
        uuid.UUID: UUIDColumn,
        # `dict` → the JSON column class, on both `json` and `jsonb`. §12's
        # PostgreSQL row adds `jsonb` because `json` has **no equality operator,
        # no `?` and no `@>`** (protocol §0.2; §8.1 `dict` row) — but that is a
        # DDL-layer choice made at the `DataType` layer (`JsonBType` /
        # `format_data_type_jsonb` on this backend), not something a column class
        # can express. The column class is the operation surface, and it is the
        # same either way: `json_path` / `json_value`, has-key, array length,
        # validity, and on `jsonb` the containment operators on top
        # (`PostgresJSONBEnhancedMixin`).
        dict: JSONColumn,
        # Native arrays — the strongest `ArrayColumn` backend of the ten, and the
        # one place where this table's answer is a real *difference* rather than
        # a shared baseline. Seven backends route `list` through a JSON document
        # (§12); `suggested-pairing-array.md` §A measures PostgreSQL 9.6 / 12 /
        # 18 with every row of the guarantee set green: `INT[]` / `TEXT[]` DDL,
        # a Python list bound directly and read back as a list, `array_length`,
        # 1-based `x[1]`, containment `x @> ARRAY[v]`, concatenation `x || v`,
        # the quantified form `v = ANY(x)`, whole-column equality, and `unnest`
        # — against a seven-backend field where the same ops are variously
        # absent, silently wrong, or need emulation. 附录 C calls this the
        # contrast case: same Python type, different storage, different answer.
        #
        # `set` / `frozenset` / `tuple` answer `ArrayColumn` too, for the same
        # reason the baseline puts them together: they are sequences to the
        # server, and order is the one thing a set does not promise.
        list: ArrayColumn,
        tuple: ArrayColumn,
        set: ArrayColumn,
        frozenset: ArrayColumn,
        # `enum.Enum` → `StringColumn`, which is the baseline and is also what
        # the value semantics say: the labels compare as strings, so equality,
        # `IN` and `ORDER BY` render as they do for text (§8.1 `enum` row).
        #
        # PostgreSQL does have enums, but as a *named* type created once by
        # `CREATE TYPE ... AS ENUM` and referred to by name — which is a DDL
        # fact, carried by `PostgresEnumColumnType` on the `DataType` side
        # (secondary-gaps §2 item 5). Spelling the label set into the column
        # class would put storage into the operation layer, which is the split
        # this protocol exists to keep.
        enum.Enum: StringColumn,
    }

    def supports_column_operation(self, column_name: str, op: str) -> bool:
        """No operation is narrowed on this backend. Every one is available.

        The declared verdict, written out rather than inherited silently, because
        "nothing was narrowed" and "nobody checked" look identical at the call
        site and this backend is the series' **control**: eight of the ten backends
        have to narrow ``ilike`` away, and this one is what proves the mechanism
        discriminates.

        What the probes measured against, per §8.1 / §8.4 and
        ``secondary-gaps-investigation.md`` 附录 D ③ ("核心提供、本后端做不到：
        几乎无——本后端能力面最全"):

        ``ilike`` (``StringColumn``)
            Native, and the reason eight backends narrow it. PostgreSQL is one of
            the two that answer it (§8.1 `str` row, §8.4 "ILIKE 仅 PG/CH").
        ``json_path`` / ``json_value`` (``JSONColumn``)
            `->>` / `->` on both `json` and `jsonb`, with `jsonb`'s `@>`, `?`,
            `?|`, `?&`, `#>>` beyond what the path accessors offer. The JSON
            guarantee set — binding round-trip, scalar and nested paths, has-key,
            array length, validity — is measured green here (§8.1 `dict` row),
            where MySQL 5.6 and Firebird have no default at all.
        ``array_length`` / ``unnest`` (``ArrayColumn``)
            Both native and both measured on 9.6 / 12 / 18, together with `x[1]`,
            `@>`, `||`, `v = ANY(x)` and whole-column equality
            (``suggested-pairing-array.md`` §A).
        tz-aware ``datetime``
            `AT TIME ZONE` is native and the offset form works everywhere; the
            one caveat §8.1 records is that a tz-aware value **reads back** in the
            session time zone, which is a value-layer converter question, not a
            missing operation.
        ``LENGTH`` on ``StringColumn``
            Characters, matching what the string family means on this backend
            (§8.4: MySQL/ClickHouse are bytes, SQL Server UTF-16 code units).
            Same answer, so nothing to declare.

        Args:
            column_name: The column class name (``"StringColumn"``). Not
                consulted — see the contract test that cross-checks the operation
                names against ``hasattr``.
            op: The operation name, as the public method that provides it.

        Returns:
            Always ``True``.
        """
        return True
