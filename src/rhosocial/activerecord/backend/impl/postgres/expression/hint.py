# src/rhosocial/activerecord/backend/impl/postgres/expression/hint.py
"""pg_hint_plan hint expression nodes.

Hints are structured expression nodes, not SQL strings.  Builders construct
these nodes; only the dialect formatter renders them into the ``/*+ ... */``
comment.  Values are validated with Python's identifier/number contracts -- no
regex, no substring checks, no SQL text inspection anywhere in this module.
"""

from enum import Enum
from typing import Optional, Tuple, TYPE_CHECKING, Union

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


class HintKind(str, Enum):
    """The pg_hint_plan hint name. A member *is* the SQL spelling."""

    __str__ = str.__str__

    SEQ_SCAN = "SeqScan"
    TID_SCAN = "TidScan"
    INDEX_SCAN = "IndexScan"
    INDEX_ONLY_SCAN = "IndexOnlyScan"
    BITMAP_SCAN = "BitmapScan"
    NO_SEQ_SCAN = "NoSeqScan"
    NO_INDEX_SCAN = "NoIndexScan"
    HASH_JOIN = "HashJoin"
    MERGE_JOIN = "MergeJoin"
    NEST_LOOP = "NestLoop"
    LEADING = "Leading"
    ROWS = "Rows"
    PARALLEL = "Parallel"
    SET = "Set"


class CorrectionKind(str, Enum):
    """Row-count correction operator for the Rows hint."""

    __str__ = str.__str__

    ABSOLUTE = "#"
    ADD = "+"
    SUBTRACT = "-"
    MULTIPLY = "*"


def _require_identifier(value: str, field: str) -> str:
    """Validate an identifier without inspecting its characters by hand."""
    if not isinstance(value, str) or not value.isidentifier():
        raise ValueError(f"Invalid pg_hint_plan {field}: {value!r}")
    return value


def _assert_identifier(value: str, field: str) -> None:
    """The render-time form of :func:`_require_identifier`.

    A malformed payload is a caller error, so the expression's
    :meth:`check_renderable` reports it as ``TypeError``; the constructor only
    collects.
    """
    if not isinstance(value, str) or not value.isidentifier():
        raise TypeError(f"Invalid pg_hint_plan {field}: {value!r}")


def _resolve_hint_kind(value: object) -> Optional[HintKind]:
    """The :class:`HintKind` a member or its wire spelling names, or ``None``."""
    if isinstance(value, HintKind):
        return value
    if isinstance(value, str):
        try:
            return HintKind(value)
        except ValueError:
            return None
    return None


def _resolve_correction_kind(value: object) -> Optional[CorrectionKind]:
    """The :class:`CorrectionKind` a member or its wire spelling names, or ``None``."""
    if isinstance(value, CorrectionKind):
        return value
    if isinstance(value, str):
        try:
            return CorrectionKind(value)
        except ValueError:
            return None
    return None


class HintGroup:
    """A nested join pair for ``Leading((a b) c)``."""

    def __init__(self, *tables: str):
        values = tuple(_require_identifier(table, "join pair table") for table in tables)
        if not values:
            raise ValueError("A join pair needs at least one table")
        self.tables = values


class PostgresHintExpression(BaseExpression):
    """One pg_hint_plan hint phrase, carrying typed fields for the formatter."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        kind: HintKind,
        *,
        tables: Tuple[str, ...] = (),
        indexes: Tuple[str, ...] = (),
        groups: Tuple[HintGroup, ...] = (),
        sequence: Tuple[object, ...] = (),
        correction_kind: Optional[CorrectionKind] = None,
        correction_amount: Optional[Union[int, float]] = None,
        workers: Optional[int] = None,
        soft: Optional[bool] = None,
        guc: Optional[str] = None,
        guc_value: Optional[Union[str, int, float]] = None,
    ):
        super().__init__(dialect)
        # Collect only. The hint's vocabulary and identifier contracts are
        # checked by :meth:`check_renderable` when SQL is asked for, so a hint
        # read back from an artifact always builds and a caller error is
        # reported as one.
        self.kind = kind
        self.tables = tuple(tables)
        self.indexes = tuple(indexes)
        self.groups = tuple(groups)
        self.sequence = tuple(sequence)
        self.correction_kind = correction_kind
        self.correction_amount = correction_amount
        self.workers = workers
        self.soft = soft
        self.guc = guc
        self.guc_value = guc_value

    def check_renderable(self) -> None:
        """The phrase's grammar: a hint name and its typed identifiers."""
        if _resolve_hint_kind(self.kind) is None:
            raise TypeError(
                f"{type(self).__name__}.kind must name a HintKind, got {self.kind!r}"
            )
        for field, values in (("table", self.tables), ("index", self.indexes)):
            for value in values:
                _assert_identifier(value, field)
        for group in self.groups:
            if not isinstance(group, HintGroup):
                raise TypeError("groups must contain HintGroup values")
        for item in self.sequence:
            if isinstance(item, str):
                _assert_identifier(item, "join target")
            elif not isinstance(item, HintGroup):
                raise TypeError("sequence must contain table names or HintGroup values")
        if self.correction_kind is not None:
            if _resolve_correction_kind(self.correction_kind) is None:
                raise TypeError("correction_kind must be a CorrectionKind")
            if not isinstance(self.correction_amount, (int, float)) or isinstance(
                self.correction_amount, bool
            ):
                raise TypeError("correction_amount must be a number")
        elif self.correction_amount is not None:
            raise TypeError("correction_amount requires correction_kind")
        if self.workers is not None and (
            not isinstance(self.workers, int) or self.workers < 0
        ):
            raise TypeError("Parallel worker count must be a non-negative integer")
        if self.soft is not None and not isinstance(self.soft, bool):
            raise TypeError(f"soft must be a bool, got {type(self.soft).__name__}")
        if self.guc is not None:
            _assert_identifier(self.guc, "GUC name")
        if self.guc_value is not None and not isinstance(
            self.guc_value, (str, int, float)
        ):
            raise TypeError("guc_value must be a string or number")
        if isinstance(self.guc_value, str):
            _assert_identifier(self.guc_value, "GUC value")

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this hint phrase."""
        return "format_pg_hint"


class PostgresHintedQueryExpression(BaseExpression):
    """A statement expression prefixed with pg_hint_plan hints."""

    def __init__(self, dialect: "SQLDialectBase", query, hints: Tuple[PostgresHintExpression, ...]):
        super().__init__(dialect)
        # Collect only; the non-empty list of hints is checked when SQL is
        # asked for, so an artifact always builds.
        self.query = query
        self.hints = tuple(hints)

    def check_renderable(self) -> None:
        """A hinted query needs at least one hint, and hints are typed hints."""
        if not self.hints:
            raise TypeError("At least one hint is required")
        if any(not isinstance(hint, PostgresHintExpression) for hint in self.hints):
            raise TypeError("hints must contain PostgresHintExpression values")

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_pg_hint_plan_hinted_query"


__all__ = [
    "CorrectionKind",
    "HintGroup",
    "HintKind",
    "PostgresHintExpression",
    "PostgresHintedQueryExpression",
]
