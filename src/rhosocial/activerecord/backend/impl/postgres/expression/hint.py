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
        if not isinstance(kind, HintKind):
            raise TypeError(f"kind must be a HintKind, got {type(kind).__name__}")
        self.kind = kind
        self.tables = tuple(_require_identifier(table, "table") for table in tables)
        self.indexes = tuple(_require_identifier(index, "index") for index in indexes)
        self.groups = tuple(groups)
        if any(not isinstance(group, HintGroup) for group in self.groups):
            raise TypeError("groups must contain HintGroup values")
        self.sequence = tuple(sequence)
        for item in self.sequence:
            if isinstance(item, str):
                _require_identifier(item, "join target")
            elif not isinstance(item, HintGroup):
                raise TypeError("sequence must contain table names or HintGroup values")
        if correction_kind is not None:
            if not isinstance(correction_kind, CorrectionKind):
                raise TypeError("correction_kind must be a CorrectionKind")
            if not isinstance(correction_amount, (int, float)) or isinstance(
                correction_amount, bool
            ):
                raise ValueError("correction_amount must be a number")
        elif correction_amount is not None:
            raise ValueError("correction_amount requires correction_kind")
        if workers is not None and (not isinstance(workers, int) or workers < 0):
            raise ValueError("Parallel worker count must be a non-negative integer")
        if soft is not None and not isinstance(soft, bool):
            raise TypeError(f"soft must be a bool, got {type(soft).__name__}")
        if guc is not None:
            _require_identifier(guc, "GUC name")
        if guc_value is not None and not isinstance(guc_value, (str, int, float)):
            raise TypeError("guc_value must be a string or number")
        if isinstance(guc_value, str):
            _require_identifier(guc_value, "GUC value")
        self.correction_kind = correction_kind
        self.correction_amount = correction_amount
        self.workers = workers
        self.soft = soft
        self.guc = guc
        self.guc_value = guc_value

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this hint phrase."""
        return "format_pg_hint"


class PostgresHintedQueryExpression(BaseExpression):
    """A statement expression prefixed with pg_hint_plan hints."""

    def __init__(self, dialect: "SQLDialectBase", query, hints: Tuple[PostgresHintExpression, ...]):
        super().__init__(dialect)
        values = tuple(hints)
        if not values:
            raise ValueError("At least one hint is required")
        if any(not isinstance(hint, PostgresHintExpression) for hint in values):
            raise TypeError("hints must contain PostgresHintExpression values")
        self.query = query
        self.hints = values

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
