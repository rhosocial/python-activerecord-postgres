# src/rhosocial/activerecord/backend/impl/postgres/expression/hint.py
"""pg_hint_plan hint payloads.

A hint is *text carried inside a comment*, not a statement.  So the payload is
a plain dictionary: the builders return :class:`PostgresHint`, a frozen
dataclass that is **not** an expression node, and the objects on the wire are
the dictionaries :meth:`PostgresHint.as_dict` produces.  Only the dialect
formatter renders them into the ``/*+ ... */`` comment.

Values are validated with Python's identifier contract -- no regex, no
substring checks, no SQL text inspection anywhere in this module.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Sequence, Tuple, Union


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


#: Every key a hint payload carries, in the order the artifact writes them.
#: A dictionary on the wire always has all of them; an absent value is ``None``
#: (or an empty sequence), which is what makes the payload self-describing.
HINT_FIELDS = (
    "kind",
    "tables",
    "indexes",
    "sequence",
    "correction_kind",
    "correction_amount",
    "workers",
    "soft",
    "guc",
    "guc_value",
)


def _require_identifier(value: str, field: str) -> str:
    """Validate an identifier without inspecting its characters by hand."""
    if not isinstance(value, str) or not value.isidentifier():
        raise ValueError(f"Invalid pg_hint_plan {field}: {value!r}")
    return value


def _assert_identifier(value: str, field: str) -> None:
    """The render-time form of :func:`_require_identifier`."""
    if not isinstance(value, str) or not value.isidentifier():
        raise TypeError(f"Invalid pg_hint_plan {field}: {value!r}")


def resolve_hint_kind(value: object) -> Optional[HintKind]:
    """The :class:`HintKind` a member or its wire spelling names, or ``None``."""
    if isinstance(value, HintKind):
        return value
    if isinstance(value, str):
        try:
            return HintKind(value)
        except ValueError:
            return None
    return None


def resolve_correction_kind(value: object) -> Optional[CorrectionKind]:
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
    """A nested join pair for ``Leading((a b) c)``.

    An *input* spelling only.  :meth:`PostgresHint.as_dict` normalises it to
    ``tuple(group.tables)``, so nothing on the wire is a ``HintGroup`` -- the
    payload is a nested array and every reader can parse it.
    """

    def __init__(self, *tables: str):
        values = tuple(_require_identifier(table, "join pair table") for table in tables)
        if not values:
            raise ValueError("A join pair needs at least one table")
        self.tables = values


def _normalise_names(value: object) -> object:
    """A ``tables``/``indexes`` field as a tuple, or the value left to refuse.

    Only a list or tuple is a sequence of names; anything else (a bare string,
    a number) is kept as given so :func:`assert_hint_payload` can report it
    rather than a string being silently split into one entry per character.
    """
    if isinstance(value, (list, tuple)):
        return tuple(value)
    return value


def _normalise_sequence_item(value: object) -> object:
    """One ``sequence`` entry in its payload form: a name or a tuple of names.

    A ``HintGroup`` is the caller's convenience object, and an array is what the
    codec reads back (JSON and XML draw no tuple/array distinction); both become
    the tuple the payload stores.  Anything else is left for
    :func:`assert_hint_payload` to refuse at render time.
    """
    if isinstance(value, HintGroup):
        return tuple(value.tables)
    if isinstance(value, (list, tuple)):
        return tuple(value)
    return value


@dataclass(frozen=True)
class PostgresHint:
    """One pg_hint_plan hint phrase as typed values.

    This is a payload, not a node: it declares no ``format_method``, is never
    registered as an expression kind, and never renders itself.  The builders
    return it, the hinted statement carries its :meth:`as_dict` form, and the
    dialect formatter is the only place a phrase becomes SQL text.
    """

    kind: HintKind
    tables: Tuple[str, ...] = ()
    indexes: Tuple[str, ...] = ()
    sequence: Tuple[Union[str, HintGroup, Sequence[str]], ...] = ()
    correction_kind: Optional[CorrectionKind] = None
    correction_amount: Optional[Union[int, float]] = None
    workers: Optional[int] = None
    soft: Optional[bool] = None
    guc: Optional[str] = None
    guc_value: Optional[Union[str, int, float]] = None

    def as_dict(self) -> dict:
        """The payload a hinted statement carries, with every key present.

        A dictionary -- not the dataclass -- is what travels: core's artifact
        codec encodes only nested nodes, containers, dicts and JSON scalars, so
        a dataclass would reach the scalar encoder and fail.  Keys are always
        all there, ``None`` standing for an absent field, so a reader never has
        to tell "absent" from "empty".
        """
        kind = self.kind.value if isinstance(self.kind, HintKind) else str(self.kind)
        correction_kind = (
            self.correction_kind.value
            if isinstance(self.correction_kind, CorrectionKind)
            else self.correction_kind
        )
        return {
            "kind": kind,
            "tables": _normalise_names(self.tables),
            "indexes": _normalise_names(self.indexes),
            "sequence": tuple(_normalise_sequence_item(item) for item in self.sequence),
            "correction_kind": correction_kind,
            "correction_amount": self.correction_amount,
            "workers": self.workers,
            "soft": self.soft,
            "guc": self.guc,
            "guc_value": self.guc_value,
        }


def hint_payload(hint: object) -> dict:
    """A hint in its canonical dictionary form, with every key present.

    Accepts either a :class:`PostgresHint` (what the builders return) or a
    dictionary (what an artifact decodes).  An artifact hands back arrays where
    the payload held tuples, so nested sequences are normalised to tuples here;
    anything else is left for :func:`assert_hint_payload` to refuse.
    """
    if isinstance(hint, PostgresHint):
        return hint.as_dict()
    if isinstance(hint, dict):
        payload = {
            field: hint.get(field) for field in HINT_FIELDS
        }
        payload["tables"] = _normalise_names(payload["tables"] if payload["tables"] is not None else ())
        payload["indexes"] = _normalise_names(payload["indexes"] if payload["indexes"] is not None else ())
        payload["sequence"] = tuple(
            _normalise_sequence_item(item) for item in (payload["sequence"] or ())
        )
        correction_kind = payload["correction_kind"]
        if isinstance(correction_kind, CorrectionKind):
            payload["correction_kind"] = correction_kind.value
        kind = payload["kind"]
        if isinstance(kind, HintKind):
            payload["kind"] = kind.value
        return payload
    return {"kind": None}


#: Each kind's required parameters, checked by :func:`assert_hint_payload`.
_REQUIRES_TABLES = (
    HintKind.SEQ_SCAN,
    HintKind.TID_SCAN,
    HintKind.INDEX_SCAN,
    HintKind.INDEX_ONLY_SCAN,
    HintKind.BITMAP_SCAN,
    HintKind.NO_SEQ_SCAN,
    HintKind.NO_INDEX_SCAN,
    HintKind.HASH_JOIN,
    HintKind.MERGE_JOIN,
    HintKind.NEST_LOOP,
    HintKind.ROWS,
    HintKind.PARALLEL,
)


def assert_hint_payload(hint: object) -> None:
    """Refuse a hint payload that is not a renderable phrase.

    The grammar: a known :class:`HintKind`, legal identifiers, and the
    parameters that kind cannot be spelled without.  A malformed payload is a
    caller error, so it is reported as ``TypeError``.
    """
    if not isinstance(hint, dict):
        raise TypeError(
            f"a hint payload must be a dict, got {type(hint).__name__}; "
            f"build one with PostgresHint.as_dict()"
        )
    missing = [field for field in HINT_FIELDS if field not in hint]
    if missing:
        raise TypeError(f"hint payload is missing {', '.join(missing)}")
    kind = resolve_hint_kind(hint["kind"])
    if kind is None:
        raise TypeError(
            f"hint payload kind must name a HintKind, got {hint['kind']!r}"
        )
    for field, values in (("table", hint["tables"]), ("index", hint["indexes"])):
        if not isinstance(values, tuple):
            raise TypeError(f"{field}s must be a sequence of identifiers, got {values!r}")
        for value in values:
            _assert_identifier(value, field)
    for item in hint["sequence"]:
        if isinstance(item, str):
            _assert_identifier(item, "join target")
        elif isinstance(item, tuple):
            if not item:
                raise TypeError("sequence join pairs must name at least one table")
            for table in item:
                _assert_identifier(table, "join pair table")
        else:
            raise TypeError("sequence must contain table names or nested join pairs")
    if hint["kind"] in {member.value for member in _REQUIRES_TABLES} and not hint["tables"]:
        raise TypeError(f"{kind.value} needs at least one table")
    if kind is HintKind.LEADING and not hint["sequence"]:
        raise TypeError("Leading needs at least one join target")
    if kind is HintKind.SET and hint["guc"] is None:
        raise TypeError("Set needs a GUC name")
    correction_kind = hint["correction_kind"]
    correction_amount = hint["correction_amount"]
    if correction_kind is not None:
        if resolve_correction_kind(correction_kind) is None:
            raise TypeError("correction_kind must be a CorrectionKind")
        if not isinstance(correction_amount, (int, float)) or isinstance(
            correction_amount, bool
        ):
            raise TypeError("correction_amount must be a number")
    elif correction_amount is not None:
        raise TypeError("correction_amount requires correction_kind")
    if kind is HintKind.ROWS and correction_kind is None:
        raise TypeError("Rows needs a correction operator")
    workers = hint["workers"]
    if workers is not None and (not isinstance(workers, int) or workers < 0):
        raise TypeError("Parallel worker count must be a non-negative integer")
    if hint["soft"] is not None and not isinstance(hint["soft"], bool):
        raise TypeError(f"soft must be a bool, got {type(hint['soft']).__name__}")
    if hint["guc"] is not None:
        _assert_identifier(hint["guc"], "GUC name")
    guc_value = hint["guc_value"]
    if guc_value is not None and not isinstance(guc_value, (str, int, float)):
        raise TypeError("guc_value must be a string or number")
    if isinstance(guc_value, str):
        _assert_identifier(guc_value, "GUC value")
    if kind is HintKind.PARALLEL and workers is None:
        raise TypeError("Parallel needs a worker count")
    if kind is HintKind.SET and guc_value is None:
        raise TypeError("Set needs a GUC value")


__all__ = [
    "CorrectionKind",
    "HINT_FIELDS",
    "HintGroup",
    "HintKind",
    "PostgresHint",
    "assert_hint_payload",
    "hint_payload",
    "resolve_correction_kind",
    "resolve_hint_kind",
]
