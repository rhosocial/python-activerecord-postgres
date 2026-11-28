# src/rhosocial/activerecord/backend/impl/postgres/expression/query_parts/hint.py
"""pg_hint_plan hint payloads.

A hint is *text carried inside a comment*, not a statement.  So the payload is
a plain dictionary: the builders return a ``dict`` with every key present (the
``HintPayload`` ``TypedDict`` states that shape to a type checker), and only the
dialect formatter renders it into the ``/*+ ... */`` comment.  There is no
payload class: a hint declares no ``format_method`` and is never a node.

A hint *target* is a relation reference, not a free string: the caller passes
the :class:`~...objects.Table` (or a
:class:`~...sources.NamedRelationRef`), and :func:`hint_target` turns it into
the one spelling pg_hint_plan matches -- the alias when there is one, else the
bare, unqualified relation name.  Resolution happens here, when the payload is
built; a payload read back from an artifact is already resolved and is never
resolved again.

Values are validated with Python's identifier contract -- no regex, no
substring checks, no SQL text inspection anywhere in this module.
"""

from enum import Enum
from typing import Optional, Sequence, Tuple, TypedDict, Union, cast

from rhosocial.activerecord.backend.expression.objects import (
    ForeignTable,
    MaterializedView,
    SchemaObject,
    Table,
    View,
)
from rhosocial.activerecord.backend.expression.sources import NamedRelationRef

#: What a caller may name a relation with: the object itself, the query-side
#: reference carrying its alias, or a raw string for a pseudo name
#: (``*VALUES*``, ``ANY_subquery``) or an alias the caller already knows.
HintTarget = Union[str, SchemaObject, NamedRelationRef]


class HintKind(str, Enum):
    """The pg_hint_plan hint name. A member *is* the SQL spelling."""

    __str__ = str.__str__

    SEQ_SCAN = "SeqScan"
    TID_SCAN = "TidScan"
    INDEX_SCAN = "IndexScan"
    INDEX_ONLY_SCAN = "IndexOnlyScan"
    BITMAP_SCAN = "BitmapScan"
    INDEX_SCAN_REGEXP = "IndexScanRegexp"
    INDEX_ONLY_SCAN_REGEXP = "IndexOnlyScanRegexp"
    BITMAP_SCAN_REGEXP = "BitmapScanRegexp"
    NO_SEQ_SCAN = "NoSeqScan"
    NO_TID_SCAN = "NoTidScan"
    NO_INDEX_SCAN = "NoIndexScan"
    NO_INDEX_ONLY_SCAN = "NoIndexOnlyScan"
    NO_BITMAP_SCAN = "NoBitmapScan"
    DISABLE_INDEX = "DisableIndex"
    HASH_JOIN = "HashJoin"
    MERGE_JOIN = "MergeJoin"
    NEST_LOOP = "NestLoop"
    NO_HASH_JOIN = "NoHashJoin"
    NO_MERGE_JOIN = "NoMergeJoin"
    NO_NEST_LOOP = "NoNestLoop"
    MEMOIZE = "Memoize"
    NO_MEMOIZE = "NoMemoize"
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
#: For the ``*Regexp`` kinds, ``indexes`` holds the POSIX patterns instead of
#: index names -- they occupy the same slot after the resolved target.
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


def _render_name(name: str, need_quote: bool) -> str:
    """The wire spelling of one identifier, quoted exactly when requested.

    Quoting is not cosmetic: a quoted name matches the catalogue entry exactly,
    so a mixed-case internal name survives, while a lowercased spelling of it
    does not.  A name the caller says needs no quote is emitted as given.
    """
    if not need_quote:
        return name
    return '"' + name.replace('"', '""') + '"'


#: A hint phrase lives inside ``/*+ ... */``, so a name may never close it.
_HINT_COMMENT_END = "*/"


def _is_hint_name(value: str) -> bool:
    """Whether ``value`` is one hint target: a bare word or a quoted name.

    The quoted form is what :func:`hint_target` emits for a relation whose
    ``name_need_quote`` is true, so the check must accept it.  A name may not
    contain a double quote of its own (the spelling cannot nest) or the comment
    terminator, which would end the phrase early.
    """
    if value.isidentifier():
        return True
    if len(value) < 3 or value[0] != '"' or value[-1] != '"':
        return False
    inner = value[1:-1]
    return '"' not in inner and _HINT_COMMENT_END not in inner and bool(inner.strip())


def hint_target(target: HintTarget) -> str:
    """The one spelling pg_hint_plan matches for ``target``.

    A hint target is a relation reference.  Measured behaviour on PostgreSQL
    18 with pg_hint_plan 1.8.1:

    * an alias wins -- ``FROM t1 a`` matches ``a`` and ignores ``t1``;
    * a schema-qualified name is not even parsed, so the schema is never
      emitted; a bare name still matches a schema-qualified reference;
    * same-named tables in one query are ambiguous and match neither, which
      is why the alias must be used;
    * quoting is exact -- ``"Mixed"`` matches, ``mixed`` does not.

    A :class:`~...sources.NamedRelationRef` yields its alias when set,
    otherwise the relation's own name.  A table (node/edge included),
    materialized view or foreign table yields its bare name, quoted exactly
    when the object asks for it.  A view cannot be hinted: pg_hint_plan
    reports the phrase as unused and the base table must be named instead.
    Any other schema object (an index, sequence, type, ...) is not a relation
    and is refused.

    A ``str`` is returned unchanged: that is the escape hatch for a pseudo
    name (``*VALUES*``, ``ANY_subquery``) or an alias the caller already knows.
    """
    if isinstance(target, str):
        return target
    if isinstance(target, NamedRelationRef):
        if target.alias:
            return _render_name(target.alias, target.alias_need_quote)
        return hint_target(target.relation)
    if isinstance(target, View):
        raise TypeError(
            f"pg_hint_plan cannot hint a view ({target!r}): it reports the "
            f"phrase as unused. Hint the tables of the query the view expands "
            f"to instead"
        )
    if isinstance(target, (Table, MaterializedView, ForeignTable)):
        return _render_name(target.name, target.name_need_quote)
    if isinstance(target, SchemaObject):
        raise TypeError(
            f"pg_hint_plan targets name a relation; {type(target).__name__} "
            f"({target!r}) is not a relation that can be hinted"
        )
    raise TypeError(
        f"a pg_hint_plan target must be a str, SchemaObject or "
        f"NamedRelationRef, got {type(target).__name__}"
    )


def _assert_identifier(value: str, field: str) -> None:
    """The render-time form of :func:`_is_hint_name`."""
    if not isinstance(value, str) or not _is_hint_name(value):
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

    An *input* spelling only.  :func:`make_hint_payload` normalises it to
    ``tuple(group.tables)``, so nothing on the wire is a ``HintGroup`` -- the
    payload is a nested array and every reader can parse it.  Its entries are
    hint targets and are resolved through :func:`hint_target`, the same as a
    top-level target.
    """

    def __init__(self, *tables: HintTarget):
        values = tuple(hint_target(table) for table in tables)
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


#: One hint phrase as its wire/storage shape. A ``TypedDict`` because the
#: payload is never instantiated as a class: the builders return a plain
#: ``dict`` with every key present, and every reader (the formatter, the
#: artifact codec) receives a mapping.
class HintPayload(TypedDict):
    kind: str
    tables: Tuple[str, ...]
    indexes: Tuple[str, ...]
    sequence: Tuple[Union[str, Tuple[str, ...]], ...]
    correction_kind: Optional[str]
    correction_amount: Optional[Union[int, float]]
    workers: Optional[int]
    soft: Optional[bool]
    guc: Optional[str]
    guc_value: Optional[Union[str, int, float]]


def make_hint_payload(
    kind: Union[HintKind, str],
    *,
    tables: Sequence[str] = (),
    indexes: Sequence[str] = (),
    sequence: Sequence[Union[str, HintGroup, Sequence[str]]] = (),
    correction_kind: Optional[CorrectionKind] = None,
    correction_amount: Optional[Union[int, float]] = None,
    workers: Optional[int] = None,
    soft: Optional[bool] = None,
    guc: Optional[str] = None,
    guc_value: Optional[Union[str, int, float]] = None,
) -> HintPayload:
    """One hint phrase as a plain dictionary with every key present.

    This only *collects* -- an absent field is ``None`` (or ``()``), and the
    values are stored as given.  Whether the phrase is renderable is
    :func:`assert_hint_payload`'s answer, at render time.

    ``HintKind``/``CorrectionKind`` members are written as the words they
    carry, and a :class:`HintGroup` becomes the tuple of the names it holds:
    the payload is language-neutral data, so no enum or host object travels.
    """
    return cast(
        HintPayload,
        {
            "kind": kind.value if isinstance(kind, HintKind) else str(kind),
            "tables": _normalise_names(tables),
            "indexes": _normalise_names(indexes),
            "sequence": tuple(_normalise_sequence_item(item) for item in sequence),
            "correction_kind": (
                correction_kind.value
                if isinstance(correction_kind, CorrectionKind)
                else correction_kind
            ),
            "correction_amount": correction_amount,
            "workers": workers,
            "soft": soft,
            "guc": guc,
            "guc_value": guc_value,
        },
    )


def hint_payload(hint: object) -> dict:
    """A hint in its canonical dictionary form, with every key present.

    Accepts the dictionary an artifact decodes (JSON and XML draw no
    tuple/array distinction, so the payload's arrays may come back as lists).
    Nested sequences are normalised to tuples here; anything else is left for
    :func:`assert_hint_payload` to refuse.
    """
    if not isinstance(hint, dict):
        # Not a payload at all; leave the refusal to assert_hint_payload.
        return {"kind": None}
    payload = {field: hint.get(field) for field in HINT_FIELDS}
    payload["tables"] = _normalise_names(payload["tables"] if payload["tables"] is not None else ())
    payload["indexes"] = _normalise_names(
        payload["indexes"] if payload["indexes"] is not None else ()
    )
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


def normalise_hints(hints: Sequence[object]) -> Tuple[dict, ...]:
    """A hinted statement's constructor argument, canonicalised.

    An artifact decodes the payload arrays as lists; canonicalising here keeps
    a rebuilt statement's ``get_params()`` equal to the encoded one, and gives
    the renderer the tuple shape it reads.
    """
    return tuple(hint_payload(hint) for hint in hints)


#: Each kind's required parameters, checked by :func:`assert_hint_payload`.
_REQUIRES_TABLES = (
    HintKind.SEQ_SCAN,
    HintKind.TID_SCAN,
    HintKind.INDEX_SCAN,
    HintKind.INDEX_ONLY_SCAN,
    HintKind.BITMAP_SCAN,
    HintKind.INDEX_SCAN_REGEXP,
    HintKind.INDEX_ONLY_SCAN_REGEXP,
    HintKind.BITMAP_SCAN_REGEXP,
    HintKind.NO_SEQ_SCAN,
    HintKind.NO_TID_SCAN,
    HintKind.NO_INDEX_SCAN,
    HintKind.NO_INDEX_ONLY_SCAN,
    HintKind.NO_BITMAP_SCAN,
    HintKind.DISABLE_INDEX,
    HintKind.HASH_JOIN,
    HintKind.MERGE_JOIN,
    HintKind.NEST_LOOP,
    HintKind.NO_HASH_JOIN,
    HintKind.NO_MERGE_JOIN,
    HintKind.NO_NEST_LOOP,
    HintKind.MEMOIZE,
    HintKind.NO_MEMOIZE,
    HintKind.ROWS,
    HintKind.PARALLEL,
)

#: Kinds whose ``indexes`` field carries POSIX regexps rather than index names.
_REGEXP_KINDS = frozenset(
    {
        HintKind.INDEX_SCAN_REGEXP,
        HintKind.INDEX_ONLY_SCAN_REGEXP,
        HintKind.BITMAP_SCAN_REGEXP,
    }
)


def _assert_hint_pattern(value: str) -> None:
    """A regexp argument is free text, but may never end the hint comment."""
    if not isinstance(value, str) or not value or _HINT_COMMENT_END in value:
        raise TypeError(f"Invalid pg_hint_plan regexp: {value!r}")


def assert_hint_payload(hint: object) -> None:
    """Refuse a hint payload that is not a renderable phrase.

    The grammar: a known :class:`HintKind`, legal identifiers, and the
    parameters that kind cannot be spelled without.  A malformed payload is a
    caller error, so it is reported as ``TypeError``.
    """
    if not isinstance(hint, dict):
        raise TypeError(
            f"a hint payload must be a dict, got {type(hint).__name__}; "
            f"build one with make_hint_payload()"
        )
    missing = [field for field in HINT_FIELDS if field not in hint]
    if missing:
        raise TypeError(f"hint payload is missing {', '.join(missing)}")
    kind = resolve_hint_kind(hint["kind"])
    if kind is None:
        raise TypeError(f"hint payload kind must name a HintKind, got {hint['kind']!r}")
    for field, values in (("table", hint["tables"]), ("index", hint["indexes"])):
        if not isinstance(values, tuple):
            raise TypeError(f"{field}s must be a sequence of identifiers, got {values!r}")
        for value in values:
            if field == "index" and kind in _REGEXP_KINDS:
                _assert_hint_pattern(value)
            else:
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
        if not isinstance(correction_amount, (int, float)) or isinstance(correction_amount, bool):
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
    "HintPayload",
    "HintTarget",
    "assert_hint_payload",
    "hint_payload",
    "hint_target",
    "make_hint_payload",
    "normalise_hints",
    "resolve_correction_kind",
    "resolve_hint_kind",
]
