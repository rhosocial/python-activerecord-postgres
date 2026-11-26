# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/index_type.py
"""PostgreSQL index access-method vocabulary.

The node keeps ``index_type`` a **string** — callers pass ``"gin"``, the driver
writes ``USING gin``, and the four-language artifact carries that string. What
this enum adds is the closed set of spellings the backend knows how to render:
a caller can no longer hand an arbitrary word to ``CREATE INDEX``.

Extension access methods (HNSW, IVFFLAT, BLOOM, PGROONGA) are in the same enum because
they are legal ``USING`` clauses; ``is_native`` / ``is_extension`` keep the two
groups distinguishable without a second vocabulary.
"""

from enum import Enum
from typing import Union


class PostgresIndexType(str, Enum):
    """Every ``USING`` method this backend can render.

    ``__str__`` is pinned to ``str.__str__`` for the same reason the other
    ``(str, Enum)`` classes in this backend pin it: the artifact and every
    f-string spell a member with the value it carries, not with the enum's
    ``Type.NAME`` repr.
    """

    __str__ = str.__str__

    # Native PostgreSQL access methods.
    BTREE = "btree"
    HASH = "hash"
    GIST = "gist"
    GIN = "gin"
    SPGIST = "spgist"
    BRIN = "brin"

    # Extension access methods. Availability still depends on the extension
    # being installed; this enum only says the spelling is renderable.
    HNSW = "hnsw"
    IVFFLAT = "ivfflat"
    BLOOM = "bloom"
    PGROONGA = "pgroonga"

    @property
    def is_native(self) -> bool:
        return self in _NATIVE

    @property
    def is_extension(self) -> bool:
        return self in _EXTENSION

    @classmethod
    def normalize(cls, value: Union[str, "PostgresIndexType"]) -> "PostgresIndexType":
        """Resolve a member, a wire word, or a case-variant spelling.

        ``sp-gist`` and ``sp_gist`` are accepted for SP-GiST and ``ivf_flat``
        for IVFFLAT, because callers spell those names more than one way.
        """
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise ValueError(f"Unsupported index type: {value!r}")
        candidate = value.strip().lower().replace("-", "").replace("_", "")
        try:
            return cls(candidate)
        except ValueError:
            supported = ", ".join(member.value for member in cls)
            raise ValueError(
                f"Unsupported index type {value!r}; expected one of: {supported}"
            ) from None

    @classmethod
    def coerce(cls, value: Union[str, "PostgresIndexType"]) -> str:
        """Validate *value* and return it as a string.

        The caller's own spelling is preserved so rendering stays byte-identical
        with the existing SQL (``"GIN"`` stays ``GIN``, ``"gin"`` stays ``gin``).
        A member is returned as its value. Nothing here uppercases the word.
        """
        member = cls.normalize(value)
        if not isinstance(value, str) or value.strip().lower() != member.value:
            # An alias such as "sp-gist" cannot be echoed back into SQL; only
            # the canonical wire word is renderable.
            return member.value
        return value


_NATIVE = frozenset(
    (
        PostgresIndexType.BTREE,
        PostgresIndexType.HASH,
        PostgresIndexType.GIST,
        PostgresIndexType.GIN,
        PostgresIndexType.SPGIST,
        PostgresIndexType.BRIN,
    )
)

_EXTENSION = frozenset(
    (
        PostgresIndexType.HNSW,
        PostgresIndexType.IVFFLAT,
        PostgresIndexType.BLOOM,
        PostgresIndexType.PGROONGA,
    )
)

__all__ = ["PostgresIndexType"]
