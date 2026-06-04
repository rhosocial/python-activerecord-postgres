# src/rhosocial/activerecord/backend/impl/postgres/collation.py
"""
PostgreSQL collation names supported by the dialect whitelist.
"""

from enum import Enum
from typing import Optional, Tuple


class PostgresCollation(Enum):
    """Common PostgreSQL collations for expression-level COLLATE."""

    C = "C"
    POSIX = "POSIX"
    UCS_BASIC = "ucs_basic"
    UND_X_ICU = "und-x-icu"


_BUILTIN_COLLATIONS = {
    PostgresCollation.C.value,
    PostgresCollation.POSIX.value,
    PostgresCollation.UCS_BASIC.value,
}

_VERSION_SENSITIVE_COLLATIONS = {
    PostgresCollation.UND_X_ICU.value: (10, 0, 0),
}

_POSTGRES_COLLATIONS = _BUILTIN_COLLATIONS | set(_VERSION_SENSITIVE_COLLATIONS)


def validate_postgres_collation_name(
    name: str,
    version: Optional[Tuple[int, int, int]] = None,
) -> str:
    if name not in _POSTGRES_COLLATIONS:
        raise ValueError(f"Unsupported PostgreSQL collation: {name!r}")
    required_version = _VERSION_SENSITIVE_COLLATIONS.get(name)
    if required_version is not None and version is not None and version < required_version:
        raise ValueError(
            f"PostgreSQL collation requires PostgreSQL {required_version[0]}.{required_version[1]}+: {name!r}"
        )
    return name
