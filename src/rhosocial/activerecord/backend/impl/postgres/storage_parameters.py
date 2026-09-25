# src/rhosocial/activerecord/backend/impl/postgres/storage_parameters.py
"""PostgreSQL relation storage parameters (reloptions).

A materialized view is stored as a regular heap relation: the server maps
``RELKIND_MATVIEW`` onto ``RELOPT_KIND_HEAP`` in ``fillRelOptions()``
(``src/backend/access/common/reloptions.c``). Therefore the parameters accepted
by ``CREATE MATERIALIZED VIEW ... WITH (...)`` / ``ALTER MATERIALIZED VIEW ...
SET (...)`` are exactly the *standard heap relation options* listed in
``stdRdOptionsTab`` in the same file.

That set is enumerated by :class:`PostgresStorageParameter`, and every MV
storage-parameter expression validates against it — no pattern matching on
user input.

Scope notes
-----------
* Namespaced options (``toast.autovacuum_enabled`` and friends) are not part of
  this enum; pass them with ``allow_unlisted_storage_parameters=True``.
* ``USING method`` table access methods may register additional options
  (``add_reloption_kind()`` / ``add_*_reloption()`` are exported server APIs,
  e.g. Citus columnar adds ``compresslevel``). Those also require
  ``allow_unlisted_storage_parameters=True``.
* ``min_version`` is only populated where PostgreSQL documents a specific
  introduction version; ``None`` means "no client-side gate, the server
  validates the value".
"""
from enum import Enum
from typing import Any, Dict, Optional, Tuple


__all__ = [
    "PostgresStorageParameter",
    "PostgresStorageParameterValueType",
    "resolve_storage_parameter",
    "validate_storage_parameters",
]


class PostgresStorageParameterValueType(Enum):
    """Value type of a storage parameter, mirroring PostgreSQL ``relopt_type``."""

    INT = "int"
    REAL = "real"
    BOOL = "bool"
    TERNARY = "ternary"
    ENUM = "enum"
    STRING = "string"


class PostgresStorageParameter(str, Enum):
    """Standard heap relation options accepted by materialized views.

    The string value is the exact PostgreSQL option name (lower case, as stored
    in ``pg_class.reloptions``).

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import (
        ...     PostgresStorageParameter,
        ... )
        >>> PostgresStorageParameter.FILLFACTOR.value
        'fillfactor'
        >>> PostgresStorageParameter("autovacuum_enabled") is \\
        ...     PostgresStorageParameter.AUTOVACUUM_ENABLED
        True
    """

    FILLFACTOR = "fillfactor"
    TOAST_TUPLE_TARGET = "toast_tuple_target"
    PARALLEL_WORKERS = "parallel_workers"
    USER_CATALOG_TABLE = "user_catalog_table"
    VACUUM_INDEX_CLEANUP = "vacuum_index_cleanup"
    VACUUM_TRUNCATE = "vacuum_truncate"
    VACUUM_MAX_EAGER_FREEZE_FAILURE_RATE = "vacuum_max_eager_freeze_failure_rate"

    AUTOVACUUM_ENABLED = "autovacuum_enabled"
    AUTOVACUUM_PARALLEL_WORKERS = "autovacuum_parallel_workers"
    AUTOVACUUM_VACUUM_THRESHOLD = "autovacuum_vacuum_threshold"
    AUTOVACUUM_VACUUM_MAX_THRESHOLD = "autovacuum_vacuum_max_threshold"
    AUTOVACUUM_VACUUM_INSERT_THRESHOLD = "autovacuum_vacuum_insert_threshold"
    AUTOVACUUM_ANALYZE_THRESHOLD = "autovacuum_analyze_threshold"
    AUTOVACUUM_VACUUM_COST_LIMIT = "autovacuum_vacuum_cost_limit"
    AUTOVACUUM_VACUUM_COST_DELAY = "autovacuum_vacuum_cost_delay"
    AUTOVACUUM_VACUUM_SCALE_FACTOR = "autovacuum_vacuum_scale_factor"
    AUTOVACUUM_VACUUM_INSERT_SCALE_FACTOR = "autovacuum_vacuum_insert_scale_factor"
    AUTOVACUUM_ANALYZE_SCALE_FACTOR = "autovacuum_analyze_scale_factor"
    AUTOVACUUM_FREEZE_MIN_AGE = "autovacuum_freeze_min_age"
    AUTOVACUUM_FREEZE_MAX_AGE = "autovacuum_freeze_max_age"
    AUTOVACUUM_FREEZE_TABLE_AGE = "autovacuum_freeze_table_age"
    AUTOVACUUM_MULTIXACT_FREEZE_MIN_AGE = "autovacuum_multixact_freeze_min_age"
    AUTOVACUUM_MULTIXACT_FREEZE_MAX_AGE = "autovacuum_multixact_freeze_max_age"
    AUTOVACUUM_MULTIXACT_FREEZE_TABLE_AGE = "autovacuum_multixact_freeze_table_age"
    LOG_AUTOVACUUM_MIN_DURATION = "log_autovacuum_min_duration"
    LOG_AUTOANALYZE_MIN_DURATION = "log_autoanalyze_min_duration"

    @property
    def value_type(self) -> PostgresStorageParameterValueType:
        """The value type PostgreSQL expects for this parameter."""
        return _VALUE_TYPES[self]

    @property
    def min_version(self) -> Optional[Tuple[int, int, int]]:
        """First PostgreSQL version accepting this parameter, when documented.

        ``None`` means the parameter has been accepted since materialized views
        exist, or that no client-side version gate is applied.
        """
        return _MIN_VERSIONS.get(self)

    @property
    def enum_values(self) -> Optional[Tuple[str, ...]]:
        """Accepted values for ``ENUM`` parameters, ``None`` for other types."""
        return _ENUM_VALUES.get(self)


_VALUE_TYPES: Dict[PostgresStorageParameter, PostgresStorageParameterValueType] = {
    PostgresStorageParameter.FILLFACTOR: PostgresStorageParameterValueType.INT,
    PostgresStorageParameter.TOAST_TUPLE_TARGET: PostgresStorageParameterValueType.INT,
    PostgresStorageParameter.PARALLEL_WORKERS: PostgresStorageParameterValueType.INT,
    PostgresStorageParameter.USER_CATALOG_TABLE: PostgresStorageParameterValueType.BOOL,
    PostgresStorageParameter.VACUUM_INDEX_CLEANUP: PostgresStorageParameterValueType.ENUM,
    PostgresStorageParameter.VACUUM_TRUNCATE: PostgresStorageParameterValueType.TERNARY,
    PostgresStorageParameter.VACUUM_MAX_EAGER_FREEZE_FAILURE_RATE: (
        PostgresStorageParameterValueType.REAL
    ),
    PostgresStorageParameter.AUTOVACUUM_ENABLED: PostgresStorageParameterValueType.TERNARY,
    PostgresStorageParameter.AUTOVACUUM_PARALLEL_WORKERS: PostgresStorageParameterValueType.INT,
    PostgresStorageParameter.AUTOVACUUM_VACUUM_THRESHOLD: PostgresStorageParameterValueType.INT,
    PostgresStorageParameter.AUTOVACUUM_VACUUM_MAX_THRESHOLD: PostgresStorageParameterValueType.INT,
    PostgresStorageParameter.AUTOVACUUM_VACUUM_INSERT_THRESHOLD: (
        PostgresStorageParameterValueType.INT
    ),
    PostgresStorageParameter.AUTOVACUUM_ANALYZE_THRESHOLD: PostgresStorageParameterValueType.INT,
    PostgresStorageParameter.AUTOVACUUM_VACUUM_COST_LIMIT: PostgresStorageParameterValueType.INT,
    PostgresStorageParameter.AUTOVACUUM_VACUUM_COST_DELAY: PostgresStorageParameterValueType.REAL,
    PostgresStorageParameter.AUTOVACUUM_VACUUM_SCALE_FACTOR: (
        PostgresStorageParameterValueType.REAL
    ),
    PostgresStorageParameter.AUTOVACUUM_VACUUM_INSERT_SCALE_FACTOR: (
        PostgresStorageParameterValueType.REAL
    ),
    PostgresStorageParameter.AUTOVACUUM_ANALYZE_SCALE_FACTOR: (
        PostgresStorageParameterValueType.REAL
    ),
    PostgresStorageParameter.AUTOVACUUM_FREEZE_MIN_AGE: PostgresStorageParameterValueType.INT,
    PostgresStorageParameter.AUTOVACUUM_FREEZE_MAX_AGE: PostgresStorageParameterValueType.INT,
    PostgresStorageParameter.AUTOVACUUM_FREEZE_TABLE_AGE: PostgresStorageParameterValueType.INT,
    PostgresStorageParameter.AUTOVACUUM_MULTIXACT_FREEZE_MIN_AGE: (
        PostgresStorageParameterValueType.INT
    ),
    PostgresStorageParameter.AUTOVACUUM_MULTIXACT_FREEZE_MAX_AGE: (
        PostgresStorageParameterValueType.INT
    ),
    PostgresStorageParameter.AUTOVACUUM_MULTIXACT_FREEZE_TABLE_AGE: (
        PostgresStorageParameterValueType.INT
    ),
    PostgresStorageParameter.LOG_AUTOVACUUM_MIN_DURATION: PostgresStorageParameterValueType.INT,
    PostgresStorageParameter.LOG_AUTOANALYZE_MIN_DURATION: PostgresStorageParameterValueType.INT,
}

# Only versions PostgreSQL documents explicitly; None elsewhere.
_MIN_VERSIONS: Dict[PostgresStorageParameter, Optional[Tuple[int, int, int]]] = {
    PostgresStorageParameter.PARALLEL_WORKERS: (11, 0, 0),
    PostgresStorageParameter.VACUUM_TRUNCATE: (12, 0, 0),
    PostgresStorageParameter.AUTOVACUUM_VACUUM_MAX_THRESHOLD: (12, 0, 0),
    PostgresStorageParameter.AUTOVACUUM_VACUUM_INSERT_THRESHOLD: (13, 0, 0),
    PostgresStorageParameter.AUTOVACUUM_VACUUM_INSERT_SCALE_FACTOR: (13, 0, 0),
    PostgresStorageParameter.LOG_AUTOANALYZE_MIN_DURATION: (15, 0, 0),
    PostgresStorageParameter.AUTOVACUUM_PARALLEL_WORKERS: (16, 0, 0),
    PostgresStorageParameter.VACUUM_MAX_EAGER_FREEZE_FAILURE_RATE: (18, 0, 0),
}

_ENUM_VALUES: Dict[PostgresStorageParameter, Tuple[str, ...]] = {
    PostgresStorageParameter.VACUUM_INDEX_CLEANUP: ("auto", "enabled", "disabled"),
}


def resolve_storage_parameter(name: Any) -> Optional[PostgresStorageParameter]:
    """Resolve a storage parameter from an enum member or its PostgreSQL name.

    Args:
        name: A :class:`PostgresStorageParameter` member or its string value.

    Returns:
        The matching member, or ``None`` when the name is not a standard heap
        relation option.
    """
    if isinstance(name, PostgresStorageParameter):
        return name
    if isinstance(name, str):
        try:
            return PostgresStorageParameter(name)
        except ValueError:
            return None
    return None


def validate_storage_parameters(
    names: Any,
    field_name: str,
    allow_unlisted: bool = False,
) -> Tuple[PostgresStorageParameter, ...]:
    """Validate storage parameter names against :class:`PostgresStorageParameter`.

    Args:
        names: Iterable of enum members and/or PostgreSQL option names.
        field_name: Field name used in error messages.
        allow_unlisted: Accept names outside the standard heap relation option
            set (namespaced ``toast.*`` options, or options registered by a
            table access method). The server remains the authority.

    Returns:
        The resolved members, in input order. When ``allow_unlisted`` is set,
        unlisted names are dropped from the result.

    Raises:
        ValueError: If a name is not a standard storage parameter and
            ``allow_unlisted`` is False.
        TypeError: If ``names`` is not an iterable of names.
    """
    if isinstance(names, (str, bytes)) or not hasattr(names, "__iter__"):
        raise TypeError(f"{field_name} must be an iterable of storage parameter names")

    resolved = []
    for name in names:
        parameter = resolve_storage_parameter(name)
        if parameter is None:
            if allow_unlisted:
                continue
            raise ValueError(
                f"{field_name} contains an unknown PostgreSQL storage parameter: {name!r}. "
                f"Pass allow_unlisted=True to forward names registered by a table "
                f"access method or namespaced under 'toast.'."
            )
        resolved.append(parameter)
    return tuple(resolved)
