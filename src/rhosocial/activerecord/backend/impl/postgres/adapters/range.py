# src/rhosocial/activerecord/backend/impl/postgres/adapters/range.py
"""Range type adapters for PostgreSQL."""

from typing import Any, Dict, List, Optional, Set, Type, Union

from ..types.range import PostgresRange, PostgresMultirange


class PostgresRangeAdapter:
    """PostgreSQL range type adapter.

    This adapter converts between Python PostgresRange objects and
    PostgreSQL range type values.

    Supported range types:
    - int4range: 4-byte integer ranges
    - int8range: 8-byte integer ranges
    - numrange: numeric ranges
    - tsrange: timestamp without time zone ranges
    - tstzrange: timestamp with time zone ranges
    - daterange: date ranges
    """

    RANGE_TYPES: Dict[str, Type] = {
        "int4range": int,
        "int8range": int,
        "numrange": type("Decimal", (), {}),
        "tsrange": type("datetime", (), {}),
        "tstzrange": type("datetime", (), {}),
        "daterange": type("date", (), {}),
    }

    ELEMENT_TYPES: Dict[str, str] = {
        "int4range": "integer",
        "int8range": "bigint",
        "numrange": "numeric",
        "tsrange": "timestamp",
        "tstzrange": "timestamptz",
        "daterange": "date",
    }

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {PostgresRange: {str}}

    def to_database(
        self, value: Union[PostgresRange, tuple, str], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Convert Python value to PostgreSQL range string."""
        if value is None:
            return None

        if isinstance(value, PostgresRange):
            return value.to_postgres_string()

        if isinstance(value, tuple):
            if len(value) == 2:
                lower, upper = value
                return PostgresRange(lower, upper).to_postgres_string()
            elif len(value) == 3:
                lower, upper, bounds = value
                bounds_map = {
                    "[]": (True, True),
                    "[)": (True, False),
                    "(]": (False, True),
                    "()": (False, False),
                }
                if bounds not in bounds_map:
                    raise ValueError(f"Invalid bounds specification: {bounds}")
                return PostgresRange(lower, upper, *bounds_map[bounds]).to_postgres_string()
            else:
                raise ValueError(f"Tuple must have 2 or 3 elements, got {len(value)}")

        if isinstance(value, str):
            value = value.strip()
            if value.lower() == "empty":
                return "empty"
            if value and value[0] in ("[", "(") and value[-1] in ("]", ")"):
                return value
            raise ValueError(f"Invalid range string format: {value}")

        raise TypeError(f"Cannot convert {type(value).__name__} to range")

    def from_database(
        self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> Optional[PostgresRange]:
        """Convert PostgreSQL range value to PostgresRange object."""
        if value is None:
            return None

        if isinstance(value, PostgresRange):
            return value

        try:
            from psycopg.types.range import Range

            if isinstance(value, Range):
                return PostgresRange(
                    lower=value.lower,
                    upper=value.upper,
                    lower_inc=value.bounds and value.bounds[0] == "[",
                    upper_inc=value.bounds and value.bounds[1] == "]",
                )
        except ImportError:
            pass

        if isinstance(value, str):
            return PostgresRange.from_postgres_string(value)

        if isinstance(value, tuple) and len(value) == 2:
            return PostgresRange(value[0], value[1])

        raise TypeError(f"Cannot convert {type(value).__name__} to PostgresRange")

    def to_database_batch(
        self, values: List[Any], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert values to database format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(
        self, values: List[Any], target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> List[Optional[PostgresRange]]:
        """Batch convert values from database format."""
        return [self.from_database(v, target_type, options) for v in values]


class PostgresMultirangeAdapter:
    """PostgreSQL multirange type adapter (PostgreSQL 14+)."""

    MULTIRANGE_TYPES: Dict[str, Type] = {
        "int4multirange": int,
        "int8multirange": int,
        "nummultirange": type("Decimal", (), {}),
        "tsmultirange": type("datetime", (), {}),
        "tstzmultirange": type("datetime", (), {}),
        "datemultirange": type("date", (), {}),
    }

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        return {PostgresMultirange: {str}}

    def to_database(
        self,
        value: Union[PostgresMultirange, List[PostgresRange], str],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Convert to PostgreSQL multirange string."""
        if value is None:
            return None

        if isinstance(value, PostgresMultirange):
            return value.to_postgres_string()

        if isinstance(value, list):
            return PostgresMultirange(value).to_postgres_string()

        if isinstance(value, str):
            return value

        raise TypeError(f"Cannot convert {type(value).__name__} to multirange")

    def from_database(
        self, value: Any, target_type: Type, options: Optional[Dict[str, Any]] = None
    ) -> Optional[PostgresMultirange]:
        """Convert from PostgreSQL multirange value."""
        if value is None:
            return None

        if isinstance(value, PostgresMultirange):
            return value

        if isinstance(value, str):
            return PostgresMultirange.from_postgres_string(value)

        try:
            from psycopg.types.range import Multirange

            if isinstance(value, Multirange):
                ranges = [
                    PostgresRange(
                        lower=r.lower,
                        upper=r.upper,
                        lower_inc=r.bounds and r.bounds[0] == "[",
                        upper_inc=r.bounds and r.bounds[1] == "]",
                    )
                    for r in value
                ]
                return PostgresMultirange(ranges=ranges)
        except ImportError:
            pass

        raise TypeError(f"Cannot convert {type(value).__name__} to PostgresMultirange")
