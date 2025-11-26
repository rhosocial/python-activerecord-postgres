# src/rhosocial/activerecord/backend/impl/postgres/types.py
from typing import Dict, Optional, Any

from rhosocial.activerecord.backend.dialect import TypeMapping
from rhosocial.activerecord.backend import DatabaseType
from rhosocial.activerecord.backend.helpers import format_with_length

def format_decimal(base_type: str, params: Dict[str, Any]) -> str:
    """Formats a decimal type with precision and scale."""
    precision = params.get("precision")
    scale = params.get("scale")

    if precision is not None:
        if scale is not None:
            return f"{base_type}({precision}, {scale})"
        return f"{base_type}({precision})"
    return base_type


# postgres type mapping configuration
POSTGRES_TYPE_MAPPINGS: Dict[DatabaseType, TypeMapping] = {
    DatabaseType.TINYINT: TypeMapping("SMALLINT"),  # postgres has no TINYINT
    DatabaseType.SMALLINT: TypeMapping("SMALLINT"),
    DatabaseType.INTEGER: TypeMapping("INTEGER"),
    DatabaseType.BIGINT: TypeMapping("BIGINT"),
    DatabaseType.FLOAT: TypeMapping("REAL"),
    DatabaseType.DOUBLE: TypeMapping("DOUBLE PRECISION"),
    DatabaseType.DECIMAL: TypeMapping("NUMERIC", format_decimal),
    DatabaseType.CHAR: TypeMapping("CHAR", format_with_length),
    DatabaseType.VARCHAR: TypeMapping("VARCHAR", format_with_length),
    DatabaseType.TEXT: TypeMapping("TEXT"),
    DatabaseType.DATE: TypeMapping("DATE"),
    DatabaseType.TIME: TypeMapping("TIME"),
    DatabaseType.DATETIME: TypeMapping("TIMESTAMP"),
    DatabaseType.TIMESTAMP: TypeMapping("TIMESTAMP WITH TIME ZONE"),
    DatabaseType.BLOB: TypeMapping("BYTEA"),
    DatabaseType.BOOLEAN: TypeMapping("BOOLEAN"),
    DatabaseType.UUID: TypeMapping("UUID"),
    DatabaseType.JSON: TypeMapping("JSONB"),  # Prefer JSONB over JSON for better performance
    DatabaseType.ARRAY: TypeMapping("JSONB"),  # Use JSONB for generic arrays
    DatabaseType.CUSTOM: TypeMapping("TEXT"),
}


class PostgresTypes:
    """postgres specific type constants"""
    # Numeric Types
    SMALLSERIAL = "SMALLSERIAL"
    SERIAL = "SERIAL"
    BIGSERIAL = "BIGSERIAL"
    MONEY = "MONEY"

    # Character Types
    VARCHAR_NO_LIMIT = "VARCHAR"  # without length limit
    CHAR_NO_LIMIT = "CHAR"  # without length limit

    # Binary Types
    BYTEA = "BYTEA"

    # Date/Time Types
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_TZ = "TIMESTAMP WITH TIME ZONE"
    TIME_TZ = "TIME WITH TIME ZONE"
    INTERVAL = "INTERVAL"

    # Network Address Types
    INET = "INET"
    CIDR = "CIDR"
    MACADDR = "MACADDR"
    MACADDR8 = "MACADDR8"

    # Geometric Types
    POINT = "POINT"
    LINE = "LINE"
    LSEG = "LSEG"
    BOX = "BOX"
    PATH = "PATH"
    POLYGON = "POLYGON"
    CIRCLE = "CIRCLE"

    # JSON Types
    JSON = "JSON"
    JSONB = "JSONB"

    # Arrays
    ARRAY_SUFFIX = "[]"

    # Full Text Search
    TSVECTOR = "TSVECTOR"
    TSQUERY = "TSQUERY"

    # Range Types
    INT4RANGE = "INT4RANGE"
    INT8RANGE = "INT8RANGE"
    NUMRANGE = "NUMRANGE"
    TSRANGE = "TSRANGE"
    TSTZRANGE = "TSTZRANGE"
    DATERANGE = "DATERANGE"


class PostgresColumnType:
    """postgres column type definition"""

    def __init__(self, sql_type: str, **constraints):
        """Initialize column type

        Args:
            sql_type: SQL type definition
            **constraints: Constraint conditions
        """
        self.sql_type = sql_type
        self.constraints = constraints
        self._is_array = False
        self._array_dimensions: Optional[int] = None

    def array(self, dimensions: Optional[int] = 1) -> 'PostgresColumnType':
        """Make this type an array type

        Args:
            dimensions: Number of array dimensions (default 1)

        Returns:
            PostgresColumnType: Self for chaining
        """
        self._is_array = True
        self._array_dimensions = dimensions
        return self

    def __str__(self):
        """Generate complete type definition statement"""
        sql = self.sql_type

        # Handle array type
        if self._is_array:
            sql += "[]" * (self._array_dimensions or 1)

        # Handle constraints
        constraint_parts = []

        # Primary key
        if self.constraints.get("primary_key"):
            constraint_parts.append("PRIMARY KEY")

        # Identity (auto-increment)
        if self.constraints.get("identity"):
            # Use GENERATED ALWAYS AS IDENTITY
            constraint_parts.append("GENERATED ALWAYS AS IDENTITY")

        # Unique constraint
        if self.constraints.get("unique"):
            constraint_parts.append("UNIQUE")

        # Not null constraint
        if self.constraints.get("not_null"):
            constraint_parts.append("NOT NULL")

        # Default value
        if "default" in self.constraints:
            default_value = self.constraints["default"]
            if isinstance(default_value, str):
                # Quote string defaults
                default_value = f"'{default_value}'"
            constraint_parts.append(f"DEFAULT {default_value}")

        # Collation
        if "collate" in self.constraints:
            constraint_parts.append(f"COLLATE \"{self.constraints['collate']}\"")

        # Check constraint
        if "check" in self.constraints:
            constraint_parts.append(f"CHECK ({self.constraints['check']})")

        # Exclusion constraint
        if "exclude" in self.constraints:
            constraint_parts.append(f"EXCLUDE {self.constraints['exclude']}")

        if constraint_parts:
            sql = f"{sql} {' '.join(constraint_parts)}"

        return sql

    @classmethod
    def get_type(cls, db_type: DatabaseType, **params) -> 'PostgresColumnType':
        """Create postgres column type from generic type

        Args:
            db_type: Generic database type definition
            **params: Type parameters and constraints

        Returns:
            PostgresColumnType: postgres column type instance

        Raises:
            ValueError: If type is not supported
        """
        mapping = POSTGRES_TYPE_MAPPINGS.get(db_type)
        if not mapping:
            raise ValueError(f"Unsupported type: {db_type}")

        sql_type = mapping.db_type
        if mapping.format_func:
            sql_type = mapping.format_func(sql_type, params)

        # Extract array parameters if present
        is_array = params.pop("is_array", False)
        array_dimensions = params.pop("array_dimensions", None)

        # Extract standard constraints
        constraints = {k: v for k, v in params.items()
                       if k in ['primary_key', 'identity', 'unique',
                                'not_null', 'default', 'collate', 'check',
                                'exclude']}

        column_type = cls(sql_type, **constraints)

        # Apply array type if requested
        if is_array:
            column_type.array(array_dimensions)

        return column_type