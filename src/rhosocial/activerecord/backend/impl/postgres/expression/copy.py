# src/rhosocial/activerecord/backend/impl/postgres/expression/copy.py
"""Typed PostgreSQL COPY expressions."""

from enum import Enum
from typing import List, Optional, Sequence, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "PostgresCopyFormat",
    "PostgresCopyOnError",
    "PostgresCopyLogVerbosity",
    "PostgresCopyToExpression",
    "PostgresCopyFromExpression",
]


class PostgresCopyFormat(str, Enum):
    TEXT = "text"
    CSV = "csv"
    JSON = "json"
    BINARY = "binary"


class PostgresCopyOnError(str, Enum):
    STOP = "stop"
    IGNORE = "ignore"
    SET_NULL = "set_null"


class PostgresCopyLogVerbosity(str, Enum):
    DEFAULT = "default"
    VERBOSE = "verbose"
    SILENT = "silent"


CopyHeader = Union[bool, int, str]
CopyColumnSelection = Union[str, Sequence[str]]


def _enum_value(value: Union[str, Enum]) -> str:
    return str(value.value if isinstance(value, Enum) else value)


def _copy_columns(value: Optional[Sequence[str]], name: str) -> Optional[List[str]]:
    if value is None:
        return None
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"{name} must be a sequence of identifiers")
    columns = list(value)
    if any(not isinstance(column, str) for column in columns):
        raise TypeError(f"{name} must contain only string identifiers")
    return columns


def _copy_force_columns(
    value: Optional[CopyColumnSelection],
) -> Optional[Union[str, List[str]]]:
    if value is None:
        return None
    if isinstance(value, str):
        return "*" if value == "*" else [value]
    if not isinstance(value, Sequence):
        raise TypeError("FORCE column selection must be '*' or a sequence of identifiers")
    columns = list(value)
    if not columns:
        raise ValueError("FORCE column selection must not be empty")
    if any(not isinstance(column, str) for column in columns):
        raise TypeError("FORCE column selection must contain only string identifiers")
    return columns


class PostgresCopyToExpression(BaseExpression):
    """A PostgreSQL 19-safe ``COPY ... TO STDOUT`` expression."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table_name: Optional[str] = None,
        query: Optional[BaseExpression] = None,
        *,
        schema: Optional[str] = None,
        columns: Optional[Sequence[str]] = None,
        format: Union[PostgresCopyFormat, str] = PostgresCopyFormat.TEXT,
        header: Optional[CopyHeader] = None,
        null: Optional[str] = None,
        delimiter: Optional[str] = None,
        quote: Optional[str] = None,
        escape: Optional[str] = None,
        force_quote: Optional[CopyColumnSelection] = None,
        force_array: Optional[bool] = None,
        partitioned: bool = False,
        is_partitioned: Optional[bool] = None,
    ):
        super().__init__(dialect)
        if table_name is None and query is None:
            raise ValueError("COPY TO requires exactly one table_name or query source")
        if table_name is not None and query is not None:
            raise ValueError("COPY TO accepts either table_name or query, not both")
        if query is not None and not isinstance(query, BaseExpression):
            raise TypeError("COPY TO query must be a BaseExpression")
        if is_partitioned is not None:
            if not isinstance(is_partitioned, bool):
                raise TypeError("is_partitioned must be a bool")
            if partitioned and not is_partitioned:
                raise ValueError("partitioned and is_partitioned disagree")
            partitioned = is_partitioned
        self.table_name = table_name
        self.query = query
        self.schema = schema
        self.columns = _copy_columns(columns, "columns")
        self.format = _enum_value(format)
        self.header = header
        self.null = null
        self.delimiter = delimiter
        self.quote = quote
        self.escape = escape
        self.force_quote = _copy_force_columns(force_quote)
        self.force_array = force_array
        self.partitioned = partitioned
        self.is_partitioned = partitioned

    @property
    def format_method(self) -> str:
        return "format_copy_to_statement"


class PostgresCopyFromExpression(BaseExpression):
    """A PostgreSQL ``COPY ... FROM STDIN`` expression."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table_name: str,
        *,
        schema: Optional[str] = None,
        columns: Optional[Sequence[str]] = None,
        format: Union[PostgresCopyFormat, str] = PostgresCopyFormat.TEXT,
        header: Optional[CopyHeader] = None,
        null: Optional[str] = None,
        default: Optional[str] = None,
        delimiter: Optional[str] = None,
        quote: Optional[str] = None,
        escape: Optional[str] = None,
        force_not_null: Optional[CopyColumnSelection] = None,
        force_null: Optional[CopyColumnSelection] = None,
        freeze: bool = False,
        where: Optional[BaseExpression] = None,
        on_error: Optional[Union[PostgresCopyOnError, str]] = None,
        reject_limit: Optional[int] = None,
        encoding: Optional[str] = None,
        log_verbosity: Optional[Union[PostgresCopyLogVerbosity, str]] = None,
    ):
        super().__init__(dialect)
        if where is not None and not isinstance(where, BaseExpression):
            raise TypeError("COPY FROM WHERE must be a BaseExpression")
        self.table_name = table_name
        self.schema = schema
        self.columns = _copy_columns(columns, "columns")
        self.format = _enum_value(format)
        self.header = header
        self.null = null
        self.default = default
        self.delimiter = delimiter
        self.quote = quote
        self.escape = escape
        self.force_not_null = _copy_force_columns(force_not_null)
        self.force_null = _copy_force_columns(force_null)
        self.freeze = freeze
        self.where = where
        self.on_error = None if on_error is None else _enum_value(on_error)
        self.reject_limit = reject_limit
        self.encoding = encoding
        self.log_verbosity = None if log_verbosity is None else _enum_value(log_verbosity)

    @property
    def format_method(self) -> str:
        return "format_copy_from_statement"
