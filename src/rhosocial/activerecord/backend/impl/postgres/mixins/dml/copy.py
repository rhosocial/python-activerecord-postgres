# src/rhosocial/activerecord/backend/impl/postgres/mixins/dml/copy.py
"""PostgreSQL COPY statement formatting and version gates."""

import re
from typing import Any, List, Optional, Sequence, Tuple, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from ...expression.copy import PostgresCopyFromExpression, PostgresCopyToExpression


class PostgresCopyMixin:
    """PostgreSQL COPY support restricted to client-side streams."""

    def supports_copy_to(self) -> bool:
        return self.version >= (9, 6, 0)

    def supports_copy_from(self) -> bool:
        return self.version >= (9, 6, 0)

    def supports_copy_json(self) -> bool:
        return self.version >= (19, 0, 0)

    def supports_copy_from_json(self) -> bool:
        return False

    def supports_copy_force_array(self) -> bool:
        return self.version >= (19, 0, 0)

    def supports_copy_force_all(self) -> bool:
        return self.version >= (17, 0, 0)

    def supports_copy_partitioned_table_to(self) -> bool:
        return self.version >= (19, 0, 0)

    def supports_copy_text_header(self) -> bool:
        return self.version >= (15, 0, 0)

    def supports_copy_header_match(self) -> bool:
        return self.version >= (15, 0, 0)

    def supports_copy_header_line_count(self) -> bool:
        return self.version >= (19, 0, 0)

    def supports_copy_on_error(self) -> bool:
        return self.version >= (17, 0, 0)

    def supports_copy_reject_limit(self) -> bool:
        return self.version >= (18, 0, 0)

    def supports_copy_set_null(self) -> bool:
        return self.version >= (19, 0, 0)

    def supports_copy_log_verbosity(self) -> bool:
        return self.version >= (17, 0, 0)

    def supports_copy_silent_log(self) -> bool:
        return self.version >= (18, 0, 0)

    def _require_copy_version(
        self,
        supported: bool,
        feature: str,
        minimum: Tuple[int, int, int],
    ) -> None:
        if not supported:
            major, minor, _ = minimum
            raise UnsupportedFeatureError(
                self.name,
                feature,
                suggestion=f"requires PostgreSQL {major}.{minor}+",
            )

    @staticmethod
    def _scan_copy_sql(sql: str) -> Tuple[List[str], bool]:
        words: List[str] = []
        has_separator = False
        index = 0
        length = len(sql)
        while index < length:
            if sql.startswith("--", index):
                newline = sql.find("\n", index + 2)
                index = length if newline < 0 else newline + 1
                continue
            if sql.startswith("/*", index):
                end = sql.find("*/", index + 2)
                if end < 0:
                    raise ValueError("COPY SQL contains an unterminated block comment")
                index = end + 2
                continue
            if sql[index] == "'":
                index += 1
                while index < length:
                    if sql[index] == "\\" and index + 1 < length:
                        index += 2
                        continue
                    if sql[index] != "'":
                        index += 1
                        continue
                    if index + 1 < length and sql[index + 1] == "'":
                        index += 2
                        continue
                    index += 1
                    break
                else:
                    raise ValueError("COPY SQL contains an unterminated string literal")
                continue
            if sql[index] == '"':
                index += 1
                while index < length:
                    if sql[index] == '"':
                        if index + 1 < length and sql[index + 1] == '"':
                            index += 2
                            continue
                        index += 1
                        break
                    index += 1
                else:
                    raise ValueError("COPY SQL contains an unterminated quoted identifier")
                continue
            if sql[index] == "$":
                match = re.match(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$", sql[index:])
                if match:
                    tag = match.group(0)
                    end = sql.find(tag, index + len(tag))
                    if end < 0:
                        raise ValueError("COPY SQL contains an unterminated dollar string")
                    index = end + len(tag)
                    continue
            if sql[index] == ";":
                has_separator = True
                index += 1
                continue
            if sql[index].isalpha() or sql[index] == "_":
                end = index + 1
                while end < length and (sql[end].isalnum() or sql[end] in "_$"):
                    end += 1
                words.append(sql[index:end].upper())
                index = end
                continue
            index += 1
        return words, has_separator

    def _validate_copy_query(self, sql: str) -> None:
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("COPY TO query rendered an empty SQL value")
        if "\x00" in sql:
            raise ValueError("COPY TO query contains a NUL character")
        words, has_separator = self._scan_copy_sql(sql)
        if has_separator:
            raise ValueError("COPY TO query must contain exactly one read-only statement")
        if not words or words[0] not in {"SELECT", "VALUES", "WITH", "TABLE"}:
            raise ValueError("COPY TO query must start with SELECT, VALUES, WITH, or TABLE")
        blocked = {
            "ALTER",
            "ANALYZE",
            "COPY",
            "CREATE",
            "DELETE",
            "DROP",
            "GRANT",
            "INSERT",
            "INTO",
            "MERGE",
            "REPACK",
            "REVOKE",
            "TRUNCATE",
            "UPDATE",
            "VACUUM",
        }
        if blocked.intersection(words[1:]):
            raise ValueError("COPY TO query must be read-only")

    def _validate_copy_predicate(self, sql: str) -> None:
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("COPY FROM WHERE rendered an empty SQL value")
        if "\x00" in sql:
            raise ValueError("COPY FROM WHERE contains a NUL character")
        _, has_separator = self._scan_copy_sql(sql)
        if has_separator:
            raise ValueError("COPY FROM WHERE must contain one SQL expression")

    def _validate_copy_child_dialect(self, expression: BaseExpression, name: str) -> None:
        try:
            child_dialect = expression.dialect
        except ValueError as error:
            raise TypeError(f"{name} must have a bound dialect") from error
        if child_dialect is not self:
            raise ValueError(f"{name} must use the PostgreSQL dialect")

    @staticmethod
    def _copy_identifier(value: str, name: str) -> str:
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a string")
        if not value or "\x00" in value:
            raise ValueError(f"{name} must be a non-empty identifier without NUL")
        return value

    def _format_copy_identifier(self, value: str, name: str) -> str:
        return self.format_identifier(self._copy_identifier(value, name))

    @staticmethod
    def _copy_column_list(values: Optional[Sequence[str]], name: str) -> Optional[List[str]]:
        if values is None:
            return None
        if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
            raise TypeError(f"{name} must be a sequence of identifiers")
        columns = list(values)
        if not columns:
            raise ValueError(f"{name} must not be empty")
        for column in columns:
            if not isinstance(column, str):
                raise TypeError(f"{name} must contain only string identifiers")
            if not column or "\x00" in column:
                raise ValueError(f"{name} contains an invalid identifier")
        if len(columns) != len(set(columns)):
            raise ValueError(f"{name} must not contain duplicates")
        return columns

    @staticmethod
    def _copy_string(value: Any, name: str, allow_empty: bool = True) -> str:
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a string")
        if "\x00" in value:
            raise ValueError(f"{name} must not contain NUL")
        if not allow_empty and not value:
            raise ValueError(f"{name} must not be empty")
        return value

    @staticmethod
    def _copy_character(value: Any, name: str) -> str:
        character = PostgresCopyMixin._copy_string(value, name, allow_empty=False)
        if len(character) != 1 or ord(character) > 127:
            raise ValueError(f"{name} must be one single-byte character")
        return character

    def _format_copy_string(self, value: Any, name: str) -> str:
        string = self._copy_string(value, name)
        escaped = string.replace("\\", "\\\\").replace("'", "''")
        prefix = "E" if "\\" in string else ""
        return f"{prefix}'{escaped}'"

    def _normalize_copy_format(self, value: Any, direction: str) -> str:
        copy_format = str(getattr(value, "value", value)).lower()
        if copy_format not in {"text", "csv", "json", "binary"}:
            raise ValueError("COPY format must be text, csv, json, or binary")
        if direction == "from" and copy_format == "json":
            raise ValueError("PostgreSQL COPY FROM does not support JSON format")
        return copy_format

    def _normalize_copy_header(
        self,
        value: Any,
        direction: str,
        copy_format: Optional[str] = None,
    ) -> Optional[Union[bool, int, str]]:
        if value is None:
            return None
        if isinstance(value, bool):
            normalized: Union[bool, int, str] = value
        elif isinstance(value, int):
            if direction == "to" and value not in {0, 1}:
                raise ValueError("COPY TO HEADER integer must be 0 or 1")
            if value < 0:
                raise ValueError("COPY HEADER integer must be non-negative")
            normalized = value
        elif isinstance(value, str):
            if direction != "from" or value.strip().upper() != "MATCH":
                raise ValueError("COPY TO does not support HEADER MATCH")
            normalized = "MATCH"
        else:
            raise TypeError("COPY HEADER must be a bool, non-negative integer, or MATCH")
        if copy_format == "text" and normalized not in (False, 0):
            self._require_copy_version(
                self.supports_copy_text_header(),
                f"COPY {direction.upper()} text HEADER",
                (15, 0, 0),
            )
        if isinstance(normalized, int) and normalized > 1:
            self._require_copy_version(
                self.supports_copy_header_line_count(),
                f"COPY {direction.upper()} HEADER line count",
                (19, 0, 0),
            )
        if normalized == "MATCH":
            self._require_copy_version(
                self.supports_copy_header_match(),
                "COPY FROM HEADER MATCH",
                (15, 0, 0),
            )
        return normalized

    @staticmethod
    def _format_copy_header(value: object) -> str:
        if value is True:
            return "HEADER"
        if value is False:
            return "HEADER FALSE"
        if isinstance(value, int):
            return f"HEADER {value}"
        return "HEADER MATCH"

    def _normalize_force_columns(
        self,
        value: Any,
        name: str,
        target_columns: Optional[Sequence[str]],
    ) -> Optional[Union[str, List[str]]]:
        if value is None:
            return None
        if isinstance(value, str) and value == "*":
            return "*"
        if isinstance(value, str):
            normalized_columns = [value]
        else:
            copied_columns = self._copy_column_list(value, name)
            if copied_columns is None:
                return None
            normalized_columns = copied_columns
        if target_columns is not None:
            missing = set(normalized_columns) - set(target_columns)
            if missing:
                raise ValueError(f"{name} names columns absent from the COPY column list")
        return normalized_columns

    def _format_copy_to_source(
        self,
        expr: "PostgresCopyToExpression",
    ) -> Tuple[str, Tuple[Any, ...]]:
        if (expr.table_name is None) == (expr.query is None):
            raise ValueError("COPY TO requires exactly one table_name or query source")
        if expr.query is not None:
            if expr.schema is not None or expr.columns is not None or expr.partitioned:
                raise ValueError("schema, columns, and partitioned are valid only for table COPY TO")
            if not isinstance(expr.query, BaseExpression):
                raise TypeError("COPY TO query must be a BaseExpression")
            self._validate_copy_child_dialect(expr.query, "COPY TO query")
            query_sql, params = expr.query.to_sql()
            self._validate_copy_query(query_sql)
            return f"({query_sql})", tuple(params)
        if not isinstance(expr.table_name, str):
            raise TypeError("COPY TO table_name must be a string")
        table = self._format_copy_identifier(expr.table_name, "table_name")
        if expr.schema is not None:
            table = f"{self._format_copy_identifier(expr.schema, 'schema')}.{table}"
        columns = self._copy_column_list(expr.columns, "columns")
        if columns is not None:
            table = "{} ({})".format(
                table,
                ", ".join(self._format_copy_identifier(column, "column") for column in columns),
            )
        return table, ()

    def format_copy_to_statement(
        self,
        expr: "PostgresCopyToExpression",
    ) -> Tuple[str, Tuple[Any, ...]]:
        """Format a client-stream-only ``COPY ... TO STDOUT`` statement."""
        self._require_copy_version(self.supports_copy_to(), "COPY TO", (9, 6, 0))
        copy_format = self._normalize_copy_format(expr.format, "to")
        if copy_format == "json":
            self._require_copy_version(
                self.supports_copy_json(),
                "COPY TO JSON format",
                (19, 0, 0),
            )
        if not isinstance(expr.partitioned, bool):
            raise TypeError("partitioned must be a bool")
        if expr.partitioned:
            self._require_copy_version(
                self.supports_copy_partitioned_table_to(),
                "COPY TO partitioned table",
                (19, 0, 0),
            )
        source, params = self._format_copy_to_source(expr)
        columns = self._copy_column_list(expr.columns, "columns")
        options = [f"FORMAT {copy_format.upper()}"]
        header = self._normalize_copy_header(expr.header, "to", copy_format)
        if header is not None:
            if copy_format in {"binary", "json"}:
                raise ValueError(f"HEADER is not allowed with COPY TO {copy_format.upper()}")
            options.append(self._format_copy_header(header))
        if expr.null is not None:
            if copy_format in {"binary", "json"}:
                raise ValueError(f"NULL is not allowed with COPY TO {copy_format.upper()}")
            options.append(f"NULL {self._format_copy_string(expr.null, 'null')}")
        if expr.delimiter is not None:
            if copy_format not in {"text", "csv"}:
                raise ValueError(f"DELIMITER is not allowed with COPY TO {copy_format.upper()}")
            delimiter = self._copy_character(expr.delimiter, "delimiter")
            options.append(f"DELIMITER {self._format_copy_string(delimiter, 'delimiter')}")
        if expr.quote is not None or expr.escape is not None:
            if copy_format != "csv":
                raise ValueError("QUOTE and ESCAPE require COPY TO CSV format")
        if expr.quote is not None:
            quote = self._copy_character(expr.quote, "quote")
            options.append(f"QUOTE {self._format_copy_string(quote, 'quote')}")
        if expr.escape is not None:
            escape = self._copy_character(expr.escape, "escape")
            options.append(f"ESCAPE {self._format_copy_string(escape, 'escape')}")
        if expr.force_quote is not None:
            if copy_format != "csv":
                raise ValueError("FORCE_QUOTE requires COPY TO CSV format")
            force_quote = self._normalize_force_columns(
                expr.force_quote,
                "force_quote",
                columns,
            )
            if expr.query is not None and force_quote != "*":
                raise ValueError("query COPY TO FORCE_QUOTE requires '*'")
            if force_quote == "*":
                options.append("FORCE_QUOTE *")
            elif force_quote:
                options.append(
                    "FORCE_QUOTE ({})".format(
                        ", ".join(
                            self._format_copy_identifier(column, "force_quote column")
                            for column in force_quote
                        )
                    )
                )
        if expr.force_array is not None:
            if not isinstance(expr.force_array, bool):
                raise TypeError("force_array must be a bool")
            if copy_format != "json":
                raise ValueError("FORCE_ARRAY requires COPY TO JSON format")
            self._require_copy_version(
                self.supports_copy_force_array(),
                "COPY TO FORCE_ARRAY",
                (19, 0, 0),
            )
            value = "" if expr.force_array else " FALSE"
            options.append(f"FORCE_ARRAY{value}")
        return f"COPY {source} TO STDOUT ({', '.join(options)})", params

    def _format_copy_from_where(
        self,
        expression: BaseExpression,
    ) -> Tuple[str, Tuple[Any, ...]]:
        self._validate_copy_child_dialect(expression, "COPY FROM WHERE")
        where_sql, params = expression.to_sql()
        self._validate_copy_predicate(where_sql)
        return where_sql, tuple(params)

    def format_copy_from_statement(
        self,
        expr: "PostgresCopyFromExpression",
    ) -> Tuple[str, Tuple[Any, ...]]:
        """Format a client-stream-only ``COPY ... FROM STDIN`` statement."""
        self._require_copy_version(self.supports_copy_from(), "COPY FROM", (9, 6, 0))
        copy_format = self._normalize_copy_format(expr.format, "from")
        if not isinstance(expr.table_name, str):
            raise TypeError("COPY FROM table_name must be a string")
        table = self._format_copy_identifier(expr.table_name, "table_name")
        if expr.schema is not None:
            table = f"{self._format_copy_identifier(expr.schema, 'schema')}.{table}"
        columns = self._copy_column_list(expr.columns, "columns")
        if columns is not None:
            table = "{} ({})".format(
                table,
                ", ".join(self._format_copy_identifier(column, "column") for column in columns),
            )
        options = [f"FORMAT {copy_format.upper()}"]
        header = self._normalize_copy_header(expr.header, "from", copy_format)
        if header is not None:
            if copy_format == "binary":
                raise ValueError("HEADER is not allowed with COPY FROM BINARY")
            options.append(self._format_copy_header(header))
        if expr.null is not None:
            if copy_format == "binary":
                raise ValueError("NULL is not allowed with COPY FROM BINARY")
            options.append(f"NULL {self._format_copy_string(expr.null, 'null')}")
        if expr.default is not None:
            if copy_format == "binary":
                raise ValueError("DEFAULT is not allowed with COPY FROM BINARY")
            options.append(f"DEFAULT {self._format_copy_string(expr.default, 'default')}")
        if expr.delimiter is not None:
            if copy_format not in {"text", "csv"}:
                raise ValueError(f"DELIMITER is not allowed with COPY FROM {copy_format.upper()}")
            delimiter = self._copy_character(expr.delimiter, "delimiter")
            options.append(f"DELIMITER {self._format_copy_string(delimiter, 'delimiter')}")
        if expr.quote is not None or expr.escape is not None:
            if copy_format != "csv":
                raise ValueError("QUOTE and ESCAPE require COPY FROM CSV format")
        if expr.quote is not None:
            quote = self._copy_character(expr.quote, "quote")
            options.append(f"QUOTE {self._format_copy_string(quote, 'quote')}")
        if expr.escape is not None:
            escape = self._copy_character(expr.escape, "escape")
            options.append(f"ESCAPE {self._format_copy_string(escape, 'escape')}")
        force_not_null = self._normalize_force_columns(
            expr.force_not_null,
            "force_not_null",
            columns,
        )
        if force_not_null is not None:
            if copy_format != "csv":
                raise ValueError("FORCE_NOT_NULL requires COPY FROM CSV format")
            if force_not_null == "*":
                self._require_copy_version(
                    self.supports_copy_force_all(),
                    "COPY FROM FORCE_NOT_NULL *",
                    (17, 0, 0),
                )
            target = (
                "*"
                if force_not_null == "*"
                else "({})".format(
                    ", ".join(
                        self._format_copy_identifier(column, "force_not_null column") for column in force_not_null
                    )
                )
            )
            options.append(f"FORCE_NOT_NULL {target}")
        force_null = self._normalize_force_columns(
            expr.force_null,
            "force_null",
            columns,
        )
        if force_null is not None:
            if copy_format != "csv":
                raise ValueError("FORCE_NULL requires COPY FROM CSV format")
            if force_null == "*":
                self._require_copy_version(
                    self.supports_copy_force_all(),
                    "COPY FROM FORCE_NULL *",
                    (17, 0, 0),
                )
            target = (
                "*"
                if force_null == "*"
                else "({})".format(
                    ", ".join(self._format_copy_identifier(column, "force_null column") for column in force_null)
                )
            )
            options.append(f"FORCE_NULL {target}")
        if not isinstance(expr.freeze, bool):
            raise TypeError("freeze must be a bool")
        if expr.freeze:
            options.append("FREEZE")
        on_error = None
        if expr.on_error is not None:
            on_error = str(getattr(expr.on_error, "value", expr.on_error)).lower()
            if on_error not in {"stop", "ignore", "set_null"}:
                raise ValueError("ON_ERROR must be stop, ignore, or set_null")
            self._require_copy_version(
                self.supports_copy_on_error(),
                "COPY FROM ON_ERROR",
                (17, 0, 0),
            )
            if on_error in {"ignore", "set_null"} and copy_format not in {"text", "csv"}:
                raise ValueError("ON_ERROR ignore/set_null require text or csv format")
            if on_error == "set_null":
                self._require_copy_version(
                    self.supports_copy_set_null(),
                    "COPY FROM ON_ERROR SET_NULL",
                    (19, 0, 0),
                )
            options.append(f"ON_ERROR {on_error.upper()}")
        if expr.reject_limit is not None:
            if isinstance(expr.reject_limit, bool) or not isinstance(expr.reject_limit, int):
                raise TypeError("reject_limit must be an integer")
            if expr.reject_limit <= 0:
                raise ValueError("reject_limit must be positive")
            if on_error != "ignore":
                raise ValueError("REJECT_LIMIT requires ON_ERROR ignore")
            self._require_copy_version(
                self.supports_copy_reject_limit(),
                "COPY FROM REJECT_LIMIT",
                (18, 0, 0),
            )
            options.append(f"REJECT_LIMIT {expr.reject_limit}")
        if expr.encoding is not None:
            encoding = self._copy_string(expr.encoding, "encoding", allow_empty=False)
            if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", encoding) is None:
                raise ValueError("encoding must be a PostgreSQL encoding name")
            options.append(f"ENCODING {self._format_copy_string(encoding, 'encoding')}")
        if expr.log_verbosity is not None:
            verbosity = str(getattr(expr.log_verbosity, "value", expr.log_verbosity)).lower()
            if verbosity not in {"default", "verbose", "silent"}:
                raise ValueError("LOG_VERBOSITY must be default, verbose, or silent")
            self._require_copy_version(
                self.supports_copy_log_verbosity(),
                "COPY FROM LOG_VERBOSITY",
                (17, 0, 0),
            )
            if verbosity == "silent":
                self._require_copy_version(
                    self.supports_copy_silent_log(),
                    "COPY FROM LOG_VERBOSITY SILENT",
                    (18, 0, 0),
                )
            options.append(f"LOG_VERBOSITY {verbosity.upper()}")
        sql = f"COPY {table} FROM STDIN ({', '.join(options)})"
        params: Tuple[Any, ...] = ()
        if expr.where is not None:
            if not isinstance(expr.where, BaseExpression):
                raise TypeError("COPY FROM WHERE must be a BaseExpression")
            where_sql, params = self._format_copy_from_where(expr.where)
            sql = f"{sql} WHERE {where_sql}"
        return sql, params
