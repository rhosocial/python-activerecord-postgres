# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/repack.py
"""PostgreSQL 19 native REPACK formatting and validation."""

from typing import List, Optional, Sequence, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from ...expression.ddl.repack import PostgresRepackExpression


class PostgresRepackMixin:
    """PostgreSQL native REPACK support, separate from the pg_repack extension."""

    def supports_repack(self) -> bool:
        return self.version >= (19, 0, 0)

    def supports_repack_all_tables(self) -> bool:
        return self.version >= (19, 0, 0)

    def supports_repack_using_index(self) -> bool:
        return self.version >= (19, 0, 0)

    def supports_repack_concurrently(self) -> bool:
        return self.version >= (19, 0, 0)

    def supports_repack_analyze(self) -> bool:
        return self.version >= (19, 0, 0)

    @staticmethod
    def _repack_identifier(value: str, name: str) -> str:
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a string")
        if not value or "\x00" in value:
            raise ValueError(f"{name} must be a non-empty identifier without NUL")
        return value

    def _format_repack_identifier(self, value: str, name: str) -> str:
        return self.format_identifier(self._repack_identifier(value, name))

    @staticmethod
    def _repack_columns(value: Optional[Sequence[str]]) -> Optional[List[str]]:
        if value is None:
            return None
        if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
            raise TypeError("REPACK columns must be a sequence of identifiers")
        columns = list(value)
        if not columns:
            raise ValueError("REPACK columns must not be empty")
        if any(not isinstance(column, str) or not column or "\x00" in column for column in columns):
            raise ValueError("REPACK columns contain an invalid identifier")
        if len(columns) != len(set(columns)):
            raise ValueError("REPACK columns must not contain duplicates")
        return columns

    def format_repack_statement(
        self,
        expr: "PostgresRepackExpression",
    ) -> Tuple[str, tuple]:
        """Format a native PostgreSQL 19 REPACK statement."""
        if not self.supports_repack():
            raise UnsupportedFeatureError(
                self.name,
                "REPACK",
                suggestion="requires PostgreSQL 19+",
            )
        for name, value in (
            ("verbose", expr.verbose),
            ("analyze", expr.analyze),
            ("concurrently", expr.concurrently),
        ):
            if not isinstance(value, bool):
                raise TypeError(f"{name} must be a bool")
        columns = self._repack_columns(expr.columns)
        if columns is not None and not expr.analyze:
            raise ValueError("REPACK columns require ANALYZE")
        if expr.analyze and expr.table_name is None:
            raise ValueError("REPACK ANALYZE requires one table")
        if expr.schema is not None and expr.table_name is None:
            raise ValueError("REPACK schema requires one table")
        if not isinstance(expr.all_using_index, bool):
            raise TypeError("all_using_index must be a bool")
        using_index = expr.using_index
        if using_index is not None and not isinstance(using_index, (bool, str)):
            raise TypeError("using_index must be a bool, string, or None")
        if isinstance(using_index, str):
            if not using_index:
                raise ValueError("using_index must not be empty")
            if "\x00" in using_index:
                raise ValueError("using_index contains NUL")
            if expr.table_name is None:
                raise ValueError("a named REPACK index requires one table")
        if expr.all_using_index and expr.table_name is not None:
            raise ValueError("all_using_index cannot be combined with a table")
        if expr.all_using_index and using_index not in {None, False, True}:
            raise ValueError("all_using_index conflicts with using_index")
        if expr.table_name is None and expr.concurrently:
            raise ValueError("REPACK CONCURRENTLY requires one table")
        parts = ["REPACK"]
        options = []
        if expr.verbose:
            options.append("VERBOSE")
        if expr.analyze:
            options.append("ANALYZE")
        if expr.concurrently:
            options.append("CONCURRENTLY")
        if options:
            parts.append("({})".format(", ".join(options)))
        use_index = expr.all_using_index or using_index is True
        if expr.table_name is not None:
            table = self._format_repack_identifier(expr.table_name, "table_name")
            if expr.schema is not None:
                table = f"{self._format_repack_identifier(expr.schema, 'schema')}.{table}"
            parts.append(table)
            if columns is not None:
                parts.append(
                    "({})".format(", ".join(self._format_repack_identifier(column, "column") for column in columns))
                )
            if use_index or isinstance(using_index, str):
                parts.append("USING INDEX")
        elif use_index:
            parts.append("USING INDEX")
        if isinstance(using_index, str):
            parts.append(self._format_repack_identifier(using_index, "using_index"))
        return " ".join(parts), ()
