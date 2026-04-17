# src/rhosocial/activerecord/backend/impl/postgres/mixins/dml/vacuum.py
"""PostgreSQL VACUUM enhancements implementation.

This module provides the PostgresVacuumMixin class for VACUUM and ANALYZE
statement generation with PostgreSQL-specific options.
"""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ...expression.ddl import VacuumExpression, AnalyzeExpression


class PostgresVacuumMixin:
    """PostgreSQL VACUUM enhancements implementation.

    All features are native, using version number for detection.
    """

    def supports_parallel_vacuum(self) -> bool:
        """Parallel VACUUM is native feature, PG 13+."""
        return self.version >= (13, 0, 0)

    def supports_index_cleanup_auto(self) -> bool:
        """INDEX_CLEANUP AUTO is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def supports_vacuum_process_toast(self) -> bool:
        """PROCESS_TOAST control is native feature, PG 14+."""
        return self.version >= (14, 0, 0)

    def format_vacuum_statement(self, expr: "VacuumExpression") -> Tuple[str, tuple]:
        """Format VACUUM statement from VacuumExpression."""
        parts = ["VACUUM"]

        # Add options
        options = []
        if expr.full:
            options.append("FULL")
        if expr.freeze:
            options.append("FREEZE")
        if expr.verbose:
            options.append("VERBOSE")
        if expr.analyze:
            options.append("ANALYZE")

        # PG 13+ parallel option
        if expr.parallel is not None:
            if not self.supports_parallel_vacuum():
                raise ValueError("Parallel VACUUM requires PostgreSQL 13+")
            options.append(f"PARALLEL {expr.parallel}")

        # PG 14+ index_cleanup option
        if expr.index_cleanup is not None:
            if not self.supports_index_cleanup_auto():
                raise ValueError("INDEX_CLEANUP requires PostgreSQL 14+")
            index_cleanup = expr.index_cleanup.upper()
            if index_cleanup not in ("AUTO", "ON", "OFF"):
                raise ValueError("index_cleanup must be 'AUTO', 'ON', or 'OFF'")
            options.append(f"INDEX_CLEANUP {index_cleanup}")

        # PG 14+ process_toast option
        if expr.process_toast is not None:
            if not self.supports_vacuum_process_toast():
                raise ValueError("PROCESS_TOAST requires PostgreSQL 14+")
            options.append(f"PROCESS_TOAST {str(expr.process_toast).upper()}")

        if expr.skip_locked:
            options.append("SKIP_LOCKED")

        # TRUNCATE is mutually exclusive with FULL
        if expr.truncate:
            if expr.full:
                raise ValueError("TRUNCATE cannot be used with FULL")
            options.append("TRUNCATE")

        if options:
            parts.append("(" + ", ".join(options) + ")")

        # Add table name if specified
        if expr.table_name:
            if expr.schema:
                parts.append(f"{self.format_identifier(expr.schema)}.{self.format_identifier(expr.table_name)}")
            else:
                parts.append(self.format_identifier(expr.table_name))

            # Add columns for ANALYZE
            if expr.columns:
                parts.append("(" + ", ".join(self.format_identifier(c) for c in expr.columns) + ")")

        return (" ".join(parts), ())

    def format_analyze_statement(self, expr: "AnalyzeExpression") -> Tuple[str, tuple]:
        """Format ANALYZE statement from AnalyzeExpression."""
        parts = ["ANALYZE"]

        if expr.verbose:
            parts.append("VERBOSE")

        if expr.skip_locked:
            parts.append("SKIP_LOCKED")

        # Add table name if specified
        if expr.table_name:
            if expr.schema:
                parts.append(f"{self.format_identifier(expr.schema)}.{self.format_identifier(expr.table_name)}")
            else:
                parts.append(self.format_identifier(expr.table_name))

            # Add columns
            if expr.columns:
                parts.append("(" + ", ".join(self.format_identifier(c) for c in expr.columns) + ")")

        return (" ".join(parts), ())
