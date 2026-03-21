# Copyright (c) 2024 rhosocial.
# Licensed under the MIT License.
#
# PostgreSQL Trigger DDL Mixin Implementation
#
# This module provides PostgreSQL-specific trigger DDL functionality,
# including CREATE TRIGGER and DROP TRIGGER statement formatting.
#
# PostgreSQL uses 'EXECUTE FUNCTION func_name()' syntax instead of
# the SQL:1999 standard 'EXECUTE func_name'.

from typing import Optional, Tuple, List, Dict, Any


class PostgresTriggerMixin:
    """PostgreSQL trigger DDL implementation.

    PostgreSQL uses 'EXECUTE FUNCTION func_name()' syntax instead of
    the SQL:1999 standard 'EXECUTE func_name'.
    """

    def supports_trigger_referencing(self) -> bool:
        """REFERENCING clause is supported since PostgreSQL 10."""
        return self.version >= (10, 0, 0)

    def supports_trigger_when(self) -> bool:
        """WHEN condition is supported in all modern versions."""
        return True

    def supports_statement_trigger(self) -> bool:
        """FOR EACH STATEMENT is supported."""
        return True

    def supports_instead_of_trigger(self) -> bool:
        """INSTEAD OF triggers are supported."""
        return True

    def supports_trigger_if_not_exists(self) -> bool:
        """IF NOT EXISTS is supported since PostgreSQL 9.5."""
        return self.version >= (9, 5, 0)

    def format_create_trigger_statement(self, expr) -> Tuple[str, tuple]:
        """Format CREATE TRIGGER statement (PostgreSQL syntax).

        PostgreSQL uses 'EXECUTE FUNCTION func_name()' instead of standard 'EXECUTE func_name'.
        """
        parts = ["CREATE TRIGGER"]

        if expr.if_not_exists and self.supports_trigger_if_not_exists():
            parts.append("IF NOT EXISTS")

        parts.append(self.format_identifier(expr.trigger_name))

        parts.append(expr.timing.value)

        if expr.update_columns:
            cols = ", ".join(self.format_identifier(c) for c in expr.update_columns)
            events_str = f"UPDATE OF {cols}"
        else:
            events_str = " OR ".join(e.value for e in expr.events)
        parts.append(events_str)

        parts.append("ON")
        parts.append(self.format_identifier(expr.table_name))

        if expr.referencing and self.supports_trigger_referencing():
            parts.append(expr.referencing)

        if expr.level:
            parts.append(expr.level.value)

        all_params = []
        if expr.condition and self.supports_trigger_when():
            cond_sql, cond_params = expr.condition.to_sql()
            parts.append(f"WHEN ({cond_sql})")
            all_params.extend(cond_params)

        parts.append("EXECUTE FUNCTION")
        parts.append(expr.function_name + "()")

        return " ".join(parts), tuple(all_params)

    def format_drop_trigger_statement(self, expr) -> Tuple[str, tuple]:
        """Format DROP TRIGGER statement (PostgreSQL syntax)."""
        parts = ["DROP TRIGGER"]

        if expr.if_exists:
            parts.append("IF EXISTS")

        parts.append(self.format_identifier(expr.trigger_name))

        if expr.table_name:
            parts.append("ON")
            parts.append(self.format_identifier(expr.table_name))

        return " ".join(parts), ()
