# Copyright (c) 2024-2025 rhosocial<rhosocial@163.com>
# SPDX-License-Identifier: MIT
# =============================================================================
# PostgreSQL Trigger Protocol
# =============================================================================

"""PostgreSQL trigger DDL protocol definition.

This module contains the PostgresTriggerSupport protocol which defines
the interface for PostgreSQL's native trigger DDL features.
"""

from typing import Protocol, runtime_checkable, Tuple


@runtime_checkable
class PostgresTriggerSupport(Protocol):
    """PostgreSQL trigger DDL protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL trigger features beyond SQL:1999 standard:
    - EXECUTE FUNCTION syntax (instead of EXECUTE proc_name)
    - REFERENCING clause for OLD/NEW row references
    - WHEN condition with arbitrary expressions
    - FOR EACH STATEMENT triggers
    - INSTEAD OF triggers for views
    - Multiple events with OR syntax
    - UPDATE OF column_list syntax
    - Constraint triggers with DEFERRABLE

    Official Documentation:
    - CREATE TRIGGER: https://www.postgresql.org/docs/current/sql-createtrigger.html
    - Trigger Functions: https://www.postgresql.org/docs/current/triggers.html

    Version Requirements:
    - Basic triggers: All versions
    - Constraint triggers: PostgreSQL 9.1+
    - Transition tables: PostgreSQL 10+
    """

    def supports_trigger_referencing(self) -> bool:
        """Whether REFERENCING clause is supported.

        Native feature, PostgreSQL 10+.
        Allows referencing OLD/NEW tables in triggers.
        """
        ...

    def supports_trigger_when(self) -> bool:
        """Whether WHEN condition is supported.

        Native feature, all versions.
        Allows conditional trigger execution.
        """
        ...

    def supports_statement_trigger(self) -> bool:
        """Whether FOR EACH STATEMENT triggers are supported.

        Native feature, all versions.
        Triggers execute once per statement, not per row.
        """
        ...

    def supports_instead_of_trigger(self) -> bool:
        """Whether INSTEAD OF triggers are supported.

        Native feature, all versions.
        Used for views to make them updatable.
        """
        ...

    def supports_trigger_if_not_exists(self) -> bool:
        """Whether CREATE TRIGGER IF NOT EXISTS is supported.

        Native feature, PostgreSQL 9.5+.
        """
        ...

    def format_create_trigger_statement(self, expr) -> Tuple[str, tuple]:
        """Format CREATE TRIGGER statement.

        PostgreSQL uses 'EXECUTE FUNCTION func_name()' syntax.
        """
        ...

    def format_drop_trigger_statement(self, expr) -> Tuple[str, tuple]:
        """Format DROP TRIGGER statement."""
        ...
