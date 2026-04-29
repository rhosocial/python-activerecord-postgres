# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pgaudit.py
"""
PostgreSQL pgaudit audit logging functionality mixin.

This module provides functionality to check pgaudit extension features
and generate audit configuration statements.

Note: The format_* methods in this mixin generate configuration statements
(ALTER ROLE, SET) for audit setup, not data query function expressions.
They remain here as they are DDL/configuration operations rather than
query-level function calls.
"""


class PostgresPgauditMixin:
    """pgaudit audit logging functionality implementation.

    The format_* methods generate configuration statements (ALTER ROLE, SET)
    for audit setup, not data query function expressions.
    """

    def supports_pgaudit(self) -> bool:
        """Check if pgaudit extension is available."""
        return self.is_extension_installed("pgaudit")

    def supports_pgaudit_session(self) -> bool:
        """Check if pgaudit supports session auditing."""
        return self.check_extension_feature("pgaudit", "session")

    def format_pgaudit_set_role(self, role: str) -> str:
        """Format audit role assignment.

        Args:
            role: Audit role name

        Returns:
            SQL ALTER ROLE statement
        """
        return f"ALTER ROLE {role} SET pgaudit.role = '{role}'"

    def format_pgaudit_log_level(self, level: str = "log") -> str:
        """Format audit log level configuration.

        Args:
            level: Log level (log, notice, info)

        Returns:
            SQL SET statement
        """
        return f"SET pgaudit.log = '{level}'"

    def format_pgaudit_include_catalog(self, include: bool = True) -> str:
        """Format include catalog statements.

        Args:
            include: Include catalog statements

        Returns:
            SQL SET statement
        """
        inc = "all" if include else "none"
        return f"SET pgaudit.log_catalog = '{inc}'"