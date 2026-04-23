# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pgaudit.py
"""
pgaudit audit logging functionality implementation.

This module provides the PostgresPgauditMixin class that adds support for
pgaudit extension features.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class PostgresPgauditMixin:
    """pgaudit audit logging functionality implementation."""

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