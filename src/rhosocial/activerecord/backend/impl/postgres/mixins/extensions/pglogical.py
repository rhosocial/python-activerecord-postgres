# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pglogical.py
"""
pglogical logical replication functionality implementation.

This module provides the PostgresPgLogicalMixin class that adds support for
pglogical extension features.
"""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    pass


class PostgresPgLogicalMixin:
    """pglogical logical replication functionality implementation."""

    def supports_pglogical(self) -> bool:
        """Check if pglogical extension is available."""
        return self.is_extension_installed("pglogical")

    def supports_pglogical_replication(self) -> bool:
        """Check if pglogical supports logical replication."""
        return self.check_extension_feature("pglogical", "replication")

    def format_pglogical_create_node(
        self, node_name: str, dsn: str, useserial: bool = False
    ) -> str:
        """Format pglogical node creation.

        Args:
            node_name: Name of the node
            dsn: Connection string
            useserial: Use serial for identity

        Returns:
            SQL SELECT statement
        """
        serial = "true" if useserial else "false"
        return (
            f"SELECT pglogical.create_node("
            f"node_name := '{node_name}', dsn := '{dsn}', "
            f"useserial := {serial})"
        )

    def format_pglogical_create_publication(
        self, pub_name: str, replicate_insert: bool = True
    ) -> str:
        """Format pglogical publication creation.

        Args:
            pub_name: Name of the publication
            replicate_insert: Replicate INSERT

        Returns:
            SQL SELECT statement
        """
        rep = "true" if replicate_insert else "false"
        return (
            f"SELECT pglogical.create_publication("
            f"pub_name := '{pub_name}', replicate_insert := {rep})"
        )

    def format_pglogical_create_subscription(
        self, sub_name: str, pub_dsn: str, replication_sets: Optional[list] = None
    ) -> str:
        """Format pglogical subscription creation.

        Args:
            sub_name: Name of the subscription
            pub_dsn: Publisher connection string
            replication_sets: Optional list of replication sets

        Returns:
            SQL SELECT statement
        """
        sets = replication_sets or ["default"]
        sets_str = ", ".join(f"'{s}'" for s in sets)
        return (
            f"SELECT pglogical.create_subscription("
            f"sub_name := '{sub_name}', provider_dsn := '{pub_dsn}', "
            f"replication_sets := ARRAY[{sets_str}])"
        )