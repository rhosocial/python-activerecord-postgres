"""PostgreSQL pglogical logical replication functionality mixin.

This module provides functionality to check pglogical extension features.

For SQL expression generation, use the function factories in
``functions/pglogical.py`` instead of the removed format_* methods.
"""


class PostgresPgLogicalMixin:
    """pglogical logical replication functionality implementation."""

    def supports_pglogical(self) -> bool:
        """Check if pglogical extension is available."""
        return self.is_extension_installed("pglogical")

    def supports_pglogical_replication(self) -> bool:
        """Check if pglogical supports logical replication."""
        return self.check_extension_feature("pglogical", "replication")
