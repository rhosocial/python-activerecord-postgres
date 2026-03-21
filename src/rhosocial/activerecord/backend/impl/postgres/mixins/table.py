# Copyright (c) 2024-present rhosocial Engineering Team.
# Licensed under the MIT License. See LICENSE for details.
# PostgreSQL table extended features implementation.


class PostgresTableMixin:
    """PostgreSQL table extended features implementation."""

    def supports_table_inheritance(self) -> bool:
        """PostgreSQL supports table inheritance."""
        return True
