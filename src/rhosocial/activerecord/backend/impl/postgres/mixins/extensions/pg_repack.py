# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_repack.py
"""
PostgreSQL pg_repack online rebuild functionality mixin.

This module provides functionality to check pg_repack extension features.

For SQL expression generation, use the function factories in
``functions/pg_repack.py`` instead of the removed format_* methods.
"""


from typing import Optional

from rhosocial.activerecord.backend.expression.objects import Function

#: Schema pg_repack installs its functions into when created with defaults.
DEFAULT_REPACK_SCHEMA = "repack"


class PostgresPgRepackMixin:
    """pg_repack online rebuild functionality implementation."""

    def supports_pg_repack(self) -> bool:
        """Check if pg_repack extension is available."""
        return self.is_extension_installed("pg_repack")

    def pg_repack_function_name(
        self, name: str, schema: Optional[str] = None
    ) -> str:
        """Render one pg_repack routine's name as a callable reference.

        pg_repack installs into its own schema, so the factories in
        ``functions/pg_repack.py`` name it explicitly rather than relying on
        ``search_path``. The qualification is produced here from a
        :class:`Function` schema object; the factory passes the routine's own
        name.

        Both slots are rendered unquoted, for the same reason as
        :meth:`PostgresOrafceMixin.orafce_function_name`: the function-name
        path case-normalises this text, and the extension installs lower-case
        names.

        Args:
            name: The routine's own name, e.g. ``repack_version``.
            schema: The schema pg_repack was installed into, or ``None`` for
                the default one.

        Returns:
            The qualified, unquoted routine name for a function call.
        """
        sql, _ = self.format_function_object(
            Function(
                self,
                name,
                schema_name=schema if schema is not None else DEFAULT_REPACK_SCHEMA,
                schema_need_quote=False,
                name_need_quote=False,
            )
        )
        return sql
