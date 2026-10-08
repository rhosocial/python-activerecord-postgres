# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/orafce.py
"""
PostgreSQL orafce Oracle compatibility functionality mixin.

This module provides functionality to check orafce extension features.

For SQL expression generation, use the function factories in
``functions/orafce.py`` instead of the removed format_* methods.
"""

from typing import Optional

from rhosocial.activerecord.backend.expression.objects import Function

#: Schema orafce installs its functions into when created with defaults.
DEFAULT_ORAFCE_SCHEMA = "oracle"


class PostgresOrafceMixin:
    """orafce Oracle compatibility functionality implementation."""

    def supports_orafce(self) -> bool:
        """Check if orafce extension is available."""
        return self.is_extension_installed("orafce")

    def orafce_function_name(
        self, name: str, schema: Optional[str] = None
    ) -> str:
        """Render one orafce routine's name as a callable reference.

        orafce installs into its own schema, and the factories in
        ``functions/orafce.py`` must name it explicitly so the call resolves
        regardless of ``search_path``. That qualification is produced here, by
        the dialect, from a :class:`Function` schema object -- the factory
        passes the routine's own name and never assembles a dotted string.

        Both slots are rendered unquoted. A call target is not a table
        reference: the function-name path that consumes this text is
        case-normalised, and a quoted mixed-case identifier would not match the
        lower-case names the extension actually installs.

        Args:
            name: The routine's own name, e.g. ``ADD_MONTHS``.
            schema: The schema orafce was installed into, or ``None`` for the
                default one.

        Returns:
            The qualified, unquoted routine name for a function call.
        """
        sql, _ = self.format_function_object(
            Function(
                self,
                name,
                schema_name=schema if schema is not None else DEFAULT_ORAFCE_SCHEMA,
                schema_need_quote=False,
                name_need_quote=False,
            )
        )
        return sql