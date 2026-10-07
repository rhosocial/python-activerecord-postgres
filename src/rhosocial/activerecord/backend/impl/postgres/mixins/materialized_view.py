# src/rhosocial/activerecord/backend/impl/postgres/mixins/materialized_view.py
"""PostgreSQL materialized view feature support implementation.

PostgreSQL grammar (https://www.postgresql.org/docs/current/sql-creatematerializedview.html):

    CREATE MATERIALIZED VIEW [ IF NOT EXISTS ] name [ (aliases) ]
        [ WITH ( storage_parameter [= value] [, ...] ) ]
        [ TABLESPACE tablespace_name ] AS query [ WITH [ NO ] DATA ]

    REFRESH MATERIALIZED VIEW [ CONCURRENTLY ] name [ WITH [ NO ] DATA ]

    ALTER MATERIALIZED VIEW name RENAME TO new_name
    ALTER MATERIALIZED VIEW name SET SCHEMA new_schema
    ALTER MATERIALIZED VIEW name SET ( storage_parameter [= value] [, ...] )
    ALTER MATERIALIZED VIEW name RESET ( storage_parameter [, ...] )
    ALTER MATERIALIZED VIEW name OWNER TO { new_owner | CURRENT_ROLE | CURRENT_USER | SESSION_USER }

    DROP MATERIALIZED VIEW [ IF EXISTS ] name [ CASCADE ]

Note: ``ALTER MATERIALIZED VIEW`` does **not** support ``SET TABLESPACE`` — a
materialized view cannot be relocated to another tablespace after creation.

This mixin must precede the core ``ViewMixin`` in ``PostgresDialect`` so the
formatters below take precedence over the generic ones.
"""
from typing import Any, Dict, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.mixins import ViewMixin
from rhosocial.activerecord.backend.expression.objects import MaterializedView

from ..expression.ddl.mv import (
    _CURRENT_ROLE_KEYWORDS,
    PostgresChangeMaterializedViewOwnerAction,
    PostgresRenameMaterializedViewAction,
    PostgresResetMaterializedViewPropertiesAction,
    PostgresSetMaterializedViewPropertiesAction,
    PostgresSetMaterializedViewSchemaAction,
)
from ..storage_parameters import resolve_storage_parameter

if TYPE_CHECKING:
    from ....expression.statements.ddl_view import CreateMaterializedViewExpression
    from ..expression.ddl.mv import (
        PostgresAlterMaterializedViewExpression,
        PostgresRefreshMaterializedViewExpression,
    )


class PostgresMaterializedViewMixin(ViewMixin):
    """PostgreSQL materialized view extended features implementation.

    Extends the core :class:`ViewMixin` so the generic REFRESH/DROP paths are
    reachable through ``super()``; this class must stay *before* ``ViewMixin``
    in ``PostgresDialect``'s base list so its formatters win.
    """

    def supports_materialized_view(self) -> bool:
        """Materialized views require PostgreSQL 9.3+."""
        return self.version >= (9, 3, 0)

    def supports_refresh_materialized_view(self) -> bool:
        """REFRESH MATERIALIZED VIEW requires PostgreSQL 9.3+."""
        return self.version >= (9, 3, 0)

    def supports_materialized_view_concurrent_refresh(self) -> bool:
        """CONCURRENTLY is supported since PostgreSQL 9.4."""
        return self.version >= (9, 4, 0)

    def supports_materialized_view_if_not_exists(self) -> bool:
        """CREATE MATERIALIZED VIEW IF NOT EXISTS is supported since PostgreSQL 9.4."""
        return self.version >= (9, 4, 0)

    def supports_materialized_view_tablespace(self) -> bool:
        """TABLESPACE on CREATE MATERIALIZED VIEW is supported since PostgreSQL 9.3."""
        return self.version >= (9, 3, 0)

    def supports_materialized_view_storage_options(self) -> bool:
        """WITH (storage_parameter) is supported since PostgreSQL 9.3."""
        return self.version >= (9, 3, 0)

    def supports_alter_materialized_view(self) -> bool:
        """ALTER MATERIALIZED VIEW is supported since PostgreSQL 9.3."""
        return self.version >= (9, 3, 0)

    def supports_materialized_view_restrict(self) -> bool:
        """``DROP MATERIALIZED VIEW ... RESTRICT`` is in the synopsis.

        PostgreSQL accepts ``DROP MATERIALIZED VIEW name [ CASCADE | RESTRICT ]``
        (measured on PostgreSQL 16); RESTRICT is the default behavior but the
        token itself is legal.
        """
        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _format_materialized_view_name(self, expr: Any) -> str:
        """Render a materialized view name, schema-qualified when available.

        Args:
            expr: An MV expression carrying the ``MaterializedView`` object it
                names. The object renders itself, so the schema it carries is
                the one that appears.

        Returns:
            The quoted (and possibly schema-qualified) identifier.
        """
        return expr.view.to_sql()[0]

    def _format_storage_parameters(self, properties: Dict[Any, Any]) -> str:
        """Render a ``KEY = value, ...`` storage parameter list.

        Names come from :class:`PostgresStorageParameter` (already lower case) and
        are emitted bare: PostgreSQL folds unquoted identifiers to lower case, so
        quoting is unnecessary and values are inserted verbatim because the
        server accepts bare literals as well as quoted strings.
        """
        rendered = []
        for key, value in properties.items():
            parameter = resolve_storage_parameter(key)
            name = parameter.value if parameter is not None else str(key)
            rendered.append(f"{name} = {value}")
        return ", ".join(rendered)

    def _format_storage_parameter_names(self, parameters: Any) -> str:
        """Render a ``key, ...`` storage parameter name list (for ``RESET``)."""
        rendered = []
        for key in parameters:
            parameter = resolve_storage_parameter(key)
            rendered.append(parameter.value if parameter is not None else str(key))
        return ", ".join(rendered)

    # ------------------------------------------------------------------
    # Formatters
    # ------------------------------------------------------------------

    def format_create_materialized_view_statement(self, expr: "CreateMaterializedViewExpression") -> Tuple[str, tuple]:
        """Format CREATE MATERIALIZED VIEW statement for PostgreSQL.

        Follows the PostgreSQL grammar order:
        ``CREATE MATERIALIZED VIEW [IF NOT EXISTS] name [(aliases)]
        [WITH (storage_parameter = value, ...)] [TABLESPACE name] AS query
        [WITH [NO] DATA]``.

        - ``expr.view`` — the ``MaterializedView`` being created; it renders
          itself, so a qualified name comes out of the namespace machinery.
        - ``expr.column_aliases`` — optional list of column aliases.
        - ``expr.if_not_exists`` — ``IF NOT EXISTS`` (PostgreSQL 9.4+).
        - ``expr.storage_options`` — optional dict of storage parameters (``WITH (… )``).
        - ``expr.tablespace`` — optional tablespace.
        - ``expr.query`` — source SELECT expression.
        - ``expr.with_data`` / ``expr.no_data`` — ``WITH DATA`` / ``WITH NO
          DATA``; neither set renders no tail (both set is refused at
          construction).

        Args:
            expr: CreateMaterializedViewExpression instance

        Returns:
            Tuple of (SQL string, params tuple)

        Raises:
            TypeError: ``expr.view`` is not a :class:`MaterializedView`. A plain
                :class:`View` is the wrong kind here and would have had its own
                name rendered as the materialized view's.
            UnsupportedFeatureError: If the dialect or the target server version
                does not support the requested materialized view feature.
        """
        if not isinstance(expr.view, MaterializedView):
            raise TypeError(
                f"CreateMaterializedViewExpression.view must be a MaterializedView, "
                f"got {type(expr.view).__name__}"
            )
        if not self.supports_materialized_view():
            raise UnsupportedFeatureError(self.name, "CREATE MATERIALIZED VIEW")

        parts = ["CREATE MATERIALIZED VIEW"]

        if getattr(expr, "if_not_exists", False):
            if not self.supports_materialized_view_if_not_exists():
                raise UnsupportedFeatureError(
                    self.name,
                    "CREATE MATERIALIZED VIEW IF NOT EXISTS",
                    f"Requires PostgreSQL 9.4+, current version is {self.version}.",
                )
            parts.append("IF NOT EXISTS")

        parts.append(self._format_materialized_view_name(expr))

        if expr.column_aliases:
            cols = ", ".join(self.format_identifier(c) for c in expr.column_aliases)
            parts.append(f"({cols})")

        if expr.storage_options and self.supports_materialized_view_storage_options():
            parts.append(f"WITH ({self._format_storage_parameters(expr.storage_options)})")

        if expr.tablespace and self.supports_materialized_view_tablespace():
            parts.append(f"TABLESPACE {self.format_identifier(expr.tablespace)}")

        query_sql, query_params = expr.query.to_sql()
        parts.append(f"AS {query_sql}")

        if expr.with_data:
            parts.append("WITH DATA")
        elif expr.no_data:
            parts.append("WITH NO DATA")

        return " ".join(parts), query_params

    def format_drop_materialized_view_statement(self, expr: Any) -> Tuple[str, tuple]:
        """Format DROP MATERIALIZED VIEW statement for PostgreSQL.

        Extends the core formatter with schema qualification and consumes the
        ``cascade`` / ``restrict`` pair. An explicit RESTRICT whose probe is
        False raises instead of being dropped.

        Raises:
            TypeError: ``expr.view`` is not a :class:`MaterializedView`. A plain
                :class:`View` is the wrong kind here: the two share a name shape
                but not a statement, and PostgreSQL refuses ``DROP MATERIALIZED
                VIEW <a view>`` at execution time with a far worse message.
            UnsupportedFeatureError: If RESTRICT is requested but not supported.
        """
        if not isinstance(expr.view, MaterializedView):
            raise TypeError(
                f"{type(expr).__name__}.view must be a MaterializedView, "
                f"got {type(expr.view).__name__}"
            )
        if not self.supports_materialized_view():
            raise UnsupportedFeatureError(self.name, "DROP MATERIALIZED VIEW")

        parts = ["DROP MATERIALIZED VIEW"]
        if expr.if_exists:
            parts.append("IF EXISTS")
        parts.append(self._format_materialized_view_name(expr))
        if expr.cascade:
            parts.append("CASCADE")
        elif expr.restrict:
            if not self.supports_materialized_view_restrict():
                raise UnsupportedFeatureError(
                    self.name,
                    "DROP MATERIALIZED VIEW RESTRICT",
                    f"{self.name} does not support DROP MATERIALIZED VIEW RESTRICT.",
                )
            parts.append("RESTRICT")
        return " ".join(parts), ()

    def format_refresh_materialized_view_statement(self, expr: Any) -> Tuple[str, tuple]:
        """Format REFRESH MATERIALIZED VIEW, gating CONCURRENTLY on PG 9.4+.

        The generic ``RefreshMaterializedViewExpression`` routes here, so the
        version gate also protects callers that do not use the PostgreSQL
        specific expression.
        """
        self._check_concurrent_refresh_support(expr)
        return super().format_refresh_materialized_view_statement(expr)

    def format_refresh_materialized_view_pg_statement(
        self, expr: "PostgresRefreshMaterializedViewExpression"
    ) -> Tuple[str, tuple]:
        """Format REFRESH MATERIALIZED VIEW statement with PG-specific options.

        Args:
            expr: PostgresRefreshMaterializedViewExpression containing all options

        Returns:
            Tuple of (SQL statement, parameters tuple)

        Raises:
            TypeError: ``expr.view`` is not a :class:`MaterializedView`.
            UnsupportedFeatureError: If CONCURRENTLY is requested on PG < 9.4.
        """
        if not isinstance(expr.view, MaterializedView):
            raise TypeError(
                f"{type(expr).__name__}.view must be a MaterializedView, "
                f"got {type(expr.view).__name__}"
            )
        self._check_concurrent_refresh_support(expr)

        parts = ["REFRESH MATERIALIZED VIEW"]
        if expr.concurrent:
            parts.append("CONCURRENTLY")
        parts.append(self._format_materialized_view_name(expr))
        if expr.with_data:
            parts.append("WITH DATA")
        elif expr.no_data:
            parts.append("WITH NO DATA")
        return " ".join(parts), ()

    def format_alter_materialized_view_statement(
        self, expr: "PostgresAlterMaterializedViewExpression"
    ) -> Tuple[str, tuple]:
        """Format ALTER MATERIALIZED VIEW statement.

        Each action renders as its own ``ALTER MATERIALIZED VIEW <target> <action>``
        statement; multiple actions are joined with ``";\\n"``.

        Args:
            expr: PostgresAlterMaterializedViewExpression with the actions to apply

        Returns:
            Tuple of (SQL statement, parameters tuple); ``params`` is always empty.

        Raises:
            UnsupportedFeatureError: If the dialect does not support
                ALTER MATERIALIZED VIEW.
        """
        if not self.supports_alter_materialized_view():
            raise UnsupportedFeatureError(self.name, "ALTER MATERIALIZED VIEW")

        target = self._format_materialized_view_name(expr)
        rendered = []
        for action in expr.actions:
            action_sql, action_params = self.format_materialized_view_alter_action(
                action
            )
            rendered.append(f"ALTER MATERIALIZED VIEW {target} {action_sql}")
        return ";\n".join(rendered), ()

    def format_materialized_view_alter_action(self, expr: Any) -> Tuple[str, tuple]:
        """Render one ALTER MATERIALIZED VIEW action body.

        Returns the ``(sql, params)`` tuple every ``format_*`` method returns.
        The action SQL is a fragment -- it has no ``ALTER MATERIALIZED VIEW
        <name>`` prefix -- but the contract is the contract: each action class
        declares this method as its ``format_method``, so ``to_sql()`` hands the
        return value straight back to its caller. Returning a bare string here
        made ``action.to_sql()`` a string, which then failed to unpack wherever
        someone treated it as the documented tuple.

        Args:
            expr: A ``MaterializedViewAlterAction`` subclass instance.

        Returns:
            A ``(sql, params)`` tuple whose ``sql`` is the action body without
            the leading ``ALTER MATERIALIZED VIEW <name>``. ``params`` is always
            empty -- a storage-parameter name is not a bind parameter.

        Raises:
            UnsupportedFeatureError: If the action is not a known PostgreSQL
                materialized view action.
        """
        if isinstance(expr, PostgresRenameMaterializedViewAction):
            return f"RENAME TO {self.format_identifier(expr.new_name)}", ()
        if isinstance(expr, PostgresSetMaterializedViewSchemaAction):
            return f"SET SCHEMA {self.format_identifier(expr.new_schema)}", ()
        if isinstance(expr, PostgresSetMaterializedViewPropertiesAction):
            return f"SET ({self._format_storage_parameters(expr.properties)})", ()
        if isinstance(expr, PostgresResetMaterializedViewPropertiesAction):
            return (
                f"RESET ({self._format_storage_parameter_names(expr.parameters)})",
                (),
            )
        if isinstance(expr, PostgresChangeMaterializedViewOwnerAction):
            if expr.new_owner.upper() in _CURRENT_ROLE_KEYWORDS:
                return f"OWNER TO {expr.new_owner.upper()}", ()
            return f"OWNER TO {self.format_identifier(expr.new_owner)}", ()
        raise UnsupportedFeatureError(
            self.name,
            f"ALTER MATERIALIZED VIEW action {getattr(expr, 'action_kind', type(expr).__name__)}",
        )

    # ------------------------------------------------------------------
    # Internal checks
    # ------------------------------------------------------------------

    def _check_concurrent_refresh_support(self, expr: Any) -> None:
        """Raise when CONCURRENTLY is requested but unsupported by the server version."""
        if getattr(expr, "concurrent", False) and not self.supports_materialized_view_concurrent_refresh():
            raise UnsupportedFeatureError(
                self.name,
                "REFRESH MATERIALIZED VIEW CONCURRENTLY",
                f"Requires PostgreSQL 9.4+, current version is {self.version}.",
            )
