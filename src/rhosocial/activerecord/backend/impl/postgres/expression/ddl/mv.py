# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/mv.py
"""
PostgreSQL DDL expressions: Materialized View operations.

PostgreSQL Documentation:
- CREATE MATERIALIZED VIEW: https://www.postgresql.org/docs/current/sql-creatematerializedview.html
- REFRESH MATERIALIZED VIEW: https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html
- ALTER MATERIALIZED VIEW: https://www.postgresql.org/docs/current/sql-altermaterializedview.html

Version Requirements:
- Materialized views: PostgreSQL 9.3+
- CONCURRENTLY refresh / IF NOT EXISTS: PostgreSQL 9.4+
- WITH NO DATA: PostgreSQL 9.4+

PostgreSQL's ``ALTER MATERIALIZED VIEW`` does not support ``SET TABLESPACE``:
a materialized view cannot be relocated to another tablespace after creation.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Sequence, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.objects import MaterializedView
from rhosocial.activerecord.backend.expression.statements.ddl_view import (
    CreateMaterializedViewExpression,
    DropMaterializedViewExpression,
    RefreshMaterializedViewExpression,
)

from ...storage_parameters import validate_storage_parameters

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = [
    "MaterializedViewAlterAction",
    "PostgresAlterMaterializedViewExpression",
    "PostgresChangeMaterializedViewOwnerAction",
    "PostgresCreateMaterializedViewExpression",
    "PostgresDropMaterializedViewExpression",
    "PostgresRefreshMaterializedViewExpression",
    "PostgresRenameMaterializedViewAction",
    "PostgresResetMaterializedViewPropertiesAction",
    "PostgresSetMaterializedViewPropertiesAction",
    "PostgresSetMaterializedViewSchemaAction",
]

_CURRENT_ROLE_KEYWORDS = frozenset({"CURRENT_ROLE", "CURRENT_USER", "SESSION_USER"})


def _validate_name(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


class PostgresCreateMaterializedViewExpression(CreateMaterializedViewExpression):
    """PostgreSQL CREATE MATERIALIZED VIEW with schema and IF NOT EXISTS support.

    Extends the generic expression with the PostgreSQL-specific ``IF NOT EXISTS``
    (PG 9.4+). The view is a :class:`MaterializedView` object, so its schema
    lives on the object rather than beside it.

    ``storage_options`` keys are validated against
    :class:`~....storage_parameters.PostgresStorageParameter`; pass
    ``allow_unlisted_storage_parameters=True`` to forward options registered by
    a table access method (``USING method``) or namespaced under ``toast.``.

    Example:
        >>> from rhosocial.activerecord.backend.expression.objects import MaterializedView
        >>> from rhosocial.activerecord.backend.impl.postgres import (
        ...     PostgresDialect,
        ...     PostgresStorageParameter,
        ... )
        >>> dialect = PostgresDialect(version=(15, 0, 0))
        >>> create = PostgresCreateMaterializedViewExpression(
        ...     dialect=dialect,
        ...     view=MaterializedView(dialect, "monthly_sales_summary", schema_name="reporting"),
        ...     query=sales_query,
        ...     if_not_exists=True,
        ...     with_data=True,
        ...     storage_options={PostgresStorageParameter.FILLFACTOR: 70},
        ... )
        >>> sql, params = create.to_sql()
        >>> sql
        'CREATE MATERIALIZED VIEW IF NOT EXISTS "reporting"."monthly_sales_summary" \\
WITH (FILLFACTOR = 70) AS SELECT ... WITH DATA'
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        view: MaterializedView,
        query: Any,
        column_aliases: Optional[List[str]] = None,
        tablespace: Optional[str] = None,
        with_data: bool = False,
        no_data: bool = False,
        storage_options: Optional[Dict[Any, Any]] = None,
        if_not_exists: bool = False,
        allow_unlisted_storage_parameters: bool = False,
    ):
        super().__init__(
            dialect,
            view=view,
            query=query,
            column_aliases=column_aliases,
            tablespace=tablespace,
            with_data=with_data,
            no_data=no_data,
            storage_options=storage_options,
        )
        if storage_options:
            validate_storage_parameters(
                storage_options.keys(),
                "storage_options",
                allow_unlisted=allow_unlisted_storage_parameters,
            )
        self.if_not_exists = if_not_exists
        self.allow_unlisted_storage_parameters = allow_unlisted_storage_parameters


class PostgresDropMaterializedViewExpression(DropMaterializedViewExpression):
    """PostgreSQL DROP MATERIALIZED VIEW.

    Example:
        >>> from rhosocial.activerecord.backend.expression.objects import MaterializedView
        >>> drop = PostgresDropMaterializedViewExpression(
        ...     dialect=dialect,
        ...     view=MaterializedView(dialect, "monthly_sales_summary", schema_name="reporting"),
        ... )
        >>> drop.to_sql()[0]
        'DROP MATERIALIZED VIEW "reporting"."monthly_sales_summary"'
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        view: MaterializedView,
        if_exists: bool = False,
        cascade: bool = False,
        restrict: bool = False,
    ):
        super().__init__(
            dialect,
            view=view,
            if_exists=if_exists,
            cascade=cascade,
            restrict=restrict,
        )


class PostgresRefreshMaterializedViewExpression(RefreshMaterializedViewExpression):
    """PostgreSQL REFRESH MATERIALIZED VIEW statement expression.

    Replaces the contents of a materialized view by recalculating its query.
    The data is replaced atomically without affecting concurrent queries.

    Extends the generic expression with the backward-compatible aliases
    ``name`` and ``concurrently``.

    Attributes:
        view: The materialized view being refreshed (inherited); carries its
            own schema.
        concurrent: Whether to refresh concurrently (inherited).
        with_data: Whether to repopulate data (inherited).
        name: Alias for the view's name.
        concurrently: Alias for concurrent.

    Example:
        >>> from rhosocial.activerecord.backend.expression.objects import MaterializedView
        >>> from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> # Regular refresh
        >>> refresh = PostgresRefreshMaterializedViewExpression(
        ...     dialect=dialect,
        ...     view=MaterializedView(dialect, "monthly_sales_summary"),
        ... )
        >>> sql, params = refresh.to_sql()
        >>> sql
        'REFRESH MATERIALIZED VIEW "monthly_sales_summary"'

        >>> # Concurrent refresh (PG 9.4+, requires unique index)
        >>> refresh = PostgresRefreshMaterializedViewExpression(
        ...     dialect=dialect,
        ...     view=MaterializedView(dialect, "monthly_sales_summary"),
        ...     concurrently=True,
        ... )

        >>> # Refresh without data (create empty, PG 9.4+)
        >>> refresh = PostgresRefreshMaterializedViewExpression(
        ...     dialect=dialect,
        ...     view=MaterializedView(dialect, "monthly_sales_summary"),
        ...     no_data=True,
        ... )

    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        view: MaterializedView,
        concurrently: bool = False,
        with_data: bool = False,
        no_data: bool = False,
    ):
        super().__init__(
            dialect,
            view=view,
            concurrent=concurrently,
            with_data=with_data,
            no_data=no_data,
        )

    @property
    def name(self) -> str:
        """Alias for the view's unqualified name."""
        return self.view.name

    @property
    def schema(self) -> Optional[str]:
        """The view's schema, read off the object."""
        return self.view.schema_name

    @property
    def concurrently(self) -> bool:
        """Alias for concurrent (backward compatibility)."""
        return self.concurrent

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_refresh_materialized_view_pg_statement"


class MaterializedViewAlterAction(BaseExpression, ABC):
    """Base class for one ALTER MATERIALIZED VIEW action.

    Supported actions (PostgreSQL):
    ``RENAME TO`` / ``SET SCHEMA`` / ``SET (...)`` / ``RESET (...)`` / ``OWNER TO``.
    """

    @property
    @abstractmethod
    def action_kind(self) -> str:
        """Stable kind name used by dialect capability dispatch."""

    @property
    def format_method(self) -> str:
        return "format_materialized_view_alter_action"


class PostgresRenameMaterializedViewAction(MaterializedViewAlterAction):
    """``ALTER MATERIALIZED VIEW name RENAME TO new_name``."""

    action_kind = "rename"

    def __init__(self, dialect: "SQLDialectBase", new_name: str) -> None:
        super().__init__(dialect)
        _validate_name(new_name, "new_name")
        self.new_name = new_name


class PostgresSetMaterializedViewSchemaAction(MaterializedViewAlterAction):
    """``ALTER MATERIALIZED VIEW name SET SCHEMA new_schema``."""

    action_kind = "set_schema"

    def __init__(self, dialect: "SQLDialectBase", new_schema: str) -> None:
        super().__init__(dialect)
        _validate_name(new_schema, "new_schema")
        self.new_schema = new_schema


class PostgresSetMaterializedViewPropertiesAction(MaterializedViewAlterAction):
    """``ALTER MATERIALIZED VIEW name SET (storage_parameter [= value] [, ...])``.

    Parameter names are validated against
    :class:`~....storage_parameters.PostgresStorageParameter`.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres.storage_parameters import PostgresStorageParameter
        >>> action = PostgresSetMaterializedViewPropertiesAction(
        ...     dialect,
        ...     {PostgresStorageParameter.FILLFACTOR: 90, "autovacuum_enabled": "true"},
        ... )
    """

    action_kind = "set_properties"

    def __init__(
        self,
        dialect: "SQLDialectBase",
        properties: Dict[Any, Any],
        allow_unlisted_storage_parameters: bool = False,
    ) -> None:
        super().__init__(dialect)
        if not isinstance(properties, dict) or not properties:
            raise ValueError("properties must be a non-empty dict")
        validate_storage_parameters(
            properties.keys(),
            "properties",
            allow_unlisted=allow_unlisted_storage_parameters,
        )
        self.properties = dict(properties)
        self.allow_unlisted_storage_parameters = allow_unlisted_storage_parameters


class PostgresResetMaterializedViewPropertiesAction(MaterializedViewAlterAction):
    """``ALTER MATERIALIZED VIEW name RESET (storage_parameter [, ...])``."""

    action_kind = "reset_properties"

    def __init__(
        self,
        dialect: "SQLDialectBase",
        parameters: Sequence[Any],
        allow_unlisted_storage_parameters: bool = False,
    ) -> None:
        super().__init__(dialect)
        validate_storage_parameters(
            parameters,
            "parameters",
            allow_unlisted=allow_unlisted_storage_parameters,
        )
        if not parameters:
            raise ValueError("parameters must contain at least one storage parameter")
        self.parameters = list(parameters)
        self.allow_unlisted_storage_parameters = allow_unlisted_storage_parameters


class PostgresChangeMaterializedViewOwnerAction(MaterializedViewAlterAction):
    """``ALTER MATERIALIZED VIEW name OWNER TO {new_owner | CURRENT_ROLE | ...}``.

    The ``CURRENT_ROLE`` / ``CURRENT_USER`` / ``SESSION_USER`` keywords are kept
    verbatim; any other value is treated as a role name and quoted.
    """

    action_kind = "change_owner"

    def __init__(self, dialect: "SQLDialectBase", new_owner: str) -> None:
        super().__init__(dialect)
        _validate_name(new_owner, "new_owner")
        self.new_owner = new_owner


class PostgresAlterMaterializedViewExpression(BaseExpression):
    """Expression for ``ALTER MATERIALIZED VIEW``.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
        >>> from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
        ...     PostgresAlterMaterializedViewExpression,
        ...     PostgresRenameMaterializedViewAction,
        ...     PostgresSetMaterializedViewPropertiesAction,
        ... )
        >>> dialect = PostgresDialect(version=(15, 0, 0))
        >>> alter = PostgresAlterMaterializedViewExpression(
        ...     dialect=dialect,
        ...     view=MaterializedView(dialect, "sales_summary"),
        ...     actions=[
        ...         PostgresRenameMaterializedViewAction(dialect, "sales_summary_v2"),
        ...         PostgresSetMaterializedViewPropertiesAction(dialect, {"fillfactor": 90}),
        ...     ],
        ... )
        >>> alter.to_sql()[0]
        'ALTER MATERIALIZED VIEW "sales_summary" RENAME TO "sales_summary_v2";\\n\\
ALTER MATERIALIZED VIEW "sales_summary" SET (FILLFACTOR = 90)'
    """

    def __init__(
        self,
        dialect: "SQLDialectBase",
        view: MaterializedView,
        actions: Sequence[MaterializedViewAlterAction],
    ) -> None:
        super().__init__(dialect)
        action_list = list(actions or [])
        if not action_list:
            raise ValueError("actions must contain at least one MaterializedViewAlterAction")
        for action in action_list:
            if not isinstance(action, MaterializedViewAlterAction):
                raise TypeError(
                    "actions must contain MaterializedViewAlterAction instances, "
                    f"got {type(action).__name__}"
                )
        self.view = view
        self.actions = action_list

    @property
    def format_method(self) -> str:
        return "format_alter_materialized_view_statement"
