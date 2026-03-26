# src/rhosocial/activerecord/backend/impl/postgres/mixins/introspection.py
"""
PostgreSQL introspection capability mixin for dialect layer.

This mixin declares introspection capabilities for PostgreSQL dialect.
It follows the two-layer introspection architecture:
- Dialect layer (this file): Declares what the database CAN do via supports_* methods
  and generates SQL via format_*_query methods
- Backend layer (introspection.py): Implements HOW to query via _query_* methods

PostgreSQL supports comprehensive introspection through pg_catalog system tables.

Key pg_catalog tables used:
- pg_database: Database information
- pg_class/pg_namespace: Table information
- pg_attribute: Column metadata
- pg_index: Index information
- pg_constraint: Foreign key constraints
- pg_trigger: Trigger information
- pg_proc: Function/procedure information

Reference: .claude/plan/db-introspection-guide.md
"""

from typing import List, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.mixins import IntrospectionMixin
from rhosocial.activerecord.backend.introspection.types import IntrospectionScope

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.introspection import (
        DatabaseInfoExpression,
        TableListExpression,
        TableInfoExpression,
        ColumnInfoExpression,
        IndexInfoExpression,
        ForeignKeyExpression,
        ViewListExpression,
        ViewInfoExpression,
        TriggerListExpression,
        TriggerInfoExpression,
    )


class PostgresIntrospectionCapabilityMixin(IntrospectionMixin):
    """
    PostgreSQL introspection capability declaration for dialect layer.

    Declares that PostgreSQL supports all introspection features through
    its comprehensive pg_catalog system tables.

    This mixin is used by PostgresDialect to declare introspection capabilities.
    The actual implementation is in PostgreSQLIntrospectionMixin (backend layer).

    Dialects declare capabilities (supports_* methods) and generate SQL
    (format_*_query methods), but do not execute queries.
    """

    # ========== Capability Detection ==========

    def supports_introspection(self) -> bool:
        """PostgreSQL supports introspection."""
        return True

    def supports_database_info(self) -> bool:
        """PostgreSQL supports database info via pg_database."""
        return True

    def supports_table_introspection(self) -> bool:
        """PostgreSQL supports table introspection via pg_class."""
        return True

    def supports_column_introspection(self) -> bool:
        """PostgreSQL supports column introspection via pg_attribute."""
        return True

    def supports_index_introspection(self) -> bool:
        """PostgreSQL supports index introspection via pg_index."""
        return True

    def supports_foreign_key_introspection(self) -> bool:
        """PostgreSQL supports foreign key introspection via pg_constraint."""
        return True

    def supports_view_introspection(self) -> bool:
        """PostgreSQL supports view introspection via pg_views."""
        return True

    def supports_trigger_introspection(self) -> bool:
        """PostgreSQL supports trigger introspection via pg_trigger."""
        return True

    def get_supported_introspection_scopes(self) -> List[IntrospectionScope]:
        """Get list of supported introspection scopes."""
        return [
            IntrospectionScope.DATABASE,
            IntrospectionScope.TABLE,
            IntrospectionScope.COLUMN,
            IntrospectionScope.INDEX,
            IntrospectionScope.FOREIGN_KEY,
            IntrospectionScope.VIEW,
            IntrospectionScope.TRIGGER,
        ]

    # ========== Helper Methods ==========

    def _quote_identifier(self, name: str) -> str:
        """Quote an identifier for PostgreSQL.

        PostgreSQL uses double quotes for identifiers.

        Args:
            name: The identifier name to quote.

        Returns:
            The quoted identifier.
        """
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def _get_default_schema(self) -> str:
        """Get default schema name.

        PostgreSQL's default schema is 'public'.

        Returns:
            The default schema name 'public'.
        """
        return 'public'

    # ========== Query Formatting Methods ==========

    def format_database_info_query(self, expr: "DatabaseInfoExpression") -> Tuple[str, tuple]:
        """Format database information query.

        Uses pg_database and version() to get database metadata.

        Args:
            expr: Database info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        params = expr.get_params()
        db_name = params.get("database_name")

        sql = """
            SELECT version() as version,
                   current_database() as name,
                   pg_encoding_to_char(encoding) as encoding,
                   datcollate as collation,
                   pg_database_size(current_database()) as size
            FROM pg_database
            WHERE datname = current_database()
        """
        return (sql, ())

    def format_table_list_query(self, expr: "TableListExpression") -> Tuple[str, tuple]:
        """Format table list query using pg_catalog.

        Uses pg_class and pg_namespace to query table information.
        The relkind field distinguishes table types:
        - 'r': Ordinary table (BASE TABLE)
        - 'v': View
        - 'm': Materialized view
        - 'f': Foreign table
        - 'p': Partitioned table

        Args:
            expr: Table list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        params = expr.get_params()
        schema = params.get("schema") or self._get_default_schema()
        include_views = params.get("include_views", True)
        include_system = params.get("include_system", False)
        table_type = params.get("table_type")

        # Build relkind filter
        if include_views:
            relkind_filter = "('r', 'v', 'm', 'f', 'p')"
        else:
            relkind_filter = "('r', 'f', 'p')"

        sql = f"""
            SELECT c.relname as table_name,
                   CASE c.relkind
                       WHEN 'r' THEN 'BASE TABLE'
                       WHEN 'v' THEN 'VIEW'
                       WHEN 'm' THEN 'MATERIALIZED VIEW'
                       WHEN 'f' THEN 'FOREIGN TABLE'
                       WHEN 'p' THEN 'PARTITIONED TABLE'
                   END as table_type,
                   obj_description(c.oid) as comment,
                   c.reltuples::bigint as row_count,
                   pg_total_relation_size(c.oid) as size_bytes
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = %s
                AND c.relkind IN {relkind_filter}
        """

        params_list = [schema]

        if not include_system:
            sql += " AND c.relname NOT LIKE 'pg_%'"

        if table_type:
            if table_type == "BASE TABLE":
                sql += " AND c.relkind IN ('r', 'p')"
            elif table_type == "VIEW":
                sql += " AND c.relkind = 'v'"
            elif table_type == "MATERIALIZED VIEW":
                sql += " AND c.relkind = 'm'"

        sql += " ORDER BY c.relname"

        return (sql, tuple(params_list))

    def format_table_info_query(self, expr: "TableInfoExpression") -> Tuple[str, tuple]:
        """Format single table information query.

        Query pg_class for table details.

        Args:
            expr: Table info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema") or self._get_default_schema()

        sql = """
            SELECT c.relname as table_name,
                   CASE c.relkind
                       WHEN 'r' THEN 'BASE TABLE'
                       WHEN 'v' THEN 'VIEW'
                       WHEN 'm' THEN 'MATERIALIZED VIEW'
                       WHEN 'f' THEN 'FOREIGN TABLE'
                       WHEN 'p' THEN 'PARTITIONED TABLE'
                   END as table_type,
                   obj_description(c.oid) as comment,
                   c.reltuples::bigint as row_count,
                   pg_total_relation_size(c.oid) as size_bytes
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = %s AND c.relname = %s
        """

        return (sql, (schema, table_name))

    def format_column_info_query(self, expr: "ColumnInfoExpression") -> Tuple[str, tuple]:
        """Format column information query using pg_attribute.

        Uses pg_attribute, pg_attrdef, and pg_class to query column metadata.
        The attnum > 0 condition excludes system columns (ctid, oid, etc.).
        The attisdropped = false condition excludes dropped columns.

        Args:
            expr: Column info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        params = expr.get_params()
        table_name = params.get("table_name", "")
        include_hidden = params.get("include_hidden", False)
        schema = params.get("schema") or self._get_default_schema()

        # Base query with standard columns
        sql = """
            SELECT a.attname as column_name,
                   a.attnum as ordinal_position,
                   pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
                   NOT a.attnotnull as is_nullable,
                   pg_get_expr(d.adbin, d.adrelid) as default_value,
                   (EXISTS (
                       SELECT 1 FROM pg_index i
                       WHERE i.indrelid = a.attrelid
                       AND i.indisprimary
                       AND a.attnum = ANY(i.indkey)
                   )) as is_primary_key,
                   col_description(a.attrelid, a.attnum) as comment
            FROM pg_attribute a
            LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = %s
                AND c.relname = %s
                AND a.attnum > 0
                AND NOT a.attisdropped
        """

        # If not including hidden columns, add filter
        if not include_hidden:
            sql += " AND NOT a.attisdropped"

        sql += " ORDER BY a.attnum"

        return (sql, (schema, table_name))

    def format_index_info_query(self, expr: "IndexInfoExpression") -> Tuple[str, tuple]:
        """Format index information query using pg_index.

        Uses pg_index, pg_class, pg_am, and pg_attribute to query index metadata.
        The indkey array contains column positions for the index.
        The amname field indicates the index type (btree, hash, gin, gist, etc.).

        Args:
            expr: Index info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema") or self._get_default_schema()

        sql = """
            SELECT i.relname as index_name,
                   a.attname as column_name,
                   array_position(ix.indkey, a.attnum) as column_position,
                   ix.indisunique as is_unique,
                   ix.indisprimary as is_primary,
                   am.amname as index_type,
                   ix.indpred as filter_condition
            FROM pg_index ix
            JOIN pg_class t ON ix.indrelid = t.oid
            JOIN pg_class i ON ix.indexrelid = i.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            JOIN pg_am am ON i.relam = am.oid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE n.nspname = %s AND t.relname = %s
            ORDER BY i.relname, column_position
        """

        return (sql, (schema, table_name))

    def format_foreign_key_query(self, expr: "ForeignKeyExpression") -> Tuple[str, tuple]:
        """Format foreign key information query using pg_constraint.

        Uses pg_constraint, pg_class, pg_attribute, and pg_namespace to query
        foreign key metadata. The contype = 'f' filters for foreign key constraints.

        Referential action codes (confdeltype/confupdtype):
        - 'a': NO ACTION
        - 'r': RESTRICT
        - 'c': CASCADE
        - 'n': SET NULL
        - 'd': SET DEFAULT

        Args:
            expr: Foreign key expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema") or self._get_default_schema()

        sql = """
            SELECT
                con.conname as constraint_name,
                a.attname as column_name,
                ref_class.relname as referenced_table,
                ref_ns.nspname as referenced_schema,
                ref_a.attname as referenced_column,
                con.confdeltype::text as on_delete,
                con.confupdtype::text as on_update,
                array_position(con.conkey, a.attnum) as column_position
            FROM pg_constraint con
            JOIN pg_class class ON con.conrelid = class.oid
            JOIN pg_namespace ns ON class.relnamespace = ns.oid
            JOIN pg_attribute a ON a.attrelid = class.oid AND a.attnum = ANY(con.conkey)
            JOIN pg_class ref_class ON con.confrelid = ref_class.oid
            JOIN pg_namespace ref_ns ON ref_class.relnamespace = ref_ns.oid
            JOIN pg_attribute ref_a ON ref_a.attrelid = ref_class.oid
                AND ref_a.attnum = con.confkey[array_position(con.conkey, a.attnum)]
            WHERE con.contype = 'f'
                AND ns.nspname = %s
                AND class.relname = %s
            ORDER BY con.conname, column_position
        """

        return (sql, (schema, table_name))

    def format_view_list_query(self, expr: "ViewListExpression") -> Tuple[str, tuple]:
        """Format view list query using pg_class.

        Uses pg_class with relkind = 'v' to query view information.
        pg_get_viewdef() returns the view definition SQL.

        Args:
            expr: View list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        params = expr.get_params()
        schema = params.get("schema") or self._get_default_schema()
        include_system = params.get("include_system", False)

        sql = """
            SELECT c.relname as view_name,
                   pg_get_viewdef(c.oid, true) as definition,
                   obj_description(c.oid) as comment
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = %s AND c.relkind = 'v'
        """

        params_list = [schema]

        if not include_system:
            sql += " AND c.relname NOT LIKE 'pg_%'"

        sql += " ORDER BY c.relname"

        return (sql, tuple(params_list))

    def format_view_info_query(self, expr: "ViewInfoExpression") -> Tuple[str, tuple]:
        """Format single view information query.

        Query pg_class for view details.

        Args:
            expr: View info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        params = expr.get_params()
        view_name = params.get("view_name", "")
        schema = params.get("schema") or self._get_default_schema()

        sql = """
            SELECT c.relname as view_name,
                   pg_get_viewdef(c.oid, true) as definition,
                   obj_description(c.oid) as comment
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'v'
        """

        return (sql, (schema, view_name))

    def format_trigger_list_query(self, expr: "TriggerListExpression") -> Tuple[str, tuple]:
        """Format trigger list query using pg_trigger.

        Uses pg_trigger, pg_class, pg_namespace, and pg_proc to query trigger metadata.
        The tgisinternal = false condition excludes system-generated triggers.

        Trigger timing is encoded in tgtype:
        - Bit 1: BEFORE (2)
        - Bit 2: AFTER (4)
        - Bit 3: INSTEAD OF (8)

        Args:
            expr: Trigger list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        params = expr.get_params()
        table_name = params.get("table_name")
        schema = params.get("schema") or self._get_default_schema()

        sql = """
            SELECT
                t.tgname as trigger_name,
                c.relname as table_name,
                CASE
                    WHEN t.tgtype::integer & 2 = 2 THEN 'BEFORE'
                    WHEN t.tgtype::integer & 4 = 4 THEN 'AFTER'
                    WHEN t.tgtype::integer & 8 = 8 THEN 'INSTEAD OF'
                END as timing,
                p.proname as function_name,
                t.tgenabled as enabled,
                pg_get_triggerdef(t.oid, true) as definition,
                t.tgtype as trigger_type
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_proc p ON t.tgfoid = p.oid
            WHERE n.nspname = %s
                AND NOT t.tgisinternal
        """

        params_list = [schema]

        if table_name:
            sql += " AND c.relname = %s"
            params_list.append(table_name)

        sql += " ORDER BY t.tgname"

        return (sql, tuple(params_list))

    def format_trigger_info_query(self, expr: "TriggerInfoExpression") -> Tuple[str, tuple]:
        """Format single trigger information query.

        Query pg_trigger for trigger details.

        Args:
            expr: Trigger info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        params = expr.get_params()
        trigger_name = params.get("trigger_name", "")
        table_name = params.get("table_name")
        schema = params.get("schema") or self._get_default_schema()

        sql = """
            SELECT
                t.tgname as trigger_name,
                c.relname as table_name,
                CASE
                    WHEN t.tgtype::integer & 2 = 2 THEN 'BEFORE'
                    WHEN t.tgtype::integer & 4 = 4 THEN 'AFTER'
                    WHEN t.tgtype::integer & 8 = 8 THEN 'INSTEAD OF'
                END as timing,
                p.proname as function_name,
                t.tgenabled as enabled,
                pg_get_triggerdef(t.oid, true) as definition,
                t.tgtype as trigger_type
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_proc p ON t.tgfoid = p.oid
            WHERE n.nspname = %s
                AND t.tgname = %s
                AND NOT t.tgisinternal
        """

        params_list = [schema, trigger_name]

        if table_name:
            sql += " AND c.relname = %s"
            params_list.append(table_name)

        return (sql, tuple(params_list))


__all__ = ['PostgresIntrospectionCapabilityMixin']
