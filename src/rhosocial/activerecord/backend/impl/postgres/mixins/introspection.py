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

from typing import List, Optional, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.mixins import IntrospectionMixin
from rhosocial.activerecord.backend.introspection.types import IntrospectionScope

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.introspection import (
        DatabaseInfoQueryStatement,
        TableListQueryStatement,
        TableInfoQueryStatement,
        ColumnInfoQueryStatement,
        IndexInfoQueryStatement,
        ForeignKeyQueryStatement,
        ViewListQueryStatement,
        ViewInfoQueryStatement,
        TriggerListQueryStatement,
        TriggerInfoQueryStatement,
    )
    from ..expression.introspection import (
        PostgresMaterializedViewInfoQueryStatement,
        PostgresMaterializedViewListQueryStatement,
        PostgresSequenceInfoQueryStatement,
        PostgresSequenceListQueryStatement,
        PostgresDatabaseSizeFunction,
        PostgresDatabaseSizeQueryStatement,
    )
    from ..expression.status import (
        PostgresSettingsQuery,
        PostgresActivityCountQuery,
        PostgresDatabaseListQuery,
        PostgresSchemaStatsQuery,
        PostgresCurrentDatabaseQuery,
        PostgresRoleListQuery,
        PostgresCurrentUserQuery,
        PostgresCurrentSchemaQuery,
        PostgresClientAddressQuery,
        PostgresSslSessionQuery,
        PostgresWalPositionQuery,
        PostgresWalFileCountQuery,
        PostgresCheckpointCountQuery,
        PostgresCheckpointLsnQuery,
        PostgresRecoveryStateQuery,
        PostgresReplicationSlotCountQuery,
        PostgresActiveReplicationCountQuery,
        PostgresReplicationLagQuery,
        PostgresArchiveStatsQuery,
        PostgresExtensionListQuery,
    )
    from ..expression.enum_ import (
        PostgresEnumTypeExistsQuery,
        PostgresEnumTypeValuesQuery,
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

    @property
    def default_schema(self) -> str:
        """The default schema name.

        PostgreSQL's default schema is 'public', but may be overridden by
        the connection config's `default_schema` or the first entry of
        `search_path`. Returns 'public' if neither is set.
        """
        config = getattr(self, "_config", None) or getattr(self, "_backend", None)
        cfg = config.config if hasattr(config, "config") else config
        if cfg is not None:
            default_schema = getattr(cfg, "default_schema", None)
            if default_schema:
                return str(default_schema)
            search_path = getattr(cfg, "search_path", None)
            if search_path:
                # search_path 形如 "broker", public → 取第一个有效 schema
                first = str(search_path).split(",")[0].strip().strip('"').strip("'")
                if first:
                    return first
        return "public"

    # ========== Query Formatting Methods ==========
    #
    # Every query below is an expression tree: a projection of Column /
    # FunctionCall / CaseExpression nodes, a relation or a chain of joins, a
    # conjunction of predicates, and an optional ordering. No formatter here
    # writes a clause, a keyword or a separator -- the core renderers do -- and
    # the relation is a Table object, so the pg_catalog names appear once, as
    # the unquoted identifiers they are.
    #
    # Bind values keep the split the concatenated queries had: a caller's
    # schema or object name is data and travels as ``%s``, while a value that
    # is a fact about the catalog (a relkind code, a table-type label, the
    # ``pg_`` prefix filter) is literal-inline and binds nothing.

    #: The label each ``pg_class.relkind`` code reports as ``table_type``.
    #: A catalog fact rather than caller data, so it is written inline.
    _RELKIND_LABELS = (
        ("r", "BASE TABLE"),
        ("v", "VIEW"),
        ("m", "MATERIALIZED VIEW"),
        ("f", "FOREIGN TABLE"),
        ("p", "PARTITIONED TABLE"),
    )

    #: The relkind codes a table listing may be narrowed to, by the type the
    #: caller asked for. ``BASE TABLE`` admits partitioned tables too, which is
    #: why it is a set rather than one code.
    _RELKIND_FILTERS = {
        "BASE TABLE": ("r", "p"),
        "VIEW": ("v",),
        "MATERIALIZED VIEW": ("m",),
    }

    #: Catalog relations whose names are spelled ``pg_``; a listing that does
    #: not ask for system objects excludes them. The percent is doubled because
    #: psycopg reads a bare ``%`` as the start of a placeholder even when the
    #: statement binds nothing.
    _SYSTEM_PREFIX = "pg_%%"

    def _catalog_column(self, name: str, table: "Optional[str]" = None, alias: "Optional[str]" = None):
        """A column reference into pg_catalog, quoted as it is written there.

        The catalog's identifiers are lowercase and never quoted, so a quoted
        *reference* would be a different (and missing) column; the *alias* is
        this query's own output name and is quoted like every other projection
        alias the core renderers write.
        """
        from rhosocial.activerecord.backend.expression import Column

        return Column(
            self,
            name,
            table=table,
            alias=alias,
            name_need_quote=False,
            table_need_quote=False,
        )

    def _catalog_relation(
        self,
        name: str,
        alias: "Optional[str]" = None,
        schema: "Optional[str]" = None,
        schema_need_quote: bool = False,
    ):
        """A catalog relation as a FROM source, with its alias.

        ``schema`` qualifies the name for the relations that live outside
        ``pg_catalog`` -- ``information_schema.sequences`` is the one this
        module reads. Neither part is quoted by default: the catalog spells
        both lower-case and never quoted, so quoting would name something
        else.
        """
        from rhosocial.activerecord.backend.expression.objects import Table
        from rhosocial.activerecord.backend.expression.sources import NamedRelationRef

        return NamedRelationRef(
            self,
            Table(
                self,
                name,
                schema_name=schema,
                schema_need_quote=schema_need_quote,
                name_need_quote=False,
            ),
            alias=alias,
            alias_need_quote=False,
        )

    def _catalog_literal(self, value):
        """A catalog constant: written into the statement, never bound."""
        from rhosocial.activerecord.backend.expression import Literal

        return Literal(self, value, inline=True)

    def _catalog_value(self, value):
        """Caller data: bound, so a name can never become part of the text."""
        from rhosocial.activerecord.backend.expression import Literal

        return Literal(self, value)

    def _catalog_select(
        self,
        select: "list",
        source,
        conditions: "Optional[list]" = None,
        order_by: "Optional[list]" = None,
        group_by: "Optional[list]" = None,
    ) -> Tuple[str, tuple]:
        """Assemble a catalog SELECT from its parts, once.

        Args:
            select: The projection, in order.
            source: The FROM source: a relation or a join chain.
            conditions: Predicates to AND together, or ``None``.
            order_by: Sort expressions, or ``None`` for no ORDER BY.
            group_by: Grouping expressions, or ``None`` for no GROUP BY.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            LogicalPredicate,
            OrderByClause,
            QueryStatement,
            WhereClause,
        )
        from rhosocial.activerecord.backend.expression.query_parts import GroupByHavingClause

        kwargs: "dict" = {"select": select, "from_": source}
        if conditions:
            kwargs["where"] = WhereClause(self, LogicalPredicate(self, "AND", *conditions))
        if group_by:
            kwargs["group_by_having"] = GroupByHavingClause(self, group_by=group_by)
        if order_by:
            kwargs["order_by"] = OrderByClause(self, order_by)
        return QueryStatement(self, **kwargs).to_sql()

    def _relkind_case(self, alias: str):
        """The CASE that names a relation's kind, shared by the table listings."""
        from rhosocial.activerecord.backend.expression.advanced_functions import CaseExpression

        return CaseExpression(
            self,
            value=self._catalog_column("relkind", "c"),
            cases=[
                (self._catalog_literal(code), self._catalog_literal(label))
                for code, label in self._RELKIND_LABELS
            ],
            alias=alias,
        )

    def _relation_projection(self) -> "list":
        """The five columns a table listing and a single-table read both report."""
        from rhosocial.activerecord.backend.expression import FunctionCall
        from rhosocial.activerecord.backend.expression.types import BigIntType

        return [
            self._catalog_column("relname", "c", alias="table_name"),
            self._relkind_case("table_type"),
            FunctionCall(
                self, "obj_description", self._catalog_column("oid", "c"), alias="comment"
            ),
            self._catalog_column("reltuples", "c").cast(BigIntType(self)).as_("row_count"),
            FunctionCall(
                self,
                "pg_total_relation_size",
                self._catalog_column("oid", "c"),
                alias="size_bytes",
            ),
        ]

    def _pg_class_join(self):
        """``pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid``."""
        from rhosocial.activerecord.backend.expression import ComparisonPredicate, JoinClause

        return JoinClause(
            self,
            left_table=self._catalog_relation("pg_class", "c"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "c"),
                self._catalog_column("oid", "n"),
            ),
        )

    def format_database_info_query_statement(self, expr: "DatabaseInfoQueryStatement") -> Tuple[str, tuple]:
        """Format database information query.

        Uses pg_database and version() to get database metadata: the row the
        server answers with describes the database this session is connected
        to, so the WHERE clause names ``current_database()`` on both sides.

        Args:
            expr: Database info expression with parameters.

        Returns:
            Tuple of (SQL string, empty parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate, FunctionCall

        current_database = FunctionCall(self, "current_database")
        return self._catalog_select(
            [
                FunctionCall(self, "version", alias="version"),
                FunctionCall(self, "current_database", alias="name"),
                FunctionCall(
                    self, "pg_encoding_to_char", self._catalog_column("encoding"), alias="encoding"
                ),
                self._catalog_column("datcollate", alias="collation"),
                FunctionCall(self, "pg_database_size", current_database, alias="size"),
            ],
            self._catalog_relation("pg_database"),
            [
                ComparisonPredicate(
                    self, "eq", self._catalog_column("datname"), current_database
                )
            ],
        )

    def format_table_list_query_statement(self, expr: "TableListQueryStatement") -> Tuple[str, tuple]:
        """Format table list query using pg_catalog.

        Uses pg_class and pg_namespace to query table information.
        The relkind field distinguishes table types:
        - 'r': Ordinary table (BASE TABLE)
        - 'v': View
        - 'm': Materialized view
        - 'f': Foreign table
        - 'p': Partitioned table

        The relkind-IN filter and the ``table_type`` narrowing are separate
        conditions, exactly as the concatenated form spelled them: the first
        says which kinds a listing may contain at all, the second narrows to the
        one the caller asked for, and the two stay visible here rather than
        folded into one expression the caller would have to reconstruct.

        Args:
            expr: Table list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            InPredicate,
            LikeOperator,
            LikePredicate,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        include_views = params.get("include_views", True)
        include_system = params.get("include_system", False)
        table_type = params.get("table_type")

        relkind_filter = ("r", "v", "m", "f", "p") if include_views else ("r", "f", "p")
        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            InPredicate(
                self,
                self._catalog_column("relkind", "c"),
                self._catalog_literal(relkind_filter),
            ),
        ]

        if not include_system:
            conditions.append(
                LikePredicate(
                    self,
                    LikeOperator.NOT_LIKE,
                    self._catalog_column("relname", "c"),
                    self._catalog_literal(self._SYSTEM_PREFIX),
                )
            )

        relkind_narrowing = self._RELKIND_FILTERS.get(table_type)
        if relkind_narrowing is not None:
            # One code narrows by equality, several by membership -- the same
            # split the concatenated branches wrote (``IN ('r', 'p')`` for a
            # base table, ``= 'v'`` for a view).
            if len(relkind_narrowing) == 1:
                conditions.append(
                    ComparisonPredicate(
                        self,
                        "eq",
                        self._catalog_column("relkind", "c"),
                        self._catalog_literal(relkind_narrowing[0]),
                    )
                )
            else:
                conditions.append(
                    InPredicate(
                        self,
                        self._catalog_column("relkind", "c"),
                        self._catalog_literal(relkind_narrowing),
                    )
                )

        return self._catalog_select(
            self._relation_projection(),
            self._pg_class_join(),
            conditions,
            [self._catalog_column("relname", "c")],
        )

    def format_table_info_query_statement(self, expr: "TableInfoQueryStatement") -> Tuple[str, tuple]:
        """Format single table information query.

        Query pg_class for table details.

        Args:
            expr: Table info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema") or self.default_schema

        return self._catalog_select(
            self._relation_projection(),
            self._pg_class_join(),
            [
                ComparisonPredicate(
                    self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "c"),
                    self._catalog_value(table_name),
                ),
            ],
        )

    def format_column_info_query_statement(self, expr: "ColumnInfoQueryStatement") -> Tuple[str, tuple]:
        """Format column information query using pg_attribute.

        Uses pg_attribute, pg_attrdef, and pg_class to query column metadata.
        The attnum > 0 condition excludes system columns (ctid, oid, etc.).
        The attisdropped = false condition excludes dropped columns.

        ``is_primary_key`` is an EXISTS subquery, and it is a node: the nested
        QueryStatement is what the predicate holds, so the primary-key test is
        inspectable rather than a parenthesised string. ``a.attnum = ANY(...)``
        is a quantified comparison against another column, which is exactly
        :class:`~...expression.advanced_functions.AnyPredicate`'s shape.

        Args:
            expr: Column info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
            LogicalPredicate,
            QueryStatement,
            Subquery,
            WhereClause,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import (
            AnyPredicate,
            ExistsPredicate,
        )

        params = expr.get_params()
        table_name = params.get("table_name", "")
        include_hidden = params.get("include_hidden", False)
        schema = params.get("schema") or self.default_schema

        # pg_index row for the primary-key test, over the column's own table.
        primary_key_index = QueryStatement(
            self,
            select=[self._catalog_literal(1)],
            from_=self._catalog_relation("pg_index", "i"),
            where=WhereClause(
                self,
                LogicalPredicate(
                    self,
                    "AND",
                    ComparisonPredicate(
                        self,
                        "eq",
                        self._catalog_column("indrelid", "i"),
                        self._catalog_column("attrelid", "a"),
                    ),
                    self._catalog_column("indisprimary", "i"),
                    AnyPredicate(
                        self,
                        self._catalog_column("attnum", "a"),
                        "=",
                        Subquery(self, self._catalog_column("indkey", "i")),
                    ),
                ),
            ),
        )

        projection = [
            self._catalog_column("attname", "a", alias="column_name"),
            self._catalog_column("attnum", "a", alias="ordinal_position"),
            FunctionCall(
                self,
                "pg_catalog.format_type",
                self._catalog_column("atttypid", "a"),
                self._catalog_column("atttypmod", "a"),
                alias="data_type",
            ),
            # ``NOT a.attnotnull``: a truth test on one column, aliased into the
            # projection. The NOT is the logical node, so the negation is a
            # node rather than an operator glued to the front of a string.
            LogicalPredicate(self, "NOT", self._catalog_column("attnotnull", "a")).as_(
                "is_nullable"
            ),
            FunctionCall(
                self,
                "pg_get_expr",
                self._catalog_column("adbin", "d"),
                self._catalog_column("adrelid", "d"),
                alias="default_value",
            ),
            ExistsPredicate(self, primary_key_index).as_("is_primary_key"),
            FunctionCall(
                self,
                "col_description",
                self._catalog_column("attrelid", "a"),
                self._catalog_column("attnum", "a"),
                alias="comment",
            ),
        ]

        attribute_join = JoinClause(
            self,
            left_table=self._catalog_relation("pg_attribute", "a"),
            right_table=self._catalog_relation("pg_attrdef", "d"),
            join_type="LEFT",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("attrelid", "a"),
                    self._catalog_column("adrelid", "d"),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("attnum", "a"),
                    self._catalog_column("adnum", "d"),
                ),
            ),
        )
        source = attribute_join.join(
            self._catalog_relation("pg_class", "c"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("attrelid", "a"),
                self._catalog_column("oid", "c"),
            ),
        ).join(
            self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "c"),
                self._catalog_column("oid", "n"),
            ),
        )

        # The base conditions exclude dropped columns and system columns; the
        # attisdropped repeat below is what this query has always rendered for
        # a caller who did not ask for hidden columns, and it is kept as a
        # second condition so the bytes (and the parameters) stay as they were.
        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relname", "c"),
                self._catalog_value(table_name),
            ),
            ComparisonPredicate(
                self,
                "gt",
                self._catalog_column("attnum", "a"),
                self._catalog_literal(0),
            ),
            LogicalPredicate(self, "NOT", self._catalog_column("attisdropped", "a")),
        ]
        if not include_hidden:
            conditions.append(
                LogicalPredicate(self, "NOT", self._catalog_column("attisdropped", "a"))
            )

        return self._catalog_select(
            projection, source, conditions, [self._catalog_column("attnum", "a")]
        )

    def format_index_info_query_statement(self, expr: "IndexInfoQueryStatement") -> Tuple[str, tuple]:
        """Format index information query using pg_index.

        Uses pg_index, pg_class, pg_am, and pg_attribute to query index metadata.
        The indkey array contains column positions for the index.
        The amname field indicates the index type (btree, hash, gin, gist, etc.).

        ``a.attnum = ANY(ix.indkey)`` is the join condition that walks an index's
        column list, and it is an :class:`AnyPredicate` over the array column --
        the same node the column query uses for the primary-key test.

        Args:
            expr: Index info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
            LogicalPredicate,
            Subquery,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import AnyPredicate

        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema") or self.default_schema

        column_position = FunctionCall(
            self,
            "array_position",
            self._catalog_column("indkey", "ix"),
            self._catalog_column("attnum", "a"),
            alias="column_position",
        )
        projection = [
            self._catalog_column("relname", "i", alias="index_name"),
            self._catalog_column("attname", "a", alias="column_name"),
            column_position,
            self._catalog_column("indisunique", "ix", alias="is_unique"),
            self._catalog_column("indisprimary", "ix", alias="is_primary"),
            self._catalog_column("amname", "am", alias="index_type"),
            self._catalog_column("indpred", "ix", alias="filter_condition"),
        ]

        # The join chain is the order the concatenated FROM clause wrote, so the
        # rendered statement reads the same way from left to right.
        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_index", "ix"),
            right_table=self._catalog_relation("pg_class", "t"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("indrelid", "ix"),
                self._catalog_column("oid", "t"),
            ),
        )
        for relation, alias, left_side, right_side in (
            ("pg_class", "i", ("indexrelid", "ix"), ("oid", "i")),
            ("pg_namespace", "n", ("relnamespace", "t"), ("oid", "n")),
            ("pg_am", "am", ("relam", "i"), ("oid", "am")),
        ):
            source = source.join(
                self._catalog_relation(relation, alias),
                join_type="JOIN",
                condition=ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column(*left_side),
                    self._catalog_column(*right_side),
                ),
            )

        source = source.join(
            self._catalog_relation("pg_attribute", "a"),
            join_type="JOIN",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("attrelid", "a"),
                    self._catalog_column("oid", "t"),
                ),
                AnyPredicate(
                    self,
                    self._catalog_column("attnum", "a"),
                    "=",
                    Subquery(self, self._catalog_column("indkey", "ix")),
                ),
            ),
        )

        return self._catalog_select(
            projection,
            source,
            [
                ComparisonPredicate(
                    self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "t"),
                    self._catalog_value(table_name),
                ),
            ],
            # ``column_position`` names the projection's alias, exactly as the
            # concatenated form ordered by it.
            [
                self._catalog_column("relname", "i"),
                self._catalog_column("column_position"),
            ],
        )

    def format_foreign_key_query_statement(self, expr: "ForeignKeyQueryStatement") -> Tuple[str, tuple]:
        """Format foreign key information query using pg_constraint.

        Uses pg_constraint, pg_class, pg_attribute, and pg_namespace to query
        foreign key metadata. The contype = 'f' filters for foreign key constraints.

        Referential action codes (confdeltype/confupdtype):
        - 'a': NO ACTION
        - 'r': RESTRICT
        - 'c': CASCADE
        - 'n': SET NULL
        - 'd': SET DEFAULT

        The referenced column is picked by a subscript on ``con.confkey``: the
        position of this side's column inside ``conkey`` indexes the other side's
        list. That subscript is an :class:`ArrayOperation`, so the pairing is a
        structure rather than a bracketed fragment of text.

        Args:
            expr: Foreign key expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
            LogicalPredicate,
            Subquery,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import (
            AnyPredicate,
            ArrayOperation,
        )
        from rhosocial.activerecord.backend.expression.types import TextType

        params = expr.get_params()
        table_name = params.get("table_name", "")
        schema = params.get("schema") or self.default_schema

        column_position = FunctionCall(
            self,
            "array_position",
            self._catalog_column("conkey", "con"),
            self._catalog_column("attnum", "a"),
            alias="column_position",
        )
        referenced_key = ArrayOperation(
            self,
            operation="ACCESS",
            base_expr=self._catalog_column("confkey", "con"),
            index_expr=FunctionCall(
                self,
                "array_position",
                self._catalog_column("conkey", "con"),
                self._catalog_column("attnum", "a"),
            ),
        )

        projection = [
            self._catalog_column("conname", "con", alias="constraint_name"),
            self._catalog_column("attname", "a", alias="column_name"),
            self._catalog_column("relname", "ref_class", alias="referenced_table"),
            self._catalog_column("nspname", "ref_ns", alias="referenced_schema"),
            self._catalog_column("attname", "ref_a", alias="referenced_column"),
            self._catalog_column("confdeltype", "con").cast(TextType(self)).as_("on_delete"),
            self._catalog_column("confupdtype", "con").cast(TextType(self)).as_("on_update"),
            column_position,
        ]

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_constraint", "con"),
            right_table=self._catalog_relation("pg_class", "class"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("conrelid", "con"),
                self._catalog_column("oid", "class"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_namespace", "ns"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "class"),
                self._catalog_column("oid", "ns"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_attribute", "a"),
            join_type="JOIN",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("attrelid", "a"),
                    self._catalog_column("oid", "class"),
                ),
                AnyPredicate(
                    self,
                    self._catalog_column("attnum", "a"),
                    "=",
                    Subquery(self, self._catalog_column("conkey", "con")),
                ),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_class", "ref_class"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("confrelid", "con"),
                self._catalog_column("oid", "ref_class"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_namespace", "ref_ns"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "ref_class"),
                self._catalog_column("oid", "ref_ns"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_attribute", "ref_a"),
            join_type="JOIN",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("attrelid", "ref_a"),
                    self._catalog_column("oid", "ref_class"),
                ),
                ComparisonPredicate(
                    self, "eq", self._catalog_column("attnum", "ref_a"), referenced_key
                ),
            ),
        )

        return self._catalog_select(
            projection,
            source,
            [
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("contype", "con"),
                    self._catalog_literal("f"),
                ),
                ComparisonPredicate(
                    self, "eq", self._catalog_column("nspname", "ns"), self._catalog_value(schema)
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "class"),
                    self._catalog_value(table_name),
                ),
            ],
            [
                self._catalog_column("conname", "con"),
                self._catalog_column("column_position"),
            ],
        )

    def format_view_list_query_statement(self, expr: "ViewListQueryStatement") -> Tuple[str, tuple]:
        """Format view list query using pg_class.

        Uses pg_class with relkind = 'v' to query view information.
        pg_get_viewdef() returns the view definition SQL.

        Args:
            expr: View list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            LikeOperator,
            LikePredicate,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        include_system = params.get("include_system", False)

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relkind", "c"),
                self._catalog_literal("v"),
            ),
        ]
        if not include_system:
            conditions.append(
                LikePredicate(
                    self,
                    LikeOperator.NOT_LIKE,
                    self._catalog_column("relname", "c"),
                    self._catalog_literal(self._SYSTEM_PREFIX),
                )
            )

        return self._catalog_select(
            self._view_projection(),
            self._pg_class_join(),
            conditions,
            [self._catalog_column("relname", "c")],
        )

    def _view_projection(self) -> "list":
        """The three columns a view listing and a single-view read both report."""
        from rhosocial.activerecord.backend.expression import FunctionCall

        return [
            self._catalog_column("relname", "c", alias="view_name"),
            FunctionCall(
                self,
                "pg_get_viewdef",
                self._catalog_column("oid", "c"),
                self._catalog_literal(True),
                alias="definition",
            ),
            FunctionCall(
                self, "obj_description", self._catalog_column("oid", "c"), alias="comment"
            ),
        ]

    def format_view_info_query_statement(self, expr: "ViewInfoQueryStatement") -> Tuple[str, tuple]:
        """Format single view information query.

        Query pg_class for view details.

        Args:
            expr: View info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        params = expr.get_params()
        view_name = params.get("view_name", "")
        schema = params.get("schema") or self.default_schema

        return self._catalog_select(
            self._view_projection(),
            self._pg_class_join(),
            [
                ComparisonPredicate(
                    self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "c"),
                    self._catalog_value(view_name),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relkind", "c"),
                    self._catalog_literal("v"),
                ),
            ],
        )

    def _matview_source(self):
        """``pg_matviews m JOIN pg_class c ... JOIN pg_namespace n ...``.

        The ``pg_class`` pair carries two comparisons: the name, and the
        namespace it resolves in. The namespace half is what makes this a
        correct read rather than a lucky one -- ``relname`` is unique per
        namespace, not per database, so pairing on the name alone lets a
        same-named row in *any* schema stand in for the one asked about. When
        that row had just been dropped by a concurrent worker the server
        refuses the query with "could not open relation with OID", which is
        what the parallelised suite reported.

        The ``pg_namespace`` join keeps its own two comparisons: the OID pair
        ties the relation to its namespace, and the name pair re-checks the
        schema ``pg_matviews`` reports against it.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            JoinClause,
            LogicalPredicate,
            QueryStatement,
            WhereClause,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import (
            ExistsPredicate,
        )

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_matviews", "m"),
            right_table=self._catalog_relation("pg_class", "c"),
            join_type="JOIN",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "c"),
                    self._catalog_column("matviewname", "m"),
                ),
                # c.relnamespace == (the namespace whose name is m.schemaname).
                # Written as a scalar subquery because pg_class carries no
                # namespace name, only its OID, and the comparison has to
                # resolve through pg_namespace to be a real test.
                ExistsPredicate(
                    self,
                    QueryStatement(
                        self,
                        select=[self._catalog_literal(1)],
                        from_=self._catalog_relation("pg_namespace", "n2"),
                        where=WhereClause(
                            self,
                            LogicalPredicate(
                                self,
                                "AND",
                                ComparisonPredicate(
                                    self,
                                    "eq",
                                    self._catalog_column("oid", "n2"),
                                    self._catalog_column("relnamespace", "c"),
                                ),
                                ComparisonPredicate(
                                    self,
                                    "eq",
                                    self._catalog_column("nspname", "n2"),
                                    self._catalog_column("schemaname", "m"),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
        return source.join(
            self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("oid", "n"),
                    self._catalog_column("relnamespace", "c"),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("nspname", "n"),
                    self._catalog_column("schemaname", "m"),
                ),
            ),
        )

    def _matview_projection(self) -> "list":
        """The materialized-view row shape, including the unique-index test.

        ``has_unique_index`` is the EXISTS subquery ``REFRESH ... CONCURRENTLY``
        depends on, and it is a nested :class:`QueryStatement` rather than a
        parenthesised string.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            LogicalPredicate,
            QueryStatement,
            WhereClause,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import ExistsPredicate

        unique_index = QueryStatement(
            self,
            select=[self._catalog_literal(1)],
            from_=self._catalog_relation("pg_index", "i"),
            where=WhereClause(
                self,
                LogicalPredicate(
                    self,
                    "AND",
                    ComparisonPredicate(
                        self,
                        "eq",
                        self._catalog_column("indrelid", "i"),
                        self._catalog_column("oid", "c"),
                    ),
                    self._catalog_column("indisunique", "i"),
                ),
            ),
        )

        return [
            self._catalog_column("schemaname", "m", alias="schema_name"),
            self._catalog_column("matviewname", "m", alias="view_name"),
            self._catalog_column("definition", "m", alias="definition"),
            self._catalog_column("ispopulated", "m", alias="is_populated"),
            FunctionCall(
                self, "obj_description", self._catalog_column("oid", "c"), alias="comment"
            ),
            ExistsPredicate(self, unique_index).as_("has_unique_index"),
            FunctionCall(
                self,
                "pg_get_viewdef",
                self._catalog_column("oid", "c"),
                self._catalog_literal(True),
                alias="view_definition",
            ),
        ]

    def format_materialized_view_list_query_statement(
        self, expr: "PostgresMaterializedViewListQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format materialized view list query.

        Uses ``pg_matviews`` (a view over ``pg_class``/``pg_rewrite``) joined with
        ``pg_index`` so callers can tell whether a materialized view is populated
        and whether it carries the UNIQUE index that ``REFRESH ... CONCURRENTLY``
        requires.

        Args:
            expr: Materialized view list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            LikeOperator,
            LikePredicate,
        )

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        include_system = params.get("include_system", False)

        conditions = [
            ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("schemaname", "m"),
                self._catalog_value(schema),
            )
        ]
        if not include_system:
            # NOTE: doubled percent -- psycopg treats a bare '%' as a placeholder start.
            conditions.append(
                LikePredicate(
                    self,
                    LikeOperator.NOT_LIKE,
                    self._catalog_column("matviewname", "m"),
                    self._catalog_literal(self._SYSTEM_PREFIX),
                )
            )

        return self._catalog_select(
            self._matview_projection(),
            self._matview_source(),
            conditions,
            [self._catalog_column("matviewname", "m")],
        )

    def format_materialized_view_info_query_statement(
        self, expr: "PostgresMaterializedViewInfoQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format single materialized view information query.

        Args:
            expr: Materialized view info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        params = expr.get_params()
        view_name = params.get("view_name", "")
        schema = params.get("schema") or self.default_schema

        return self._catalog_select(
            self._matview_projection(),
            self._matview_source(),
            [
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("schemaname", "m"),
                    self._catalog_value(schema),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("matviewname", "m"),
                    self._catalog_value(view_name),
                ),
            ],
        )

    def _sequence_source(self, include_pg_sequences: bool = True):
        """``information_schema.sequences s`` with ``pg_sequences p`` beside it.

        The standard view is the object list: it exists on every server this
        backend supports. ``pg_sequences`` carries the state the standard view
        omits, and arrived in PostgreSQL 10, so it is joined only when the
        caller says the server has it. The join is a LEFT JOIN because
        ``pg_sequences`` omits sequences the current role cannot read; those
        rows must still be listed, with the withheld columns as NULL.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            JoinClause,
            LogicalPredicate,
        )

        sequences = self._catalog_relation("sequences", "s", schema="information_schema")
        if not include_pg_sequences:
            return sequences

        return JoinClause(
            self,
            left_table=sequences,
            right_table=self._catalog_relation("pg_sequences", "p"),
            join_type="LEFT JOIN",
            condition=LogicalPredicate(
                self,
                "AND",
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("sequence_schema", "s"),
                    self._catalog_column("schemaname", "p"),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("sequence_name", "s"),
                    self._catalog_column("sequencename", "p"),
                ),
            ),
        )

    def _sequence_projection(self, include_pg_sequences: bool = True) -> "list":
        """The sequence row shape.

        ``information_schema`` reports the numeric attributes as
        ``character_data``, so each is cast to ``bigint`` -- the type the
        server actually stores -- rather than left for the caller to parse.
        """
        from rhosocial.activerecord.backend.expression.types import BigIntType

        def numeric(name: str):
            return self._catalog_column(name, "s").cast(BigIntType(self))

        projection = [
            self._catalog_column("sequence_schema", "s", alias="schema_name"),
            self._catalog_column("sequence_name", "s", alias="sequence_name"),
            self._catalog_column("data_type", "s", alias="data_type"),
            numeric("start_value").as_("start_value"),
            numeric("minimum_value").as_("minimum_value"),
            numeric("maximum_value").as_("maximum_value"),
            numeric("increment").as_("increment"),
            self._catalog_column("cycle_option", "s", alias="cycle"),
        ]
        if include_pg_sequences:
            projection.extend(
                [
                    self._catalog_column("cache_size", "p", alias="cache_size"),
                    self._catalog_column("last_value", "p", alias="last_value"),
                ]
            )
        return projection

    def format_sequence_list_query_statement(
        self, expr: "PostgresSequenceListQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the sequence list query for one schema.

        Args:
            expr: Sequence list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        params = expr.get_params()
        schema = params.get("schema") or self.default_schema
        include_pg_sequences = params.get("include_pg_sequences", True)

        return self._catalog_select(
            self._sequence_projection(include_pg_sequences),
            self._sequence_source(include_pg_sequences),
            [
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("sequence_schema", "s"),
                    self._catalog_value(schema),
                )
            ],
            [self._catalog_column("sequence_name", "s")],
        )

    def format_sequence_info_query_statement(
        self, expr: "PostgresSequenceInfoQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the single-sequence query.

        Args:
            expr: Sequence info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate

        params = expr.get_params()
        sequence_name = params.get("sequence_name", "")
        schema = params.get("schema") or self.default_schema
        include_pg_sequences = params.get("include_pg_sequences", True)

        return self._catalog_select(
            self._sequence_projection(include_pg_sequences),
            self._sequence_source(include_pg_sequences),
            [
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("sequence_schema", "s"),
                    self._catalog_value(schema),
                ),
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("sequence_name", "s"),
                    self._catalog_value(sequence_name),
                ),
            ],
        )

    def _trigger_source(self):
        """``pg_trigger t JOIN pg_class c ... JOIN pg_namespace n ... JOIN pg_proc p ...``."""
        from rhosocial.activerecord.backend.expression import ComparisonPredicate, JoinClause

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_trigger", "t"),
            right_table=self._catalog_relation("pg_class", "c"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("tgrelid", "t"),
                self._catalog_column("oid", "c"),
            ),
        )
        source = source.join(
            self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "c"),
                self._catalog_column("oid", "n"),
            ),
        )
        return source.join(
            self._catalog_relation("pg_proc", "p"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("tgfoid", "t"),
                self._catalog_column("oid", "p"),
            ),
        )

    def _trigger_projection(self) -> "list":
        """The trigger row shape, with ``timing`` derived from the tgtype bitmask.

        The bitmask test is a CASE whose conditions compare an arithmetic
        expression against its own mask value: ``tgtype::integer & 64 = 64``
        is ``BinaryExpression`` on the left and a literal on the right, so the
        bit test is a structure rather than a fragment of text.
        """
        from rhosocial.activerecord.backend.expression import (
            BinaryExpression,
            ComparisonPredicate,
            FunctionCall,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import CaseExpression
        from rhosocial.activerecord.backend.expression.types import IntegerType

        def masked(mask: int):
            return BinaryExpression(
                self,
                "&",
                self._catalog_column("tgtype", "t").cast(IntegerType(self)),
                self._catalog_literal(mask),
            )

        def mask_test(mask: int):
            return ComparisonPredicate(
                self, "eq", masked(mask), self._catalog_literal(mask)
            )

        return [
            self._catalog_column("tgname", "t", alias="trigger_name"),
            self._catalog_column("relname", "c", alias="table_name"),
            CaseExpression(
                self,
                cases=[
                    (mask_test(64), self._catalog_literal("INSTEAD OF")),
                    (mask_test(2), self._catalog_literal("BEFORE")),
                ],
                else_result=self._catalog_literal("AFTER"),
                alias="timing",
            ),
            self._catalog_column("proname", "p", alias="function_name"),
            self._catalog_column("tgenabled", "t", alias="enabled"),
            FunctionCall(
                self,
                "pg_get_triggerdef",
                self._catalog_column("oid", "t"),
                self._catalog_literal(True),
                alias="definition",
            ),
            self._catalog_column("tgtype", "t", alias="trigger_type"),
        ]

    def format_trigger_list_query_statement(self, expr: "TriggerListQueryStatement") -> Tuple[str, tuple]:
        """Format trigger list query using pg_trigger.

        Uses pg_trigger, pg_class, pg_namespace, and pg_proc to query trigger metadata.
        The tgisinternal = false condition excludes system-generated triggers.

        Trigger timing is encoded in tgtype bitmask:
        - Bit 1 (2): BEFORE trigger
        - Bit 6 (64): INSTEAD OF trigger
        - Neither: AFTER trigger (default)

        Note: Bits 2-5 encode trigger events (INSERT, DELETE, UPDATE, TRUNCATE),
        not timing.

        Filtering by table does not change the ordering: every trigger row of a
        table is still reported in trigger-name order, which is what the
        concatenated form rendered on both branches (only its leading
        whitespace differed).

        Args:
            expr: Trigger list expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate, LogicalPredicate

        params = expr.get_params()
        table_name = params.get("table_name")
        schema = params.get("schema") or self.default_schema

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            LogicalPredicate(self, "NOT", self._catalog_column("tgisinternal", "t")),
        ]
        if table_name:
            conditions.append(
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "c"),
                    self._catalog_value(table_name),
                )
            )

        return self._catalog_select(
            self._trigger_projection(),
            self._trigger_source(),
            conditions,
            [self._catalog_column("tgname", "t")],
        )

    def format_trigger_info_query_statement(self, expr: "TriggerInfoQueryStatement") -> Tuple[str, tuple]:
        """Format single trigger information query.

        Query pg_trigger for trigger details.

        Args:
            expr: Trigger info expression with parameters.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import ComparisonPredicate, LogicalPredicate

        params = expr.get_params()
        trigger_name = params.get("trigger_name", "")
        table_name = params.get("table_name")
        schema = params.get("schema") or self.default_schema

        conditions = [
            ComparisonPredicate(
                self, "eq", self._catalog_column("nspname", "n"), self._catalog_value(schema)
            ),
            ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("tgname", "t"),
                self._catalog_value(trigger_name),
            ),
            LogicalPredicate(self, "NOT", self._catalog_column("tgisinternal", "t")),
        ]
        if table_name:
            conditions.append(
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("relname", "c"),
                    self._catalog_value(table_name),
                )
            )

        return self._catalog_select(self._trigger_projection(), self._trigger_source(), conditions)

    # ========== PostgreSQL database size ==========

    def format_postgres_database_size_function(
        self, expr: "PostgresDatabaseSizeFunction"
    ) -> Tuple[str, tuple]:
        """Format the ``pg_database_size(name)`` call.

        The name is a bound value: PostgreSQL parses the argument, so it travels
        through the driver's placeholder rather than being spelled into the text.

        Args:
            expr: Database size function expression with the database name.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import FunctionCall

        params = expr.get_params()
        return FunctionCall(
            self, "pg_database_size", self._catalog_value(params.get("database_name"))
        ).to_sql()

    def format_database_size_query_statement(
        self, expr: "PostgresDatabaseSizeQueryStatement"
    ) -> Tuple[str, tuple]:
        """Format the query that reports a database's size in bytes.

        The SELECT and the ``size_bytes`` alias belong to this query, not to
        the function: the caller reads the row by that column name, so the row
        shape is the query node's contract and the function's spelling stays
        the function node's. The call is the same :class:`FunctionCall` node the
        function formatter writes -- the alias is this query's.

        Args:
            expr: Database size query expression with the database name.

        Returns:
            Tuple of (SQL string, parameters tuple).
        """
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement

        params = expr.get_params()
        return QueryStatement(
            self,
            select=[
                FunctionCall(
                    self,
                    "pg_database_size",
                    self._catalog_value(params.get("database_name")),
                    alias="size_bytes",
                )
            ],
        ).to_sql()


    # ------------------------------------------------------------------
    # Status and enum queries (org guide §16.2: the SQL text is the
    # formatter's; the status introspector and the enum manager only
    # construct these nodes and execute them).
    # ------------------------------------------------------------------

    def format_postgres_settings_query(
        self, expr: "PostgresSettingsQuery"
    ) -> Tuple[str, tuple]:
        """Read ``pg_settings`` rows, optionally filtered by name.

        The name list is data, so it binds: an ``IN`` list of caller-supplied
        setting names travels as one parameter per name, in the order given.
        """
        from rhosocial.activerecord.backend.expression import InPredicate

        conditions = []
        if expr.names:
            conditions.append(
                InPredicate(
                    self,
                    self._catalog_column("name"),
                    self._catalog_value(list(expr.names)),
                )
            )

        return self._catalog_select(
            [
                self._catalog_column("name"),
                self._catalog_column("setting"),
                self._catalog_column("unit"),
            ],
            self._catalog_relation("pg_settings"),
            conditions,
            [self._catalog_column("name")],
        )

    def format_postgres_activity_count_query(
        self, expr: "PostgresActivityCountQuery"
    ) -> Tuple[str, tuple]:
        """Count rows in ``pg_stat_activity``."""
        from rhosocial.activerecord.backend.expression import QueryStatement, RowCountWildcard
        from rhosocial.activerecord.backend.expression.functions import CountFunction

        return QueryStatement(
            self,
            select=[CountFunction(self, RowCountWildcard(self)).as_("total_connections")],
            from_=self._catalog_relation("pg_stat_activity"),
        ).to_sql()

    def format_postgres_database_list_query(
        self, expr: "PostgresDatabaseListQuery"
    ) -> Tuple[str, tuple]:
        """List databases with sizes, non-templates plus the current one."""
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            LogicalPredicate,
        )

        return self._catalog_select(
            [
                self._catalog_column("datname", alias="name"),
                FunctionCall(
                    self,
                    "pg_database_size",
                    self._catalog_column("datname"),
                    alias="size_bytes",
                ),
            ],
            self._catalog_relation("pg_database"),
            [
                LogicalPredicate(
                    self,
                    "OR",
                    ComparisonPredicate(
                        self,
                        "eq",
                        self._catalog_column("datistemplate"),
                        self._catalog_literal(False),
                    ),
                    ComparisonPredicate(
                        self,
                        "eq",
                        self._catalog_column("datname"),
                        FunctionCall(self, "current_database"),
                    ),
                )
            ],
            [self._catalog_column("datname")],
        )

    def format_postgres_schema_stats_query(
        self, expr: "PostgresSchemaStatsQuery"
    ) -> Tuple[str, tuple]:
        """Per-schema table and view counts."""
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            InPredicate,
            JoinClause,
            LogicalPredicate,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import CaseExpression
        from rhosocial.activerecord.backend.expression.functions import CountFunction

        def counted(relkind: str):
            return CountFunction(
                self,
                CaseExpression(
                    self,
                    cases=[
                        (
                            ComparisonPredicate(
                                self,
                                "eq",
                                self._catalog_column("relkind", "c"),
                                self._catalog_literal(relkind),
                            ),
                            self._catalog_literal(1),
                        )
                    ]
                ),
            )

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_namespace", "n"),
            right_table=self._catalog_relation("pg_class", "c"),
            join_type="LEFT",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("relnamespace", "c"),
                self._catalog_column("oid", "n"),
            ),
        )

        return self._catalog_select(
            [
                self._catalog_column("nspname", "n", alias="schema_name"),
                counted("r").as_("table_count"),
                counted("v").as_("view_count"),
            ],
            source,
            [
                LogicalPredicate(
                    self,
                    "NOT",
                    InPredicate(
                        self,
                        self._catalog_column("nspname", "n"),
                        self._catalog_literal(
                            ("pg_catalog", "information_schema", "pg_toast")
                        ),
                    ),
                )
            ],
            [self._catalog_column("nspname", "n")],
            group_by=[self._catalog_column("nspname", "n")],
        )

    def format_postgres_current_database_query(
        self, expr: "PostgresCurrentDatabaseQuery"
    ) -> Tuple[str, tuple]:
        """The current database's name."""
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement

        return QueryStatement(self, select=[FunctionCall(self, "current_database")]).to_sql()

    def format_postgres_role_list_query(
        self, expr: "PostgresRoleListQuery"
    ) -> Tuple[str, tuple]:
        """List roles with superuser flag and owner."""
        from rhosocial.activerecord.backend.expression import FunctionCall

        return self._catalog_select(
            [
                self._catalog_column("rolname", alias="name"),
                self._catalog_column("rolsuper", alias="is_superuser"),
                FunctionCall(
                    self,
                    "pg_catalog.pg_get_userbyid",
                    self._catalog_column("oid"),
                    alias="owner",
                ),
            ],
            self._catalog_relation("pg_roles"),
            order_by=[self._catalog_column("rolname")],
        )

    # ------------------------------------------------------------------
    # The session / WAL / replication / archive reads
    # ------------------------------------------------------------------

    def format_postgres_current_user_query(
        self, expr: "PostgresCurrentUserQuery"
    ) -> Tuple[str, tuple]:
        """The session's role.

        ``CURRENT_USER`` is niladic: the standard writes it without parentheses,
        and PostgreSQL refuses ``CURRENT_USER()`` (measured: syntax error at or
        near ``"("`` on 17.11). ``niladic`` is what tells the call renderer.
        """
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement

        return QueryStatement(
            self, select=[FunctionCall(self, "current_user", niladic=True)]
        ).to_sql()

    def format_postgres_current_schema_query(
        self, expr: "PostgresCurrentSchemaQuery"
    ) -> Tuple[str, tuple]:
        """The session's current schema."""
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement

        return QueryStatement(self, select=[FunctionCall(self, "current_schema")]).to_sql()

    def format_postgres_client_address_query(
        self, expr: "PostgresClientAddressQuery"
    ) -> Tuple[str, tuple]:
        """The client's address (null over a Unix-domain socket)."""
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement

        return QueryStatement(self, select=[FunctionCall(self, "inet_client_addr")]).to_sql()

    def format_postgres_ssl_session_query(
        self, expr: "PostgresSslSessionQuery"
    ) -> Tuple[str, tuple]:
        """This session's SSL state: three columns of its own ``pg_stat_ssl`` row."""
        from rhosocial.activerecord.backend.expression import ComparisonPredicate, FunctionCall

        return self._catalog_select(
            [
                self._catalog_column("ssl"),
                self._catalog_column("version"),
                self._catalog_column("cipher"),
            ],
            self._catalog_relation("pg_stat_ssl"),
            [
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("pid"),
                    FunctionCall(self, "pg_backend_pid"),
                )
            ],
        )

    def format_postgres_wal_position_query(
        self, expr: "PostgresWalPositionQuery"
    ) -> Tuple[str, tuple]:
        """Bytes of WAL written, as the LSN distance from ``0/0``.

        The function pair is version-dependent: ``wal`` from PostgreSQL 10, and
        ``xlog`` before it. The cast to BIGINT is the row's type, not a
        presentation choice -- the difference is a ``numeric``.
        """
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement
        from rhosocial.activerecord.backend.expression.types import BigIntType

        current, diff = (
            ("pg_current_wal_lsn", "pg_wal_lsn_diff")
            if self.version >= (10, 0, 0)
            else ("pg_current_xlog_location", "pg_xlog_location_diff")
        )
        return QueryStatement(
            self,
            select=[
                FunctionCall(
                    self,
                    diff,
                    FunctionCall(self, current),
                    self._catalog_literal("0/0"),
                )
                .cast(BigIntType(self))
                .as_("wal_bytes")
            ],
        ).to_sql()

    def format_postgres_wal_file_count_query(
        self, expr: "PostgresWalFileCountQuery"
    ) -> Tuple[str, tuple]:
        """How many files the WAL directory holds.

        ``pg_ls_waldir()`` is a set-returning function, so it is a row source:
        the count reads its rows rather than calling it in the select list.
        It exists from PostgreSQL 10, which is why the version refuses it below.
        """
        from rhosocial.activerecord.backend.expression import QueryStatement, RowCountWildcard
        from rhosocial.activerecord.backend.expression.functions import CountFunction
        from rhosocial.activerecord.backend.expression.sources import TableFunctionSource
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

        if self.version < (10, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "pg_ls_waldir()",
                "The WAL directory listing was introduced in PostgreSQL 10; "
                "count the files with pg_ls_dir('pg_wal') on older servers.",
            )

        return QueryStatement(
            self,
            select=[CountFunction(self, RowCountWildcard(self)).as_("wal_files")],
            from_=TableFunctionSource(self, "pg_ls_waldir"),
        ).to_sql()

    def format_postgres_checkpoint_count_query(
        self, expr: "PostgresCheckpointCountQuery"
    ) -> Tuple[str, tuple]:
        """Checkpoints run so far: the timed and requested counters, summed.

        The counters moved from ``pg_stat_bgwriter`` to ``pg_stat_checkpointer``
        in PostgreSQL 17, and the column names changed with them; both the
        relation and the names are therefore the server version's.
        """
        from rhosocial.activerecord.backend.expression import (
            FunctionCall,
            QueryStatement,
        )
        from rhosocial.activerecord.backend.expression.operators import ArithmeticOperation

        if self.version >= (17, 0, 0):
            relation, timed, requested = "pg_stat_checkpointer", "num_timed", "num_requested"
        else:
            relation, timed, requested = "pg_stat_bgwriter", "checkpoints_timed", "checkpoints_req"

        def coalesced(column: str):
            return FunctionCall(
                self, "coalesce", self._catalog_column(column), self._catalog_literal(0)
            )

        return QueryStatement(
            self,
            select=[
                ArithmeticOperation(self, "add", coalesced(timed), coalesced(requested)).as_(
                    "checkpoint_count"
                )
            ],
            from_=self._catalog_relation(relation),
        ).to_sql()

    def format_postgres_checkpoint_lsn_query(
        self, expr: "PostgresCheckpointLsnQuery"
    ) -> Tuple[str, tuple]:
        """The last checkpoint's LSN.

        ``pg_control_checkpoint()`` answers one composite row, and its
        ``checkpoint_lsn`` field is read by naming the function as a row source
        and selecting the field off the alias. The earlier form
        ``(pg_control_checkpoint()).checkpoint_lsn`` puts a parenthesised call in
        the select list, which PostgreSQL refuses (measured 17.11: syntax error
        at or near ``"."``).
        """
        from rhosocial.activerecord.backend.expression import QueryStatement
        from rhosocial.activerecord.backend.expression.sources import TableFunctionSource
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

        if self.version < (9, 6, 0):
            raise UnsupportedFeatureError(
                self.name,
                "pg_control_checkpoint()",
                "The control-data accessor was introduced in PostgreSQL 9.6.",
            )

        return QueryStatement(
            self,
            select=[self._catalog_column("checkpoint_lsn", "c", alias="checkpoint_info")],
            from_=TableFunctionSource(self, "pg_control_checkpoint", alias="c"),
        ).to_sql()

    def format_postgres_recovery_state_query(
        self, expr: "PostgresRecoveryStateQuery"
    ) -> Tuple[str, tuple]:
        """Whether this server is replaying WAL (a standby)."""
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement

        return QueryStatement(self, select=[FunctionCall(self, "pg_is_in_recovery")]).to_sql()

    def format_postgres_replication_slot_count_query(
        self, expr: "PostgresReplicationSlotCountQuery"
    ) -> Tuple[str, tuple]:
        """How many replication slots exist."""
        from rhosocial.activerecord.backend.expression import QueryStatement, RowCountWildcard
        from rhosocial.activerecord.backend.expression.functions import CountFunction

        return QueryStatement(
            self,
            select=[CountFunction(self, RowCountWildcard(self)).as_("slot_count")],
            from_=self._catalog_relation("pg_replication_slots"),
        ).to_sql()

    def format_postgres_active_replication_count_query(
        self, expr: "PostgresActiveReplicationCountQuery"
    ) -> Tuple[str, tuple]:
        """How many standbys are streaming right now."""
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            QueryStatement,
            RowCountWildcard,
        )
        from rhosocial.activerecord.backend.expression.functions import CountFunction

        return QueryStatement(
            self,
            select=[CountFunction(self, RowCountWildcard(self)).as_("active_count")],
            from_=self._catalog_relation("pg_stat_replication"),
            where=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("state"),
                self._catalog_literal("streaming"),
            ),
        ).to_sql()

    def format_postgres_replication_lag_query(
        self, expr: "PostgresReplicationLagQuery"
    ) -> Tuple[str, tuple]:
        """The replay lag in bytes, as the receive/replay LSN distance.

        The three function names moved together in PostgreSQL 10 (``xlog`` to
        ``wal``), so the pair is chosen as a pair: a 9.x server has no
        ``pg_wal_lsn_diff`` at all.
        """
        from rhosocial.activerecord.backend.expression import FunctionCall, QueryStatement
        from rhosocial.activerecord.backend.expression.types import BigIntType

        if self.version >= (10, 0, 0):
            diff, receive, replay = (
                "pg_wal_lsn_diff",
                "pg_last_wal_receive_lsn",
                "pg_last_wal_replay_lsn",
            )
        else:
            diff, receive, replay = (
                "pg_xlog_location_diff",
                "pg_last_xlog_receive_location",
                "pg_last_xlog_replay_location",
            )

        return QueryStatement(
            self,
            select=[
                FunctionCall(
                    self,
                    diff,
                    FunctionCall(self, receive),
                    FunctionCall(self, replay),
                )
                .cast(BigIntType(self))
                .as_("lag_bytes")
            ],
        ).to_sql()

    def format_postgres_archive_stats_query(
        self, expr: "PostgresArchiveStatsQuery"
    ) -> Tuple[str, tuple]:
        """The archiver's counters and its last success/failure times."""
        from rhosocial.activerecord.backend.expression import QueryStatement
        from rhosocial.activerecord.backend.expression.types import TextType

        return QueryStatement(
            self,
            select=[
                self._catalog_column("archived_count"),
                self._catalog_column("failed_count"),
                self._catalog_column("last_archived_time")
                .cast(TextType(self))
                .as_("last_archived"),
                self._catalog_column("last_failed_time")
                .cast(TextType(self))
                .as_("last_failed"),
            ],
            from_=self._catalog_relation("pg_stat_archiver"),
        ).to_sql()

    def format_postgres_extension_list_query(
        self, expr: "PostgresExtensionListQuery"
    ) -> Tuple[str, tuple]:
        """Every installed extension with its schema and description."""
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            FunctionCall,
            JoinClause,
        )

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_extension", "e"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="LEFT",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("oid", "n"),
                self._catalog_column("extnamespace", "e"),
            ),
        )

        return self._catalog_select(
            [
                self._catalog_column("extname", "e", alias="name"),
                self._catalog_column("extversion", "e", alias="version"),
                self._catalog_column("nspname", "n", alias="schema"),
                FunctionCall(
                    self,
                    "pg_catalog.obj_description",
                    self._catalog_column("oid", "e"),
                    self._catalog_literal("pg_extension"),
                    alias="description",
                ),
            ],
            source,
            order_by=[self._catalog_column("extname", "e")],
        )

    def format_postgres_enum_type_exists_query(
        self, expr: "PostgresEnumTypeExistsQuery"
    ) -> Tuple[str, tuple]:
        """Whether an enum type exists; name (and optional schema) are bound.

        The whole question is one EXISTS over a join between ``pg_type`` and
        ``pg_namespace``, so the select list is the EXISTS predicate itself and
        the subquery is a QueryStatement.

        Raises:
            ValueError: The enum name is blank, which no catalog row can match.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            JoinClause,
            LogicalPredicate,
            QueryStatement,
            WhereClause,
        )
        from rhosocial.activerecord.backend.expression.advanced_functions import ExistsPredicate

        if not expr.name:
            raise ValueError(
                "PostgresEnumTypeExistsQuery needs a type name: an empty name "
                "cannot match a catalog row."
            )

        conditions = [
            ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("typname", "t"),
                self._catalog_value(expr.name),
            )
        ]
        if expr.schema is not None:
            conditions.append(
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("nspname", "n"),
                    self._catalog_value(expr.schema),
                )
            )

        subquery = QueryStatement(
            self,
            select=[self._catalog_literal(1)],
            from_=JoinClause(
                self,
                left_table=self._catalog_relation("pg_type", "t"),
                right_table=self._catalog_relation("pg_namespace", "n"),
                join_type="JOIN",
                condition=ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("typnamespace", "t"),
                    self._catalog_column("oid", "n"),
                ),
            ),
            where=WhereClause(self, LogicalPredicate(self, "AND", *conditions)),
        )
        return QueryStatement(self, select=[ExistsPredicate(self, subquery)]).to_sql()

    def format_postgres_enum_type_values_query(
        self, expr: "PostgresEnumTypeValuesQuery"
    ) -> Tuple[str, tuple]:
        """The labels of an enum type in sort order; name (and schema) are bound.

        Raises:
            ValueError: The enum name is blank, which no catalog row can match.
        """
        from rhosocial.activerecord.backend.expression import (
            ComparisonPredicate,
            JoinClause,
        )

        if not expr.name:
            raise ValueError(
                "PostgresEnumTypeValuesQuery needs a type name: an empty name "
                "cannot match a catalog row."
            )

        conditions = [
            ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("typname", "t"),
                self._catalog_value(expr.name),
            )
        ]
        if expr.schema is not None:
            conditions.append(
                ComparisonPredicate(
                    self,
                    "eq",
                    self._catalog_column("nspname", "n"),
                    self._catalog_value(expr.schema),
                )
            )

        source = JoinClause(
            self,
            left_table=self._catalog_relation("pg_type", "t"),
            right_table=self._catalog_relation("pg_namespace", "n"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("typnamespace", "t"),
                self._catalog_column("oid", "n"),
            ),
        ).join(
            self._catalog_relation("pg_enum", "e"),
            join_type="JOIN",
            condition=ComparisonPredicate(
                self,
                "eq",
                self._catalog_column("oid", "t"),
                self._catalog_column("enumtypid", "e"),
            ),
        )

        return self._catalog_select(
            [self._catalog_column("enumlabel", "e")],
            source,
            conditions,
            [self._catalog_column("enumsortorder", "e")],
        )


__all__ = ["PostgresIntrospectionCapabilityMixin"]
