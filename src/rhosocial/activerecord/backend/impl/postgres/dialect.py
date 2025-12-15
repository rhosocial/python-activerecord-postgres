# src/rhosocial/activerecord/backend/impl/postgres/dialect.py
import re
from enum import Enum
from typing import Optional, List, Any, Set, Dict, Tuple, Union

from rhosocial.activerecord.backend.dialect import (
    SQLExpressionBase, SQLDialectBase, ReturningClauseHandler, ExplainOptions, ExplainType, ExplainFormat, SQLBuilder,
    AggregateHandler, JsonOperationHandler, CTEHandler
)
from rhosocial.activerecord.backend.errors import GroupingSetNotSupportedError, WindowFunctionNotSupportedError, \
    JsonOperationNotSupportedError
from rhosocial.activerecord.backend.typing import ConnectionConfig

# postgres version boundary constants
PG_8_4 = (8, 4, 0)  # First version with window functions and CTE
PG_9_1 = (9, 1, 0)  # Support for unlogged tables, serializable isolation level improvements
PG_9_2 = (9, 2, 0)  # JSON support introduced
PG_9_3 = (9, 3, 0)  # LATERAL JOIN support
PG_9_4 = (9, 4, 0)  # FILTER clause support, JSONB type
PG_9_5 = (9, 5, 0)  # GROUPING SETS, CUBE, and ROLLUP support
PG_9_6 = (9, 6, 0)  # Parallel query execution, phrase search in FTS
PG_10_0 = (10, 0, 0)  # Logical replication, declarative table partitioning
PG_11_0 = (11, 0, 0)  # GROUPS frames support, stored procedures, JIT compilation
PG_12_0 = (12, 0, 0)  # Generated columns, improvements to indexing
PG_13_0 = (13, 0, 0)  # Enhanced DISTINCT ON support, incremental sorting
PG_14_0 = (14, 0, 0)  # Multirange types, improved query planner
PG_15_0 = (15, 0, 0)  # MERGE command, compression for tables
PG_16_0 = (16, 0, 0)  # Anti-forgery token for SQL/JSON path expressions


# Driver type enum
class DriverType(Enum):
    PSYCOPG = "psycopg"
    PSYCOPG2 = "psycopg2"
    ASYNCPG = "asyncpg"
    PG8000 = "pg8000"


# Helper function for version comparison
def _is_version_at_least(current_version, required_version):
    """Check if current version is at least the required version"""
    return current_version >= required_version

class PostgresExpression(SQLExpressionBase):
    """postgres expression implementation"""

    def format(self, dialect: SQLDialectBase) -> str:
        """Format postgres expression"""
        return self.expression


class PostgresReturningHandler(ReturningClauseHandler):
    """
    postgres RETURNING clause handler implementation.

    postgres has the most comprehensive RETURNING clause support,
    including expressions, aliases, and functions.
    """

    @property
    def is_supported(self) -> bool:
        """
        Check if RETURNING clause is supported.

        postgres has always supported RETURNING.

        Returns:
            bool: Always True for postgres
        """
        return True

    def format_clause(self, columns: Optional[List[str]] = None) -> str:
        """
        Format RETURNING clause.

        Args:
            columns: Column names to return. None means all columns (*).

        Returns:
            str: Formatted RETURNING clause
        """
        if not columns:
            return "RETURNING *"

        # Validate and escape each column name
        safe_columns = [self._validate_column_name(col) for col in columns]
        return f"RETURNING {', '.join(safe_columns)}"

    def format_advanced_clause(self,
                               columns: Optional[List[str]] = None,
                               expressions: Optional[List[Dict[str, Any]]] = None,
                               aliases: Optional[Dict[str, str]] = None,
                               dialect_options: Optional[Dict[str, Any]] = None) -> str:
        """
        Format advanced RETURNING clause for postgres.

        postgres supports expressions, functions, and subqueries in RETURNING.

        Args:
            columns: List of column names to return
            expressions: List of expressions to return
            aliases: Dictionary mapping column/expression names to aliases
            dialect_options: postgres-specific options

        Returns:
            str: Formatted RETURNING clause
        """
        # Process expressions and aliases
        items = []

        # Add columns with potential aliases
        if columns:
            for col in columns:
                alias = aliases.get(col) if aliases else None
                if alias:
                    items.append(f"{self._validate_column_name(col)} AS {self._validate_column_name(alias)}")
                else:
                    items.append(self._validate_column_name(col))

        # Add expressions with potential aliases
        if expressions:
            for expr in expressions:
                expr_text = expr.get("expression", "")
                expr_alias = expr.get("alias")
                # Check for function calls that need special handling
                if expr.get("type") == "function":
                    func_name = expr.get("function_name", "")
                    args = expr.get("args", [])
                    expr_text = f"{func_name}({', '.join(args)})"

                if expr_alias:
                    items.append(f"{expr_text} AS {self._validate_column_name(expr_alias)}")
                else:
                    items.append(expr_text)

        # Check for CTE (WITH clause) in RETURNING
        if dialect_options and "with_clause" in dialect_options:
            with_clause = dialect_options["with_clause"]
            # Add WITH clause to RETURNING if supported by postgres version
            # This is a very advanced feature, handle with care
            # Not implemented yet, would require version check

        # If no items specified, return all columns
        if not items:
            return "RETURNING *"

        return f"RETURNING {', '.join(items)}"

    def _validate_column_name(self, column: str) -> str:
        """
        Validate and escape column name for postgres.

        postgres uses double quotes for identifiers.

        Args:
            column: Column name to validate

        Returns:
            str: Validated and properly quoted column name

        Raises:
            ValueError: If column name is invalid
        """
        # Remove any quotes first
        clean_name = column.strip('"')

        # Basic validation
        if not clean_name or clean_name.isspace():
            raise ValueError("Empty column name")

        # Check for common SQL injection patterns
        dangerous_patterns = [';', '--', 'union', 'select', 'drop', 'delete', 'update']
        lower_name = clean_name.lower()
        if any(pattern in lower_name for pattern in dangerous_patterns):
            raise ValueError(f"Invalid column name: {column}")

        # If name contains special chars, wrap in double quotes
        if ' ' in clean_name or '.' in clean_name or '"' in clean_name:
            return f'"{clean_name}"'

        return clean_name

    def supports_feature(self, feature: str) -> bool:
        """
        Check if a specific RETURNING feature is supported by postgres.

        postgres supports all RETURNING features including expressions,
        aliases, functions, and even CTEs in some versions.

        Args:
            feature: Feature name

        Returns:
            bool: True if feature is supported, False otherwise
        """
        # postgres supports almost everything in RETURNING
        supported_features = {
            "columns", "expressions", "aliases", "functions",
            "subqueries", "aggregates", "window_functions"
        }
        return feature in supported_features


class PostgresJsonHandler(JsonOperationHandler):
    """postgres-specific implementation of JSON operations."""

    def __init__(self, version: tuple):
        """Initialize handler with postgres version info.

        Args:
            version: postgres version as (major, minor, patch) tuple
        """
        self._version = version

        # Cache capability detection results
        self._json_supported = None
        self._jsonb_supported = None
        self._function_support = {}

    @property
    def supports_json_operations(self) -> bool:
        """Check if postgres version supports JSON operations.

        postgres supports JSON from version 9.2
        postgres supports JSONB from version 9.4 (preferred)

        Returns:
            bool: True if JSON operations are supported
        """
        if self._json_supported is None:
            self._json_supported = _is_version_at_least(self._version, PG_9_2)
        return self._json_supported

    @property
    def supports_jsonb(self) -> bool:
        """Check if postgres version supports JSONB type.

        postgres supports JSONB from version 9.4

        Returns:
            bool: True if JSONB type is supported
        """
        if self._jsonb_supported is None:
            self._jsonb_supported = _is_version_at_least(self._version, PG_9_4)
        return self._jsonb_supported

    def format_json_operation(self,
                              column: Union[str, Any],
                              path: Optional[str] = None,
                              operation: str = "extract",
                              value: Any = None,
                              alias: Optional[str] = None) -> str:
        """Format JSON operation according to postgres syntax.

        This method converts abstract JSON operations into postgres-specific syntax,
        handling version differences and using alternatives for unsupported functions.

        Args:
            column: JSON column name or expression
            path: JSON path (e.g. '$.name')
            operation: Operation type (extract, text, contains, exists, etc.)
            value: Value for operations that need it (contains, insert, etc.)
            alias: Optional alias for the result

        Returns:
            str: Formatted postgres JSON operation

        Raises:
            JsonOperationNotSupportedError: If JSON operations not supported by postgres version
        """
        if not self.supports_json_operations:
            raise JsonOperationNotSupportedError(
                f"JSON operations are not supported in postgres {'.'.join(map(str, self._version))}"
            )

        # Convert JSONPath to postgres path format
        pg_path = self._convert_json_path(path) if path else ''

        # Handle column formatting
        col = str(column)

        # Format operation based on type
        if operation == "extract":
            expr = f"{col}{pg_path}"

        elif operation == "text":
            # For postgres, ->> extracts as text
            expr = f"{col}{pg_path.replace('->', '->>')}" if '->' in pg_path else f"{col}->>{pg_path}"

        elif operation == "contains":
            if isinstance(value, (dict, list)):
                # Use @> for JSON containment
                import json
                json_value = json.dumps(value)
                expr = f"{col} @> '{json_value}'::jsonb"
            elif isinstance(value, str):
                expr = f"{col}{pg_path} = '{value}'"
            else:
                expr = f"{col}{pg_path} = {value}"

        elif operation == "exists":
            # Check if path exists using ? operator for JSONB
            if self.supports_jsonb:
                text_path = pg_path.replace('->', '.').replace('->>', '.')
                if text_path.startswith('.'):
                    text_path = text_path[1:]
                expr = f"{col} ?? '{text_path}'"
            else:
                # Fallback for older versions
                expr = f"{col}{pg_path} IS NOT NULL"

        elif operation == "type":
            expr = f"jsonb_typeof({col}{pg_path})"

        elif operation == "remove":
            if isinstance(path, str):
                expr = f"{col} - '{path}'"
            else:
                paths = ", ".join([f"'{p}'" for p in path])
                expr = f"{col} - ARRAY[{paths}]"

        elif operation == "insert" or operation == "set":
            if isinstance(value, (dict, list)):
                import json
                json_value = json.dumps(value)
                expr = f"jsonb_set({col}, '{{{path}}}', '{json_value}'::jsonb)"
            elif isinstance(value, str):
                expr = f"jsonb_set({col}, '{{{path}}}', '\"{value}\"'::jsonb)"
            else:
                expr = f"jsonb_set({col}, '{{{path}}}', '{value}'::jsonb)"

        elif operation == "array_length":
            expr = f"jsonb_array_length({col}{pg_path})"

        elif operation == "keys":
            expr = f"jsonb_object_keys({col}{pg_path})"

        else:
            # Default to extract if operation not recognized
            expr = f"{col}{pg_path}"

        if alias:
            return f"{expr} as {alias}"
        return expr

    def _convert_json_path(self, json_path: str) -> str:
        """Convert JSONPath to postgres path format.

        Args:
            json_path: Standard JSONPath (e.g., '$.user.name')

        Returns:
            str: postgres JSON path (e.g., '->>user->>name')
        """
        if not json_path or json_path == '$':
            return ''

        # Remove leading '$' if present
        if json_path.startswith('$'):
            json_path = json_path[1:]

        # Remove leading dot if present
        if json_path.startswith('.'):
            json_path = json_path[1:]

        # Handle array access like $.users[0].name
        path_parts = re.findall(r'\.?([^\.\[\]]+)|\[(\d+)\]', json_path)

        result = ''
        for part in path_parts:
            # Handle normal key
            if part[0]:
                result += f'->>{part[0]}'
            # Handle array index
            elif part[1]:
                result += f'->{part[1]}'

        return result

    def supports_json_function(self, function_name: str) -> bool:
        """Check if specific JSON function is supported in this postgres version.

        Args:
            function_name: Name of JSON function to check (e.g., "jsonb_extract_path")

        Returns:
            bool: True if function is supported
        """
        # Cache results for performance
        if function_name in self._function_support:
            return self._function_support[function_name]

        # All functions require JSON support
        if not self.supports_json_operations:
            self._function_support[function_name] = False
            return False

        # Define version requirements for each function
        function_versions = {
            # Core JSON functions available since 9.2
            "json_extract_path": PG_9_2,
            "json_extract_path_text": PG_9_2,
            "json_array_elements": PG_9_2,
            "json_array_length": PG_9_2,
            "json_object_keys": PG_9_2,
            "json_typeof": PG_9_2,

            # JSONB functions available since 9.4
            "jsonb_extract_path": PG_9_4,
            "jsonb_extract_path_text": PG_9_4,
            "jsonb_array_elements": PG_9_4,
            "jsonb_array_length": PG_9_4,
            "jsonb_object_keys": PG_9_4,
            "jsonb_typeof": PG_9_4,
            "jsonb_set": PG_9_4,
            "jsonb_insert": PG_9_5,
            "jsonb_pretty": PG_9_5,

            # JSON/JSONB comparison operators
            "@>": PG_9_4,  # containment
            "<@": PG_9_4,  # contained by
            "?": PG_9_4,  # contains key/element
            "?|": PG_9_4,  # contains any key
            "?&": PG_9_4,  # contains all keys

            # JSON Path functions (available since postgres 12)
            "jsonb_path_query": PG_12_0,
            "jsonb_path_query_array": PG_12_0,
            "jsonb_path_exists": PG_12_0,
            "jsonb_path_match": PG_12_0
        }

        # Check if function is supported based on version
        required_version = function_versions.get(function_name.lower())
        if required_version:
            is_supported = _is_version_at_least(self._version, required_version)
        else:
            # Unknown function, assume not supported
            is_supported = False

        # Cache result
        self._function_support[function_name] = is_supported
        return is_supported


class PostgresAggregateHandler(AggregateHandler):
    """postgres-specific aggregate functionality handler."""

    def __init__(self, version: tuple):
        """Initialize with postgres version.

        Args:
            version: postgres version tuple (major, minor, patch)
        """
        super().__init__(version)
        self._window_support_cache = None
        self._json_support_cache = None
        self._advanced_grouping_cache = None

    @property
    def supports_window_functions(self) -> bool:
        """Check if postgres supports window functions.

        postgres has supported window functions since version 8.4 (2009),
        so they are available in all modern versions.
        """
        if self._window_support_cache is None:
            self._window_support_cache = True  # All modern postgres versions support window functions
        return self._window_support_cache

    @property
    def supports_json_operations(self) -> bool:
        """Check if postgres supports JSON operations.

        postgres has supported JSON since version 9.2 and JSONB since 9.4,
        so they are available in all modern versions.
        """
        if self._json_support_cache is None:
            self._json_support_cache = _is_version_at_least(self._version, PG_9_2)
        return self._json_support_cache

    @property
    def supports_advanced_grouping(self) -> bool:
        """Check if postgres supports advanced grouping.

        postgres has supported CUBE, ROLLUP and GROUPING SETS since version 9.5.
        """
        if self._advanced_grouping_cache is None:
            self._advanced_grouping_cache = _is_version_at_least(self._version, PG_9_5)
        return self._advanced_grouping_cache

    def format_window_function(self,
                               expr: str,
                               partition_by: Optional[List[str]] = None,
                               order_by: Optional[List[str]] = None,
                               frame_type: Optional[str] = None,
                               frame_start: Optional[str] = None,
                               frame_end: Optional[str] = None,
                               exclude_option: Optional[str] = None) -> str:
        """Format window function SQL for postgres.

        postgres has the most comprehensive window function support.

        Args:
            expr: Base expression for window function
            partition_by: PARTITION BY columns
            order_by: ORDER BY columns
            frame_type: Window frame type (ROWS/RANGE/GROUPS)
            frame_start: Frame start specification
            frame_end: Frame end specification
            exclude_option: Frame exclusion option

        Returns:
            str: Formatted window function SQL

        Raises:
            WindowFunctionNotSupportedError: If using features not supported in the current version
        """
        window_parts = []

        if partition_by:
            window_parts.append(f"PARTITION BY {', '.join(partition_by)}")

        if order_by:
            window_parts.append(f"ORDER BY {', '.join(order_by)}")

        # Build frame clause
        frame_clause = []
        if frame_type:
            # Check for GROUPS support in older versions
            if frame_type == "GROUPS" and not _is_version_at_least(self._version, PG_11_0):
                raise WindowFunctionNotSupportedError(
                    f"GROUPS frame type requires postgres 11.0 or higher. "
                    f"Current version: {'.'.join(map(str, self._version))}"
                )

            frame_clause.append(frame_type)

            if frame_start:
                if frame_end:
                    frame_clause.append(f"BETWEEN {frame_start} AND {frame_end}")
                else:
                    frame_clause.append(frame_start)

        if frame_clause:
            window_parts.append(" ".join(frame_clause))

        if exclude_option:
            # Check postgres version for EXCLUDE option support
            if not _is_version_at_least(self._version, PG_11_0):
                raise WindowFunctionNotSupportedError(
                    f"EXCLUDE options require postgres 11.0 or higher. "
                    f"Current version: {'.'.join(map(str, self._version))}"
                )
            window_parts.append(exclude_option)

        window_clause = " ".join(window_parts)
        return f"{expr} OVER ({window_clause})"

    def format_json_operation(self,
                              column: str,
                              path: str,
                              operation: str = "extract",
                              value: Any = None) -> str:
        """Format JSON operation SQL for postgres.

        Args:
            column: JSON column name
            path: JSON path string
            operation: Operation type (extract, contains, exists)
            value: Value for contains operation

        Returns:
            str: Formatted JSON operation SQL
        """
        # Ensure minimum version for JSON support
        if not _is_version_at_least(self._version, PG_9_2):
            raise JsonOperationNotSupportedError(
                f"JSON operations not supported in postgres {'.'.join(map(str, self._version))}"
            )

        # Convert JSONPath to postgres path format
        pg_path = self._convert_json_path(path)

        if operation == "extract":
            # Use ->> for text output, -> for JSON output
            # Typically ->> is more useful for direct comparison
            return f"{column}{pg_path}"
        elif operation == "contains":
            if value is None:
                raise ValueError("Value is required for 'contains' operation")

            # Require JSONB for containment operations
            if not _is_version_at_least(self._version, PG_9_4):
                raise JsonOperationNotSupportedError("JSON containment requires postgres 9.4+ with JSONB support")

            # For JSON object containment
            if isinstance(value, dict):
                import json
                json_value = json.dumps(value)
                return f"{column} @> '{json_value}'::jsonb"
            # For checking if array contains value
            elif isinstance(value, list):
                import json
                json_value = json.dumps(value)
                return f"{column} @> '{json_value}'::jsonb"
            # For checking if key exists with specific value
            else:
                return f"{column}{pg_path} = '{value}'"
        elif operation == "exists":
            # Require JSONB for ? operator
            if not _is_version_at_least(self._version, PG_9_4):
                # Fallback for older versions
                return f"{column}{pg_path} IS NOT NULL"
            # Check if path exists using ? operator for JSONB
            if _is_version_at_least(self._version, PG_9_4):
                text_path = pg_path.replace('->', '.').replace('->>', '.')
                if text_path.startswith('.'):
                    text_path = text_path[1:]
                return f"{column} ?? '{text_path}'"
            # Fallback for older versions
            return f"{column}{pg_path} IS NOT NULL"
        else:
            raise ValueError(f"Unsupported JSON operation: {operation}")

    def _convert_json_path(self, json_path: str) -> str:
        """Convert JSONPath to postgres path format.

        Args:
            json_path: Standard JSONPath (e.g., '$.user.name')

        Returns:
            str: postgres JSON path (e.g., '->>user->>name')
        """
        if not json_path or json_path == '$':
            return ''

        # Remove leading '$' if present
        if json_path.startswith('$'):
            json_path = json_path[1:]

        # Remove leading dot if present
        if json_path.startswith('.'):
            json_path = json_path[1:]

        # Handle array access like $.users[0].name
        path_parts = re.findall(r'\.?([^\.\[\]]+)|\[(\d+)\]', json_path)

        result = ''
        for part in path_parts:
            # Handle normal key
            if part[0]:
                result += f'->>{part[0]}'
            # Handle array index
            elif part[1]:
                result += f'->{part[1]}'

        return result

    def format_grouping_sets(self,
                             type_name: str,
                             columns: List[Union[str, List[str]]]) -> str:
        """Format grouping sets SQL for postgres.

        postgres has comprehensive support for CUBE, ROLLUP and GROUPING SETS.

        Args:
            type_name: Grouping type (CUBE, ROLLUP, GROUPING SETS)
            columns: Columns to group by

        Returns:
            str: Formatted grouping sets SQL

        Raises:
            GroupingSetNotSupportedError: If advanced grouping not supported in this postgres version
        """
        if not self.supports_advanced_grouping:
            raise GroupingSetNotSupportedError(
                f"Advanced grouping ({type_name}) requires postgres 9.5 or higher. "
                f"Current version: {'.'.join(map(str, self._version))}"
            )

        if type_name in ("CUBE", "ROLLUP"):
            # postgres uses standard syntax for CUBE and ROLLUP
            if isinstance(columns[0], list):
                # Not typical for CUBE/ROLLUP, but handle just in case
                flat_columns = []
                for col_group in columns:
                    if isinstance(col_group, list):
                        flat_columns.extend(col_group)
                    else:
                        flat_columns.append(col_group)

                return f"{type_name}({', '.join(flat_columns)})"
            else:
                return f"{type_name}({', '.join(columns)})"
        elif type_name == "GROUPING SETS":
            # Handle GROUPING SETS with multiple sets
            if isinstance(columns[0], list):
                column_groups = []
                for group in columns:
                    if isinstance(group, list):
                        column_groups.append(f"({', '.join(group)})")
                    else:
                        column_groups.append(f"({group})")

                return f"GROUPING SETS({', '.join(column_groups)})"
            else:
                # Single grouping set
                return f"GROUPING SETS(({', '.join(columns)}))"
        else:
            raise GroupingSetNotSupportedError(f"Unknown grouping type: {type_name}")


class PostgresCTEHandler(CTEHandler):
    """postgres-specific CTE handler."""

    def __init__(self, version: tuple):
        self._version = version

    @property
    def is_supported(self) -> bool:
        # CTEs are supported in all modern postgres versions
        return _is_version_at_least(self._version, PG_8_4)

    @property
    def supports_recursive(self) -> bool:
        return self.is_supported

    @property
    def supports_materialized_hint(self) -> bool:
        # MATERIALIZED hint introduced in postgres 12
        return self._version[0] >= 12

    @property
    def supports_cte_in_dml(self) -> bool:
        return self.is_supported

    @property
    def supports_compound_recursive(self) -> bool:
        """Check if compound queries in recursive CTEs are supported.

        postgres has always supported compound queries in recursive CTEs
        in all modern versions.

        Returns:
            bool: Always True for postgres
        """
        return self.is_supported

    def format_cte(self,
                   name: str,
                   query: str,
                   columns: Optional[List[str]] = None,
                   recursive: bool = False,
                   materialized: Optional[bool] = None) -> str:
        """Format postgres CTE."""
        name = self.validate_cte_name(name)

        # Add column definitions if provided
        column_def = ""
        if columns:
            column_def = f"({', '.join(columns)})"

        # Add materialization hint if supported and specified
        materialized_hint = ""
        if materialized is not None and self.supports_materialized_hint:
            materialized_hint = "MATERIALIZED " if materialized else "NOT MATERIALIZED "

        return f"{name}{column_def} AS {materialized_hint}({query})"

    def format_with_clause(self,
                           ctes: List[Dict[str, Any]],
                           recursive: bool = False) -> str:
        """Format postgres WITH clause."""
        if not ctes:
            return ""

        recursive_keyword = "RECURSIVE " if recursive else ""

        formatted_ctes = []
        for cte in ctes:
            formatted_ctes.append(self.format_cte(
                name=cte['name'],
                query=cte['query'],
                columns=cte.get('columns'),
                recursive=recursive, # Use the overall recursive flag
                materialized=cte.get('materialized')
            ))

        return f"WITH {recursive_keyword}{', '.join(formatted_ctes)}"


class PostgresSQLBuilder(SQLBuilder):
    """postgres specific SQL Builder

    Extends the base SQLBuilder to handle psycopg3's %s placeholder syntax.
    This builder focuses on '%s' placeholders, directly inlining SQLExpressionBase
    objects into the SQL string and collecting the remaining parameters.
    """

    def __init__(self, dialect: SQLDialectBase):
        """Initialize postgres SQL builder

        Args:
            dialect: postgres dialect instance
        """
        super().__init__(dialect)

    def build(self, sql: str, params: Optional[Union[Tuple, List, Dict]] = None) -> Tuple[str, Tuple]:
        """Build SQL statement with parameters for postgres.

        This method processes SQL with '%s' placeholders. It directly inlines
        SQLExpressionBase objects into the SQL string and collects other
        parameters for the database driver.

        Args:
            sql: SQL statement with '%s' placeholders.
            params: Parameter values. Can be a tuple, list, or dict.

        Returns:
            Tuple[str, Tuple]: (Processed SQL, Processed parameters)

        Raises:
            ValueError: If parameter count doesn't match placeholder count.
        """
        if not params:
            return sql, ()

        # Ensure params is a tuple for consistent iteration
        if isinstance(params, list):
            params_tuple = tuple(params)
        elif isinstance(params, dict):
            # If a dict is passed, we treat its values as positional parameters.
            params_tuple = tuple(params.values())
        else:
            params_tuple = params # Already a tuple

        final_sql_parts = []
        regular_params = []
        param_idx_in_sql_placeholders = 0 # Tracks position of %s in SQL
        
        last_pos = 0
        for match in re.finditer(r'%s', sql):
            start, end = match.span()
            final_sql_parts.append(sql[last_pos:start]) # Add text before %s

            if param_idx_in_sql_placeholders < len(params_tuple):
                current_param = params_tuple[param_idx_in_sql_placeholders]
                if isinstance(current_param, SQLExpressionBase):
                    # If it's an expression, format it directly into the SQL
                    final_sql_parts.append(self.dialect.format_expression(current_param))
                else:
                    # If it's a regular parameter, keep %s in SQL and collect the parameter
                    final_sql_parts.append('%s')
                    regular_params.append(current_param)
            else:
                # Error: More %s placeholders in SQL than parameters provided
                raise ValueError(
                    f"Parameter count mismatch: SQL has more placeholders (%s) than parameters. "
                    f"Expected at least {param_idx_in_sql_placeholders + 1}, but only {len(params_tuple)} were provided."
                )
            param_idx_in_sql_placeholders += 1
            last_pos = end
        
        final_sql_parts.append(sql[last_pos:]) # Add remaining text after last %s

        # Final check: Ensure no extra parameters were provided
        if param_idx_in_sql_placeholders < len(params_tuple):
            raise ValueError(
                f"Parameter count mismatch: Too many parameters provided for SQL. "
                f"SQL expects {param_idx_in_sql_placeholders} parameters (expressions inlined), "
                f"but {len(params_tuple)} were provided in total."
            )

        return ''.join(final_sql_parts), tuple(regular_params)


class PostgresDialect(SQLDialectBase):
    """postgres dialect implementation"""

    def __init__(self, config: ConnectionConfig):
        """Initialize postgres dialect

        Args:
            config: Database connection configuration
        """
        # Get version from connection config or use default
        version = getattr(config, 'version', (9, 6, 0))
        if isinstance(version, str):
            # Parse version string (e.g. "14.5" into (14, 5, 0))
            parts = version.split('.')
            version = (
                int(parts[0]),
                int(parts[1]) if len(parts) > 1 else 0,
                int(parts[2]) if len(parts) > 2 else 0
            )

        super().__init__(version)

        if hasattr(config, 'driver_type') and config.driver_type:
            self._driver_type = config.driver_type
        else:
            self._driver_type = DriverType.PSYCOPG

        # Initialize handlers
        self._returning_handler = PostgresReturningHandler()
        self._aggregate_handler = PostgresAggregateHandler(version)
        self._json_operation_handler = PostgresJsonHandler(version)
        self._cte_handler = PostgresCTEHandler(version)

    def format_expression(self, expr: SQLExpressionBase) -> str:
        """Format postgres expression"""
        if not isinstance(expr, PostgresExpression):
            raise ValueError(f"Unsupported expression type: {type(expr)}")
        return expr.format(self)

    def get_placeholder(self) -> str:
        """Get postgres parameter placeholder"""
        return "%s"

    def format_string_literal(self, value: str) -> str:
        """Quote string literal

        postgres uses single quotes for string literals
        """
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    def format_identifier(self, identifier: str) -> str:
        """Quote identifier (table/column name)

        postgres uses double quotes for identifiers
        """
        if '"' in identifier:
            escaped = identifier.replace('"', '""')
            return f'"{escaped}"'
        return f'"{identifier}"'

    def format_limit_offset(self, limit: Optional[int] = None,
                            offset: Optional[int] = None) -> Tuple[Optional[str], List[Any]]:
        """Format LIMIT and OFFSET clause"""
        params = []
        sql_parts = []

        if limit is not None:
            sql_parts.append("LIMIT %s")
            params.append(limit)
        if offset is not None:
            sql_parts.append("OFFSET %s")
            params.append(offset)

        if not sql_parts:
            return None, []

        return " ".join(sql_parts), params

    def get_parameter_placeholder(self, position: int) -> str:
        """Get postgres parameter placeholder

        For psycopg (and psycopg2, pg8000), %s placeholders are used.
        This builder focuses exclusively on '%s' placeholders.

        Args:
            position: Parameter position (0-based) - not used for %s.

        Returns:
            str: Parameter placeholder for postgres (%s)
        """
        # Given this builder focuses exclusively on %s, always return %s.
        return "%s"

    def format_explain(self, sql: str, options: Optional[ExplainOptions] = None) -> str:
        """Format postgres EXPLAIN statement

        Args:
            sql: SQL to explain
            options: EXPLAIN options

        Returns:
            str: Formatted EXPLAIN statement
        """
        if not options:
            options = ExplainOptions()

        explain_options = []

        if options.type == ExplainType.ANALYZE:
            explain_options.append("ANALYZE")

        if options.buffers:
            explain_options.append("BUFFERS")

        if not options.costs:
            explain_options.append("COSTS OFF")

        if options.timing:
            explain_options.append("TIMING")

        if options.verbose:
            explain_options.append("VERBOSE")

        if options.settings:
            explain_options.append("SETTINGS")

        # WAL option requires postgres 13+
        if options.wal and _is_version_at_least(self._version, PG_13_0):
            explain_options.append("WAL")
        elif options.wal:
            major, minor, patch = self._version
            explain_options.append(
                f"/* Note: WAL option requires postgres 13+. Current version: {major}.{minor}.{patch} */")

        if options.format != ExplainFormat.TEXT:
            # Check format compatibility
            if options.format == ExplainFormat.XML and not _is_version_at_least(self._version, PG_9_1):
                major, minor, patch = self._version
                raise ValueError(f"XML format requires postgres 9.1+. Current version: {major}.{minor}.{patch}")

            if options.format == ExplainFormat.JSON and not _is_version_at_least(self._version, PG_9_4):
                major, minor, patch = self._version
                raise ValueError(f"JSON format requires postgres 9.4+. Current version: {major}.{minor}.{patch}")

            if options.format == ExplainFormat.YAML and not _is_version_at_least(self._version, PG_9_4):
                major, minor, patch = self._version
                raise ValueError(f"YAML format requires postgres 9.4+. Current version: {major}.{minor}.{patch}")

            explain_options.append(f"FORMAT {options.format.value}")

        if explain_options:
            return f"EXPLAIN ({', '.join(explain_options)}) {sql}"
        return f"EXPLAIN {sql}"

    @property
    def supported_formats(self) -> Set[ExplainFormat]:
        """Get supported EXPLAIN output formats for current postgres version"""
        # All versions support TEXT format
        formats = {ExplainFormat.TEXT}

        # XML format added in 9.1
        if _is_version_at_least(self._version, PG_9_1):
            formats.add(ExplainFormat.XML)

        # JSON and YAML formats added in 9.4
        if _is_version_at_least(self._version, PG_9_4):
            formats.add(ExplainFormat.JSON)
            formats.add(ExplainFormat.YAML)

        return formats

    def create_expression(self, expression: str) -> PostgresExpression:
        """Create postgres expression"""
        return PostgresExpression(expression)

    def update_version(self, version: tuple) -> None:
        """Update dialect version information dynamically.

        This method should be called when the actual database server version
        is determined after connection is established.

        Args:
            version: Database server version tuple (major, minor, patch)
        """
        self._version = version

        # Also update version information in dialect's handlers
        if hasattr(self, '_returning_handler'):
            # postgres's returning handler doesn't use version info,
            # but include for consistency
            pass

        if hasattr(self, '_aggregate_handler'):
            self._aggregate_handler._version = version
            # Reset caches
            self._aggregate_handler._window_support_cache = None
            self._aggregate_handler._json_support_cache = None
            self._aggregate_handler._advanced_grouping_cache = None

        if hasattr(self, '_json_operation_handler'):
            self._json_operation_handler._version = version
            # Reset caches
            self._json_operation_handler._json_supported = None
            self._json_operation_handler._jsonb_supported = None
            self._json_operation_handler._function_support = {}