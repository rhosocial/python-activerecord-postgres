# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/jsonb_enhanced.py
"""
PostgreSQL JSONB enhanced mixin.

Implements the PostgresJSONBEnhancedSupport protocol.
"""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression import JSONExpression


class PostgresJSONBEnhancedMixin:
    """Mixin for PostgreSQL enhanced JSONB support.

    Provides version-aware capability detection for PostgreSQL
    JSON and JSONB features beyond the standard JSONSupport protocol.

    PostgreSQL can navigate a document two ways, and which one applies is
    decided by the expression's :class:`JSONPathMode` rather than by
    preference:

    - ``->`` / ``->>`` arrows, available since 9.4 for ``jsonb``. Cheap, and
      the idiomatic form.
    - ``jsonb_path_query_first``, the SQL/JSON path language, since 12.0. More
      expressive, and the only form that can evaluate a real path predicate.
    """

    #: The JSON path functions PostgreSQL spells this way. Declared so a
    #: conformance check can tell them from a function inherited from the core,
    #: which is MySQL's JSON_EXTRACT. Both operators go through
    #: jsonb_path_query_first; `->>` then casts the result to text.
    _JSON_FUNCTION_NAMES = ("jsonb_path_query_first", "jsonb_path_query_array", "#>>")

    def supports_json_function(self, function_name: str) -> bool:
        """Whether a named JSON function is available on this server."""
        return function_name.lower() in {n.lower() for n in self._JSON_FUNCTION_NAMES}

    def format_json_expression(self, expr: "JSONExpression") -> Tuple[str, tuple]:
        """Render a JSON path access according to the expression's mode.

        Overriding the core dispatch entry point previously ignored
        ``expr.mode`` entirely, so ``ARROW`` silently produced jsonpath SQL
        and ``FUNCTION`` was indistinguishable from ``AUTO`` — even though
        ``supports_json_arrow_operators()`` advertised arrows that this method
        never emitted.

        ``AUTO`` keeps resolving to the path form rather than to arrows, and
        that is deliberate. The two spellings do not take the same path
        argument: ``->`` and ``->>`` take a *key name*, while
        ``jsonb_path_query_first`` takes a *jsonpath* such as
        ``$.tags[0]``. ``expression.functions.json_extract_text`` passes a
        jsonpath, so preferring arrows here would look up a key literally named
        ``$.tags[0]``, find nothing, and yield NULL. Preferring arrows is only
        safe when the path is a single key, which the caller cannot be assumed
        to know.

        Args:
            expr: The JSONExpression node.

        Returns:
            Tuple of (SQL string, parameters tuple).

        Raises:
            UnsupportedFeatureError: If ARROW mode is requested before
                PostgreSQL 9.4, or FUNCTION mode before 12.0.
        """
        from rhosocial.activerecord.backend.expression.advanced_functions import JSONPathMode

        mode: JSONPathMode = getattr(expr, "mode", JSONPathMode.AUTO)

        if mode is JSONPathMode.ARROW:
            return self.format_json_arrow_expression(expr)

        # AUTO and FUNCTION both render the path form: the path argument is a
        # jsonpath, which the arrow operators cannot consume.
        return self._format_json_path_expression(expr)

    def _format_json_path_expression(self, expr: "JSONExpression") -> Tuple[str, tuple]:
        """Render via ``jsonb_path_query_first``, the SQL/JSON path form.

        The path is bound as a ``jsonpath`` parameter rather than inlined, so
        a document-driven path cannot alter the statement's shape.
        """
        from rhosocial.activerecord.backend.expression import bases

        if not self.supports_json_path():
            raise UnsupportedFeatureError(
                self.name,
                "SQL/JSON path expressions (jsonb_path_query_first)",
                "This PostgreSQL is older than 12.0, which introduced the "
                "SQL/JSON path language. Use JSONPathMode.ARROW, or the -> "
                "and ->> operators, instead.",
            )

        if isinstance(expr.column, bases.BaseExpression):
            col_sql, col_params = expr.column.to_sql()
        else:
            col_sql, col_params = self.format_identifier(str(expr.column)), ()

        placeholder = self.get_parameter_placeholder()
        value_sql = f"jsonb_path_query_first({col_sql}, {placeholder}::jsonpath)"
        if expr.operation == "->>":
            sql = f"({value_sql} #>> '{{}}')"
        else:
            sql = value_sql
        params = col_params + (expr.path,)

        if expr.alias:
            sql = f"{sql} AS {self.format_identifier(expr.alias)}"

        return sql, params

    def supports_json_type(self) -> bool:
        """JSON is supported since PostgreSQL 9.2."""
        return self.version >= (9, 2, 0)

    def supports_jsonb(self) -> bool:
        """Check if PostgreSQL version supports JSONB type (introduced in 9.4)."""
        return self.version >= (9, 4, 0)

    def supports_json_path(self) -> bool:
        """Whether SQL/JSON path expressions are supported (PostgreSQL 12+)."""
        return self.version >= (12, 0, 0)

    def supports_json_table(self) -> bool:
        """PostgreSQL has no ``JSON_TABLE`` function, so this is always False.

        The probe previously claimed 12.0+ and the core formatter then emitted
        ``JSON_TABLE(col, 'path' COLUMNS(...))``, which the server rejects.
        PostgreSQL's closest relative is ``jsonb_to_recordset()``, and it is
        not a drop-in: it takes no path, no per-column path, and returns a
        set of rows rather than a table expression. See
        :meth:`format_json_table_expression` for the guided error.
        """
        return False

    def format_json_table_expression(self, expr) -> Tuple[str, tuple]:
        """Always refuse, naming the construct that actually works here.

        Args:
            expr: The JSONTableExpression node.

        Raises:
            UnsupportedFeatureError: Always.
        """
        raise UnsupportedFeatureError(
            self.name,
            "JSON_TABLE function",
            "PostgreSQL has no JSON_TABLE. Use jsonb_to_recordset() in a "
            "FROM clause instead: it takes the document and a column "
            "definition list, but it has no path argument and no per-column "
            "PATH, so a nested JSON_TABLE cannot be translated one-for-one.",
        )

    def supports_jsonb_subscript(self) -> bool:
        """PostgreSQL has no ``jsonb['key']`` subscript, so this is always False.

        This probe was previously defined three times across the mixin
        hierarchy with two different version gates (11.0 and 14.0), and the
        MRO silently picked a winner. No formatter anywhere emitted a
        subscript — the arrow operators are the only spelling this backend
        uses — so a True here advertised syntax PostgreSQL cannot parse.
        """
        return False

    def supports_infinity_numeric_infinity_jsonb(self) -> bool:
        """Whether numeric infinity values are allowed in JSONB (PostgreSQL 17+)."""
        return self.version >= (17, 0, 0)
