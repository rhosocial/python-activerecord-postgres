# src/rhosocial/activerecord/backend/impl/postgres/protocols/types/jsonb_enhanced.py
"""PostgreSQL JSONB enhanced support protocol definition.

This module defines the protocol for PostgreSQL-specific JSON/JSONB
enhancements beyond the core JSON support.
"""

from typing import Protocol, runtime_checkable

from rhosocial.activerecord.backend.dialect.protocols import JSONSupport


@runtime_checkable
class PostgresJSONBEnhancedSupport(JSONSupport, Protocol):
    """PostgreSQL JSONB enhanced support protocol.

    PostgreSQL provides the JSONB data type with indexing support,
    JSON path expressions, and various query functions that go
    beyond basic JSON operations.

    This protocol declares what PostgreSQL *adds* to the core JSON contract, so
    it derives from :class:`~rhosocial.activerecord.backend.dialect.protocols.JSONSupport`
    and repeats none of its members. That is not a style preference. Both classes
    end up in ``PostgresDialect``'s base list, so a member restated here was
    declared twice in one MRO, and ``supports_json_type`` -- a probe, declared
    here as an abstract ``...`` and answered by the mixin -- resolved from four
    classes: this protocol, the core protocol, the core mixin default and the
    implementation. One implementation is not a defect; a second *declaration*
    of the same probe is what put the count over the line, and derivation is
    what removes it without removing a contract.

    JSONB enhanced features:
    - JSONB type with indexing (PG 9.4+)
    - JSONB extraction functions jsonb_extract_path (PG 9.4+),
      json_extract_path (PG 9.3+), json_extract_path_text (PG 9.5+)
    - SQL/JSON path language: jsonb_path_query, jsonb_path_query_first,
      jsonb_path_query_array, jsonb_path_exists, jsonb_path_match, and the @?
      and @@ operators (PG 12.0+)
    - Numeric infinity in JSONB (PG 17+)

    Two features PostgreSQL does *not* have, so the mixin answers False and
    refuses: ``JSON_TABLE``, whose declaration is the core one and whose gate
    this file used to put at 12+, and the ``jsonb['key']`` subscript, declared
    below and used to be gated at 14+. Both dated capabilities the server does
    not have; ``jsonb_to_recordset`` in a FROM clause is PostgreSQL's nearest
    relative to JSON_TABLE and is not a drop-in.

    Feature Source: Native support (no extension required)

    Official Documentation:
    https://www.postgresql.org/docs/current/datatype-json.html
    https://www.postgresql.org/docs/current/functions-json.html

    Version Requirements:
    - JSON type: PostgreSQL 9.2+ (core JSONSupport)
    - JSON path read (->, #> operators): PostgreSQL 9.3+
    - JSONB type: PostgreSQL 9.4+
    - JSON path language: PostgreSQL 12.0+
    - Numeric infinity in JSONB: PostgreSQL 17+
    """

    def supports_json_function(self, function_name: str) -> bool:
        """Whether a named JSON path function is available on this server.

        Declared because a conformance check compares the rendered SQL against
        the functions this dialect claims, and PostgreSQL spells them
        jsonb_path_query_* rather than the core's MySQL-shaped JSON_EXTRACT. The
        answer is gated per function, because they did not arrive together.
        """
        ...

    def supports_jsonb(self) -> bool:
        """Whether JSONB data type is supported.

        Native feature, PostgreSQL 9.4+.
        The JSONB type stores binary-format JSON data with
        indexing support for efficient querying.
        """
        ...

    def supports_json_path(self) -> bool:
        """Whether a JSON path can be read at all.

        Native feature, PostgreSQL 9.3+, which added ``->``, ``->>``, ``#>`` and
        ``#>>`` for ``json``. The SQL/JSON path language is 12.0 and is a
        separate capability, :meth:`supports_sql_json_path`; a server can have
        one without the other, and PostgreSQL has had both since 12.0.
        """
        ...

    def supports_sql_json_path(self) -> bool:
        """Whether the SQL/JSON path language is available.

        Native feature, PostgreSQL 12.0+.
        Includes jsonb_path_query, jsonb_path_query_first,
        jsonb_path_exists, and jsonb_path_match functions, plus the @? and @@
        operators. Only these can evaluate a filter or a wildcard, so a path
        that needs one is refused on an older server rather than approximated.
        """
        ...

    def supports_jsonb_subscript(self) -> bool:
        """Whether JSONB subscript notation is supported.

        Always False: PostgreSQL has no ``jsonb['key']`` subscript. SQL
        subscripting covers arrays and composites, and jsonb values are read
        with ``->`` / ``->>``, the ``#>`` / ``#>>`` path operators, or the
        SQL/JSON path language.
        """
        ...

    def supports_infinity_numeric_infinity_jsonb(self) -> bool:
        """Whether numeric infinity values are allowed in JSONB.

        Native feature, PostgreSQL 17+.
        Allows Infinity and -Infinity in numeric context within JSONB.
        """
        ...
