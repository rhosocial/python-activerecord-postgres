# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/jsonb_enhanced.py
"""
PostgreSQL JSONB enhanced mixin.

Implements the PostgresJSONBEnhancedSupport protocol.
"""

import re
from typing import Tuple, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression import (
        JSONDocumentExpression,
        JSONTextExpression,
    )

    #: What both formatters below accept. Core used to have one class for both
    #: JSON access operators; it now has two, one per operator, and this dialect
    #: renders either from the same code path -- so the parameter is a union.
    #:
    #: The union is spelled out even though ``JSONTextExpression`` subclasses
    #: ``JSONDocumentExpression`` and a checker would collapse it. That
    #: inheritance exists so ``->>`` keeps the accessors that let a path chain
    #: continue; it is not a claim that ``->>`` yields a document. Annotating
    #: only the document class would type a text node as a document, which is
    #: the confusion the split was made to remove.
    JSONPathNode = Union["JSONDocumentExpression", "JSONTextExpression"]


# ---------------------------------------------------------------------------
# A jsonpath, translated to the key array ``#>`` takes
# ---------------------------------------------------------------------------

#: One accessor of a jsonpath, in the only two shapes a key array can hold: an
#: object key and an array subscript. Everything else in the language -- filters,
#: wildcards, arithmetic, ``last`` -- has no spelling here, and
#: :func:`_jsonpath_key_array` refuses rather than approximate it.
#:
#: The three alternatives are exactly PostgreSQL's lax-mode accessor syntax:
#: ``.key`` for an unquoted key, ``."key"`` for anything else (a space, a dash,
#: a leading digit), and ``[0]`` for a subscript. ``$.tags[0]`` therefore
#: translates to ``("tags", "0")``, which is what ``#> '{tags,0}'`` walks.
_JSONPATH_ACCESSOR = re.compile(
    r"""
      \.\s*(?P<key>[A-Za-z_][A-Za-z0-9_]*)      # .key
    | \.\s*"(?P<quoted>(?:[^"\\]|\\.)*)"        # ."a key with spaces"
    | \[\s*(?P<index>[0-9]+)\s*\]              # [0]
    """,
    re.VERBOSE,
)

#: Constructs a jsonpath can contain that a key array cannot express, each with
#: the clause to use when refusing it. Looked for in this order because some of
#: them contain others, and because in lax mode ``$.a-b`` is a *subtraction*,
#: not a key named ``a-b`` -- so the arithmetic markers have to be recognised
#: before anything treats the text as a name.
_UNSUPPORTED_JSONPATH_CONSTRUCTS = (
    ("..", "uses a recursive descent (..)"),
    ("*", "uses a wildcard accessor ([*] or .*)"),
    ("?", "uses a filter expression (?(...))"),
    ("$", "uses a jsonpath variable"),
    ("last", "uses the [last] subscript"),
    ("+", "uses the addition operator"),
    ("-", "uses the subtraction operator"),
    ("/", "uses the division operator"),
    ("%", "uses the modulus operator"),
    ("||", "uses the concatenation operator"),
    ("<", "compares two values with <"),
    (">", "compares two values with >"),
    ("=", "compares two values with =="),
    ("!", "compares two values"),
)


def _describe_untranslatable_path(fragment: str) -> str:
    """Name the construct in ``fragment`` that a key array cannot hold.

    Returns a clause that reads after "because it ...", so the caller can put it
    straight into an ``UnsupportedFeatureError`` without re-parsing the path.
    """
    for needle, clause in _UNSUPPORTED_JSONPATH_CONSTRUCTS:
        if needle in fragment:
            return clause
    return f"is not a path the -> and #> operators can express: {fragment.strip()!r}"


def _jsonpath_accessors(text: str) -> Tuple[Tuple[str, str], ...]:
    """Walk an accessor chain into ``(kind, text)`` pairs.

    ``kind`` is ``"index"`` (``[0]``), ``"key"`` (``.a``) or ``"quoted"``
    (``."a b"``), one per alternative of :data:`_JSONPATH_ACCESSOR`; ``text``
    is the match as written, so a quoted key keeps its backslash escapes.

    This is the one walk both spellings of a path are built from.
    :func:`_jsonpath_key_array` takes the values, and
    :func:`_canonical_jsonpath` needs the kind as well, because ``[0]`` and
    ``."0"`` collapse to the same key array element but are different
    jsonpaths.

    Raises:
        ValueError: If any accessor is something a key array cannot hold. The
            message names the construct, ready for an
            ``UnsupportedFeatureError`` without re-parsing.
    """
    accessors = []
    position = 0
    while position < len(text):
        accessor = _JSONPATH_ACCESSOR.match(text, position)
        if accessor is None:
            raise ValueError(_describe_untranslatable_path(text[position:]))
        position = accessor.end()
        if accessor.group("index") is not None:
            accessors.append(("index", accessor.group("index")))
        elif accessor.group("key") is not None:
            accessors.append(("key", accessor.group("key")))
        else:
            accessors.append(("quoted", accessor.group("quoted")))
    return tuple(accessors)


def _jsonpath_key_array(path: str) -> Tuple[str, ...]:
    """Translate a jsonpath accessor chain into the keys ``#>`` takes.

    PostgreSQL before 12.0 has no jsonpath. Its path operators take a *key
    array* instead, so a path written in the language has to be taken apart
    before it can be rendered -- and taking it apart is only sound for the
    subset that walks keys and subscripts. Anything else is refused rather than
    approximated, because every approximation here returns a different value
    instead of an error.

    Two things about the accepted subset are worth stating outright, because
    they are the two places where this spelling and ``jsonb_path_query_first``
    can disagree, both checked against a live 12.22 server:

    * A path with no ``$`` is read the way ``->`` reads a path -- as a key, or
      as a subscript -- and only when it is exactly one accessor. The path
      language reads a rootless form as something else: ``'a'`` is a variable
      reference, ``'a.b'`` and ``'[0]'`` are rejected, and ``'"a b"'`` is the
      *string literal* ``a b``. Reading a single rootless accessor as a key is
      deliberate, because that is what :meth:`format_json_arrow_expression`
      reads, and the jsonpath spelling roots the same accessor rather than
      passing it through (see :func:`_canonical_jsonpath`), so the same
      argument means the same thing in ARROW mode, in this spelling, and in
      the path language. A longer rootless chain is refused rather than
      invented: the two spellings do not agree on what it means.
    * ``#>`` is strict where the path language is lax. A path whose shape does
      not match the document -- a member of a scalar, a subscript into an
      object -- returns NULL here and the container itself under
      ``jsonb_path_query_first``, because lax mode returns the item it was
      standing on rather than failing. For a path that matches the shape, which
      is every path written on purpose, the two agree exactly.

    Args:
        path: A jsonpath such as ``$.a``, ``$.tags[0]``, ``$."a key"`` or ``a``.

    Returns:
        The keys, outermost first. Empty for the root ``$``, which
        ``#> '{}'`` reads as the whole document.

    Raises:
        ValueError: If ``path`` uses anything a key array cannot express. The
            message is a clause naming the construct, so the caller can quote it
            in an ``UnsupportedFeatureError`` without parsing the path again.
    """
    text = path.strip()
    if not text:
        raise ValueError("is empty")

    rootless = not text.startswith("$")
    if rootless:
        # A leading dot is added so a rootless accessor matches the same pattern
        # as one written after a root.
        if text[0] not in '.[':
            text = "." + text
    else:
        text = text[1:].lstrip()
        if text and text[0] not in '.[':
            # ``$name`` is a variable reference, not the member ``name``: lax
            # mode always spells a member ``$.name``. Turning it into a key
            # would look up a key called ``name`` and answer with whatever that
            # key holds, which is not what the jsonpath asked for.
            raise ValueError("uses a jsonpath variable")

    accessors = _jsonpath_accessors(text)
    keys = tuple(
        raw.replace('\\"', '"').replace("\\\\", "\\")
        if kind == "quoted"
        else raw
        for kind, raw in accessors
    )

    if rootless and len(keys) != 1:
        raise ValueError(
            "omits the root $, which the path language allows only for a single "
            "accessor"
        )

    return tuple(keys)


def _canonical_jsonpath(path: str) -> str:
    """Root a single rootless accessor, for the jsonpath spelling.

    ``jsonb_path_query_first`` takes a jsonpath, and the path language reads a
    rootless form as something other than a key or a subscript: ``'a'`` is a
    variable reference, ``'[0]'`` and ``'a.b'`` are rejected, and ``'"a b"'``
    is the *string literal* ``a b``. The arrow spelling this backend renders
    reads a single rootless accessor as a key or subscript, so an argument
    written that way has to arrive at the server rooted: ``a`` becomes
    ``$.a``, ``[0]`` becomes ``$[0]`` and ``"a b"`` becomes ``$."a b"``.

    Only one accessor is rooted. A longer rootless chain is refused rather
    than completed, because the two spellings do not agree on what it means:
    the key array reads it as keys, the path language rejects it. What counts
    as a single accessor is :func:`_jsonpath_key_array`'s question -- it is
    the authority on which rootless forms have a shared meaning -- and this
    asks it through the same :func:`_jsonpath_accessors` walk.

    A rooted path is returned unchanged: it is already what the server
    expects, filters and wildcards included.

    Args:
        path: A jsonpath such as ``$.a``, ``$.tags[0]``, ``$."a key"`` or a
            single rootless accessor such as ``a``, ``[0]`` or ``"a b"``.

    Returns:
        The rooted jsonpath, or ``path`` unchanged when it already starts
        with ``$``.

    Raises:
        ValueError: If ``path`` is empty, or omits the root without being a
            single accessor. The message is a clause naming the reason, ready
            to quote in an ``UnsupportedFeatureError``.
    """
    text = path.strip()
    if not text:
        raise ValueError("is empty")

    if text.startswith("$"):
        return text

    # A leading dot is added so a rootless accessor matches the same pattern
    # as one written after a root, exactly as _jsonpath_key_array does it.
    walkable = text if text[0] in '.[' else "." + text
    try:
        accessors = _jsonpath_accessors(walkable)
    except ValueError as exc:
        raise ValueError(
            f"omits the root $, which the arrow spelling and the path language "
            f"read the same way only for a single accessor ({exc})"
        ) from None

    if len(accessors) != 1:
        raise ValueError(
            "omits the root $, which the arrow spelling and the path language "
            "read the same way only for a single accessor"
        )

    kind, literal = accessors[0]
    if kind == "index":
        return f"$[{literal}]"
    if kind == "key":
        return f"$.{literal}"
    # Quoted, kept as written so its backslash escapes stay escapes.
    return f'$."{literal}"'


class PostgresJSONBEnhancedMixin:
    """Mixin for PostgreSQL enhanced JSONB support.

    Provides version-aware capability detection for PostgreSQL
    JSON and JSONB features beyond the standard JSONSupport protocol.

    PostgreSQL can navigate a document two ways, and which one applies is
    decided by the expression's :class:`JSONPathMode` rather than by
    preference:

    - ``->`` / ``->>`` arrows, one key and no subscript, since 9.3 for ``json``
      and 9.4 for ``jsonb``. Cheap, and the idiomatic form.
    - a *path*, in two spellings that arrived nine years apart. ``#>`` and
      ``#>>`` take a key array and came with the arrows in 9.3 (9.4 for
      ``jsonb``); ``jsonb_path_query_first`` and the rest of the SQL/JSON path
      language take a jsonpath and came in 12.0.

    For a path that only walks keys and subscripts the two spellings return the
    same value, so AUTO and FUNCTION render whichever one the server has --
    :meth:`supports_sql_json_path` says which -- and the mode still decides
    between ARROW and the path form. The language is preferred where it exists,
    because it is the only one that can evaluate a predicate. A path that
    genuinely needs it -- a filter, a wildcard, arithmetic -- is refused on an
    older server, and refusing is the point: the alternative is a translation
    that returns the wrong answer instead of an error.
    """

    #: The JSON functions this backend declares, each with the version that
    #: introduced it, so :meth:`supports_json_function` never claims a function
    #: the server does not have. Before 12.0 the ``jsonb_path_query_*`` family
    #: does not exist at all -- that is the entire reason for the second
    #: spelling of a path below -- and the probe answered from a flat set with
    #: no version in it, so it claimed all three on a 9.6 server.
    #:
    #: ``#>`` and ``#>>`` are deliberately absent: they are operators, and a
    #: probe that takes a function name should not answer about one.
    _JSON_FUNCTIONS_BY_VERSION = (
        ((9, 3, 0), ("json_extract_path",)),
        ((9, 4, 0), ("jsonb_extract_path",)),
        ((9, 5, 0), ("json_extract_path_text",)),
        (
            (12, 0, 0),
            ("jsonb_path_query", "jsonb_path_query_array", "jsonb_path_query_first"),
        ),
    )

    def supports_json_function(self, function_name: str) -> bool:
        """Whether a named JSON function exists on this server.

        Gated per function rather than as one set, because the names did not
        arrive together: ``json_extract_path`` is 9.3, ``jsonb_extract_path``
        is 9.4, ``json_extract_path_text`` is 9.5 and the ``jsonb_path_query_*``
        family is 12.0. A server has some of these and not others.
        """
        wanted = function_name.lower()
        return any(
            wanted in {name.lower() for name in names}
            for introduced, names in self._JSON_FUNCTIONS_BY_VERSION
            if self.version >= introduced
        )

    def format_json_expression(self, expr: "JSONPathNode") -> Tuple[str, tuple]:
        """Render a JSON path access according to the expression's mode.

        Overriding the core dispatch entry point previously ignored
        ``expr.mode`` entirely, so ``ARROW`` silently produced jsonpath SQL
        and ``FUNCTION`` was indistinguishable from ``AUTO`` -- even though
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
        to know. Rooting a single rootless accessor for the path form (see
        :func:`_canonical_jsonpath`) does not change that: it makes ``a`` mean
        the key ``a`` in both modes, but arrows read their argument literally,
        so a path the caller wrote with a root -- the only spelling that can
        chain -- still cannot be handed to them.

        Args:
            expr: The JSON path node -- a
                :class:`~rhosocial.activerecord.backend.expression.JSONDocumentExpression`
                for ``->`` or a
                :class:`~rhosocial.activerecord.backend.expression.JSONTextExpression`
                for ``->>``. Either is accepted; see ``JSONPathNode``.

        Returns:
            Tuple of (SQL string, parameters tuple).

        Raises:
            UnsupportedFeatureError: If ARROW mode is asked of a server with no
                arrows, or if the path needs the SQL/JSON path language on a
                server older than 12.0. Both are refusals of something the
                server cannot do -- never of something it can.
        """
        from rhosocial.activerecord.backend.expression.advanced_functions import JSONPathMode

        mode: JSONPathMode = getattr(expr, "mode", JSONPathMode.AUTO)

        if mode is JSONPathMode.ARROW:
            return self.format_json_arrow_expression(expr)

        # AUTO and FUNCTION both render the path form: the path argument is a
        # jsonpath, which the arrow operators cannot consume.
        return self._format_json_path_expression(expr)

    def _format_json_path_expression(self, expr: "JSONPathNode") -> Tuple[str, tuple]:
        """Render the path form, in whichever spelling this server has.

        Both core JSON path classes are accepted, and both are told apart by
        ``expr.operation`` rather than by type: ``->>`` unwraps because it
        yields text, ``->`` does not because it yields a document. The
        ``operation`` field is what renders here, so which class the caller
        happened to build does not change the SQL.

        Which spelling is used is the server's answer, not the caller's: the
        SQL/JSON path language where it exists, the ``#>`` / ``#>>`` operators
        where it does not. Both are the *path* form, which is what AUTO and
        FUNCTION mean here.

        The path is bound as a parameter in both spellings -- as the jsonpath
        itself on 12.0+, and as one bound element per key before that -- so a
        document-driven path cannot alter the statement's shape either way.
        """
        from rhosocial.activerecord.backend.expression import bases

        if not self.supports_json_path():
            raise UnsupportedFeatureError(
                self.name,
                "JSON path expressions (-> and #> operators)",
                "This PostgreSQL is older than 9.3, which added the JSON path "
                "operators. There is no way to read a path out of a JSON value "
                "on this server: store the value as text and parse it in "
                "Python instead.",
            )

        if isinstance(expr.column, bases.BaseExpression):
            col_sql, col_params = expr.column.to_sql()
        else:
            col_sql, col_params = self.format_identifier(str(expr.column)), ()

        if self.supports_sql_json_path():
            sql, params = self._format_sql_json_path_expression(col_sql, expr)
        else:
            sql, params = self._format_key_path_expression(col_sql, expr)
        params = col_params + params

        if expr.alias:
            sql = f"{sql} AS {self.format_identifier(expr.alias)}"

        return sql, params

    def _format_sql_json_path_expression(
        self, col_sql: str, expr: "JSONPathNode"
    ) -> Tuple[str, tuple]:
        """Render via ``jsonb_path_query_first``, the SQL/JSON path form.

        The path is bound as a ``jsonpath`` parameter rather than inlined, so
        a document-driven path cannot alter the statement's shape. A rootless
        single accessor is rooted first -- ``a`` becomes ``$.a``, ``[0]``
        becomes ``$[0]`` -- because the path language reads a rootless form
        as a variable reference or a syntax error where the arrow spelling
        reads a key; see :func:`_canonical_jsonpath`.

        The column expression is cast to ``jsonb`` before the call, because
        the function exists only for ``jsonb``. Without the cast a ``json`` or
        ``text`` column is an ``UndefinedFunction`` on every server that has
        the function rather than a value. The cast is a no-op on a ``jsonb``
        column; for a ``json`` column it means the document is parsed and
        normalised as ``jsonb`` first -- duplicate object keys resolve to the
        last one and object key order is not preserved -- which is also how
        the ``#>`` spelling reads the same column through its own cast, so a
        caller cannot choose between the two trade-offs by choosing a
        spelling. A ``text`` column is parsed as JSON, so its contents have to
        be valid JSON, as they must already be for any path to be read out of
        them.

        Raises:
            UnsupportedFeatureError: If the path omits the root without being
                a single accessor -- a longer rootless chain has no meaning
                shared by the arrow spelling and the path language, so there
                is no rooted jsonpath to bind.
        """
        placeholder = self.get_parameter_placeholder()

        try:
            path = _canonical_jsonpath(expr.path)
        except ValueError as exc:
            raise UnsupportedFeatureError(
                self.name,
                "this JSON path",
                f"The SQL/JSON path language reads only a rooted path, and this "
                f"path {exc}. A rootless chain has no meaning shared by the "
                f"arrow spelling and the path language, so only a single "
                f"rootless accessor is accepted; write the root explicitly, "
                f"e.g. '$.a'.",
            ) from None

        value_sql = (
            f"jsonb_path_query_first(({col_sql})::jsonb, {placeholder}::jsonpath)"
        )
        if expr.operation == "->>":
            return f"({value_sql} #>> '{{}}')", (path,)
        return value_sql, (path,)

    def _format_key_path_expression(
        self, col_sql: str, expr: "JSONPathNode"
    ) -> Tuple[str, tuple]:
        """Render via ``#>`` / ``#>>``, the path form PostgreSQL had before 12.0.

        ``#>`` takes a key array. The column is cast first -- to ``jsonb``, or
        to ``json`` on 9.3, the one version this spelling reaches before
        ``jsonb`` exists -- so the operator accepts whatever the column is
        declared as: ``jsonb`` and ``json`` are JSON types and the cast is a
        no-op for the first and a normalisation for the second, and ``text``
        -- which has no ``#>`` operator at all -- is accepted once cast.
        Without it a ``text`` column raises ``operator does not exist: text
        #>> text[]`` on every version, and this spelling is the only one
        servers before 12.0 have. The cast is also what lets one formatter
        serve every column type: the function spellings of the same walk,
        ``json_extract_path`` and ``jsonb_extract_path``, would each need to
        know the column's declared type to be chosen correctly, while ``#>``
        works on either JSON type without having to know which one it holds.

        The cost of the cast is confined to a ``json`` (or ``text``) column:
        the document is parsed and normalised as ``jsonb``, so duplicate
        object keys resolve to the last one and object key order is not
        preserved. The 12.0+ jsonpath spelling casts the same way, so the two
        spellings agree on the value.

        ``#>>`` yields text and ``#>`` a document, matching what ``->>`` and
        ``->`` yield.

        Each key is bound as its own parameter and assembled into ``ARRAY[...]``,
        so no key is ever interpolated into SQL.

        Raises:
            UnsupportedFeatureError: If the path needs the SQL/JSON path
                language -- a filter, a wildcard, arithmetic, a variable, or a
                rootless chain -- since nothing before 12.0 can evaluate one.
                The message names the construct so the caller can see what the
                server is missing rather than that the server is old. Note the
                two deliberate limits of this spelling in
                :func:`_jsonpath_key_array`.
        """
        try:
            keys = _jsonpath_key_array(expr.path)
        except ValueError as exc:
            raise UnsupportedFeatureError(
                self.name,
                "this JSON path",
                f"This PostgreSQL is older than 12.0, which introduced the "
                f"SQL/JSON path language, and the path cannot be translated to "
                f"the key array -> and #> take because it {exc}. Nothing before "
                f"12.0 can evaluate it.",
            ) from None

        if keys:
            placeholder = self.get_parameter_placeholder()
            array_sql = "ARRAY[{}]::text[]".format(
                ", ".join(f"{placeholder}::text" for _ in keys)
            )
            params = keys
        else:
            # The root. No key to bind, and '{}' is an empty text[] literal, so
            # there is nothing here a caller could have influenced.
            array_sql = "'{}'"
            params = ()

        # ``jsonb`` is the cast target wherever it exists, but it arrived in
        # 9.4 and this spelling reaches back to 9.3, where ``json`` is the
        # only JSON type and ``#`` works on it just the same.
        cast_type = "jsonb" if self.supports_jsonb() else "json"
        operator = "#>>" if expr.operation == "->>" else "#>"
        return f"({col_sql})::{cast_type} {operator} {array_sql}", params

    def supports_json_type(self) -> bool:
        """JSON is supported since PostgreSQL 9.2."""
        return self.version >= (9, 2, 0)

    def supports_jsonb(self) -> bool:
        """Check if PostgreSQL version supports JSONB type (introduced in 9.4)."""
        return self.version >= (9, 4, 0)

    def supports_json_path(self) -> bool:
        """Whether a JSON path can be read at all. PostgreSQL 9.3+.

        The core contract for this probe is "can this server read a path out of
        a JSON value", and PostgreSQL could from 9.3, when ``->``, ``->>``,
        ``#>`` and ``#>>`` arrived for ``json`` (``jsonb`` followed in 9.4 with
        the type itself).

        The probe answered 12.0+, which is the version of the *SQL/JSON path
        language* -- a different capability, now
        :meth:`supports_sql_json_path`. With the two conflated, 9.6, 10 and 11
        refused to render a path the server answers, behind two probes that
        both said PostgreSQL had JSON.
        """
        return self.version >= (9, 3, 0)

    def supports_sql_json_path(self) -> bool:
        """Whether the SQL/JSON path language is available. PostgreSQL 12.0+.

        This is the capability :meth:`supports_json_path` used to be given credit
        for, and the reason a path is rendered differently on 9.6 than on 12.0:
        the jsonpath functions and the ``@?`` / ``@@`` operators arrived in
        12.0, and only they can evaluate a filter or a wildcard.

        Split out because a server can have one without the other. PostgreSQL has
        had a path read since 9.3 and the language since 12.0, and a caller that
        asks the only versioned question gets a refusal it cannot act on.
        """
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
        subscript -- the arrow operators are the only spelling this backend
        uses -- so a True here advertised syntax PostgreSQL cannot parse.
        """
        return False

    def supports_infinity_numeric_infinity_jsonb(self) -> bool:
        """Whether numeric infinity values are allowed in JSONB (PostgreSQL 17+)."""
        return self.version >= (17, 0, 0)
