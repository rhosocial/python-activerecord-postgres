# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_json_path_modes.py
"""JSON path rendering modes on PostgreSQL.

``format_json_expression`` overrode the core dispatch entry point and ignored
``expr.mode``: ARROW produced the jsonpath form and FUNCTION was
indistinguishable from AUTO, while ``supports_json_arrow_operators()``
advertised arrows that the implementation never emitted. A probe and its
implementation have to agree, so the mode now decides.

Core also split what used to be one ``JSONExpression`` class in two -- one per
operator -- so the tests below name the class their operator means: ``->>``
yields text and is a ``JSONTextExpression``, ``->`` yields a document and is a
``JSONDocumentExpression``. PostgreSQL renders both from the same code path and
still branches on ``expr.operation``, so this is where the class is chosen, not
what changes the SQL.

The path form then had to grow a second spelling. It was the SQL/JSON path
language, ``jsonb_path_query_first``, which is 12.0+, and every mode that means
"read a path" refused below that -- so ``json_extract_text`` could not be used at
all on 9.6, 10 or 11, on servers that have read a path since 9.3 through ``->``,
``#>`` and ``jsonb_extract_path``. Below 12.0 the path form is now the ``#>`` /
``#>>`` operators, which take a key array rather than a jsonpath, so a path has
to be taken apart to be rendered. 12.0 and later are untouched.

Two things the renderer assumed about its column are asserted here because they
were wrong. ``jsonb_path_query_first`` exists only for ``jsonb``, so a ``json``
or ``text`` column handed to it without a cast is an ``UndefinedFunction``; ``#>`` has
no ``text`` overload on any version. Both spellings now cast the column first.
And the path language reads a rootless argument as something other than a key
(``'a'`` is a variable reference, ``'a.b'`` a syntax error), so the jsonpath
spelling roots a single rootless accessor and refuses a longer chain, the way
the key-array spelling always has.

Every assertion but the last is on rendered SQL, and those need no database.
The last one executes the rendered statements against the scenario the run
selected, because a cast and a canonicalised path are only proven by the
server's parser.
"""

# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_json_path_modes.py
import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.mixins.json import JSONMixin
from rhosocial.activerecord.backend.dialect.protocols import JSONSupport
from rhosocial.activerecord.backend.expression import (
    Column,
    JSONDocumentExpression,
    JSONTextExpression,
)
from rhosocial.activerecord.backend.expression.advanced_functions import JSONPathMode
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.mixins.types.jsonb_enhanced import (
    PostgresJSONBEnhancedMixin,
)
from rhosocial.activerecord.backend.impl.postgres.protocols.types.jsonb_enhanced import (
    PostgresJSONBEnhancedSupport,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

from providers.scenarios import get_enabled_scenarios, get_scenario

#: A SELECT, so the backend reads the result set back into ``.data``.
DQL = ExecutionOptions(stmt_type=StatementType.DQL)

#: The scenarios the ``--scenarios`` option left enabled. Read here so the
#: execution test runs against the selected server, not every configured one:
#: the fixture layer next door predates the option's filtering and would walk
#: all of them.
LIVE_SCENARIOS = list(get_enabled_scenarios().keys())


def _dialect(version=(16, 2, 1)):
    dialect = PostgresDialect()
    dialect._version = version
    return dialect


def _text_expr(dialect, mode=None, path="$.a"):
    """A ``->>`` access, which yields text and so is a JSONTextExpression."""
    return JSONTextExpression(dialect, Column(dialect, "data", table="t"), path, "->>", mode=mode)


def _document_expr(dialect, mode=None, path="$.a"):
    """A ``->`` access, which yields a document and so is a JSONDocumentExpression."""
    return JSONDocumentExpression(dialect, Column(dialect, "data", table="t"), path, "->", mode=mode)


# ---------------------------------------------------------------------------
# The mode decides
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["arrow", JSONPathMode.ARROW])
def test_arrow_modes_use_the_operators(mode):
    """Only an explicit ARROW renders arrows.

    The default (mode=None, which becomes AUTO) is deliberately absent. It
    renders the path form, because the two spellings take different path
    arguments: ``->`` takes a key name while ``jsonb_path_query_first`` takes a
    jsonpath. ``json_extract_text`` passes jsonpaths, so AUTO cannot assume
    the caller wants a key.
    """
    sql, _ = _text_expr(_dialect(), mode).to_sql()
    assert sql == """"t"."data"->>'$.a'"""


@pytest.mark.parametrize("mode", [None, "auto", "function"])
def test_default_and_function_modes_use_the_path_language(mode):
    sql, _ = _text_expr(_dialect(), mode).to_sql()
    assert "jsonb_path_query_first" in sql


def test_function_mode_uses_the_path_language():
    sql, params = _text_expr(_dialect(), "function").to_sql()
    assert "jsonb_path_query_first" in sql
    assert params == ("$.a",)


def test_the_two_modes_really_differ():
    """Before the fix all four mode values produced the same SQL."""
    arrow, _ = _text_expr(_dialect(), "arrow").to_sql()
    function, _ = _text_expr(_dialect(), "function").to_sql()
    assert arrow != function


def test_json_operation_keeps_the_jsonb_result():
    """``->`` yields JSONB, so the path form is not unwrapped to text."""
    sql, _ = _document_expr(_dialect(), "function").to_sql()
    assert " #>> " not in sql
    assert "jsonb_path_query_first" in sql


def test_text_operation_unwraps_the_scalar():
    """``->>`` yields text, so the path form is unwrapped with ``#>> '{}'``."""
    sql, _ = _text_expr(_dialect(), "function").to_sql()
    assert "#>> '{}'" in sql


def test_path_is_bound_not_inlined():
    """A document-driven path must not be able to change the statement shape."""
    sql, params = _text_expr(
        _dialect(), "function", path="$.a'); DROP TABLE t; --"
    ).to_sql()
    assert "DROP TABLE" not in sql
    assert params == ("$.a'); DROP TABLE t; --",)


def test_alias_is_appended():
    dialect = _dialect()
    expr = JSONTextExpression(
        dialect, Column(dialect, "data", table="t"), "$.a", "->>", alias="v", mode="arrow"
    )
    assert expr.to_sql()[0] == """"t"."data"->>'$.a' AS "v\""""


# ---------------------------------------------------------------------------
# Version gating
# ---------------------------------------------------------------------------


def test_path_can_be_read_from_nine_three():
    """The path operators arrived with 9.3, so the probe says so.

    It used to answer 12.0+, which is the version of the SQL/JSON *path
    language*. Those are two capabilities: 9.6 through 11 refused to render a
    path the server answers, behind a supports_json_type() that said PostgreSQL
    had JSON.
    """
    assert _dialect((9, 3, 0)).supports_json_path() is True
    assert _dialect((9, 2, 0)).supports_json_path() is False


def test_the_path_language_is_its_own_probe():
    """12.0 is the language, not the path."""
    assert _dialect((12, 0, 0)).supports_sql_json_path() is True
    assert _dialect((11, 16, 0)).supports_sql_json_path() is False
    # ...and the older server still reads a path, just not with jsonpath.
    assert _dialect((11, 16, 0)).supports_json_path() is True


def test_function_mode_uses_the_path_operators_before_twelve():
    """Below 12.0 the path form is ``#>``, which takes a key array.

    This test asserted the opposite -- that FUNCTION mode refuses below 12.0 --
    and that assertion was the defect: a formatter raising behind a probe that
    said PostgreSQL had JSON, so ``json_extract_text`` could not be used at all
    on three of the eleven supported servers.
    """
    sql, params = _text_expr(_dialect((11, 5, 0)), "function").to_sql()
    assert "#>>" in sql
    assert "jsonb_path_query_first" not in sql
    assert params == ("a",)


def test_function_mode_keeps_the_jsonpath_from_twelve():
    """12.0 and later are unchanged: the path language, not ``#>``."""
    sql, params = _text_expr(_dialect((12, 0, 0)), "function").to_sql()
    assert "jsonb_path_query_first" in sql
    assert "#>> ARRAY" not in sql
    assert params == ("$.a",)


def test_a_rootless_accessor_is_rooted_for_the_path_language():
    """``a`` must arrive at the server as ``$.a``.

    The path language reads a rootless ``'a'`` as a variable reference and
    rejects it; ARROW mode and the pre-12 key-array spelling read it as the
    key ``a``. Binding ``$.a`` is how the same argument means the same thing
    in the spelling that actually takes a jsonpath.
    """
    sql, params = _text_expr(_dialect((12, 0, 0)), "function", path="a").to_sql()
    assert "jsonb_path_query_first" in sql
    assert params == ("$.a",)


def test_a_rootless_subscript_and_quoted_key_are_rooted_too():
    """The other two shapes a single rootless accessor can have.

    ``[0]`` is a subscript and ``"a b"`` a quoted key, and they have to stay
    different jsonpaths: rooting the key array's string form would turn
    ``[0]`` into ``$."0"``, a different member.
    """
    dialect = _dialect((12, 0, 0))
    assert _text_expr(dialect, "function", path="[0]").to_sql()[1] == ("$[0]",)
    assert _text_expr(dialect, "function", path='"a b"').to_sql()[1] == ('$."a b"',)


def test_a_rootless_chain_is_refused_with_the_path_language():
    """The two spellings do not agree on ``a.b``, so 12.0 refuses it too.

    Below 12.0 the refusal is that no key array can be built; here the
    jsonpath cannot be rooted. Both come from the one single-accessor rule,
    and both have to be errors: reading ``a.b`` as ``$.a.b`` would answer an
    argument the key-array spelling refuses, and reading it as the variable
    reference the path language sees would answer a different question.
    """
    with pytest.raises(UnsupportedFeatureError) as excinfo:
        _text_expr(_dialect((12, 0, 0)), "function", path="a.b").to_sql()
    message = str(excinfo.value)
    assert "root $" in message
    assert "single" in message


def test_the_key_array_is_built_from_the_path():
    """``$.tags[0]`` is the key array ``{tags,0}``, one bound element per key."""
    dialect = _dialect((11, 5, 0))
    sql, params = _text_expr(dialect, "function", path="$.tags[0]").to_sql()
    assert params == ("tags", "0")
    assert sql.count("%s::text") == 2, sql


def test_the_root_is_an_empty_key_array():
    sql, params = _text_expr(_dialect((11, 5, 0)), "function", path="$").to_sql()
    assert params == ()
    assert "'{}'" in sql


def test_keys_are_bound_not_inlined():
    """A document-driven path must not be able to change the statement shape.

    Before 12.0 the path is taken apart into keys, so this is the stronger form
    of the check: a key carrying SQL comes out as a parameter and never as text,
    and it does not have to be escaped to get there.
    """
    dialect = _dialect((11, 5, 0))
    sql, params = _text_expr(
        dialect, "function", path="$.\"a'); DROP TABLE t; --\""
    ).to_sql()
    assert "DROP TABLE" not in sql
    assert params == ("a'); DROP TABLE t; --",)


def test_a_path_that_needs_the_language_still_refuses_before_twelve():
    """12.0 is where a filter becomes answerable, so 11 refuses that one.

    The refusal names the construct and the version, because "this server is
    old" is not something a caller can act on.
    """
    with pytest.raises(UnsupportedFeatureError) as excinfo:
        _text_expr(_dialect((11, 5, 0)), "function", path="$.tags[*]").to_sql()
    message = str(excinfo.value)
    assert "12.0" in message
    assert "wildcard" in message


@pytest.mark.parametrize(
    "path, reason",
    [
        ("$.tags[*]", "wildcard"),
        ("$.a ? (@.b)", "filter"),
        ("$..a", "recursive descent"),
        ("$.tags[last]", "[last]"),
        ("$.a-b", "subtraction"),
        ("$name", "jsonpath variable"),
        ("a.b", "root $"),
    ],
)
def test_every_language_only_construct_is_refused_with_its_reason(path, reason):
    with pytest.raises(UnsupportedFeatureError) as excinfo:
        _text_expr(_dialect((11, 5, 0)), "function", path=path).to_sql()
    assert reason in str(excinfo.value)


def test_a_single_rootless_accessor_is_read_as_a_key():
    """``a`` is what ``->`` reads, so the path form reads it the same way.

    The path language reads a rootless form as something else -- ``a`` is a
    variable reference and ``a.b`` is a syntax error -- but ARROW mode on this
    dialect has always read ``a`` as the key ``a``, and the two modes should not
    disagree about the same argument on the same server.
    """
    sql, params = _text_expr(_dialect((11, 5, 0)), "function", path="a").to_sql()
    assert params == ("a",)
    assert "#>> ARRAY" in sql


def test_a_rootless_chain_is_refused_rather_than_invented():
    with pytest.raises(UnsupportedFeatureError) as excinfo:
        _text_expr(_dialect((11, 5, 0)), "function", path="a.b.c").to_sql()
    assert "root $" in str(excinfo.value)


def test_no_path_at_all_is_refused_before_nine_three():
    """9.2 has a JSON type and no way to read a path out of it."""
    with pytest.raises(UnsupportedFeatureError) as excinfo:
        _text_expr(_dialect((9, 2, 0)), "function").to_sql()
    assert "9.3" in str(excinfo.value)


def test_arrow_mode_works_before_twelve():
    """Arrows are older than the path language, so PG 11 still has them."""
    sql, _ = _text_expr(_dialect((11, 5, 0)), "arrow").to_sql()
    assert "->>" in sql


def test_arrow_probe_is_gated_on_nine_three():
    """``->`` and ``->>`` arrived in 9.3, and the probe now says so.

    It returned True unconditionally, so on 9.2 -- where JSON exists and the
    operators do not -- it advertised a spelling the parser rejects. The class
    docstring above has always said 9.3.
    """
    assert _dialect((9, 3, 0)).supports_json_arrow_operators() is True
    assert _dialect((9, 2, 4)).supports_json_arrow_operators() is False


def test_arrow_mode_is_refused_before_nine_three():
    """The arrow formatter consults the probe, so ARROW refuses on 9.2."""
    with pytest.raises(UnsupportedFeatureError):
        _text_expr(_dialect((9, 2, 4)), "arrow").to_sql()


def test_arrow_probe_needs_a_version_to_answer():
    """An unadapted dialect raises from ``self.version``, like every probe.

    The version is what the question is about; a dialect that has not been
    told one has no answer to give, and quietly answering True is how the
    versionless probe above survived.
    """
    from rhosocial.activerecord.backend.dialect.exceptions import (
        DialectNotAdaptedException,
    )

    with pytest.raises(DialectNotAdaptedException):
        PostgresDialect().supports_json_arrow_operators()


# ---------------------------------------------------------------------------
# The cast that makes the column's declared type stop mattering
# ---------------------------------------------------------------------------


def test_the_jsonpath_spelling_casts_the_column_to_jsonb():
    """``jsonb_path_query_first`` has overloads for ``jsonb`` only.

    A ``json`` or ``text`` column handed to it without a cast is an
    ``UndefinedFunction`` on every server the function exists on, which is
    why a live 12.22 refused the previous SQL. The cast is a no-op on a
    ``jsonb`` column.
    """
    sql, _ = _text_expr(_dialect((12, 0, 0)), "function", path="$.a").to_sql()
    assert 'jsonb_path_query_first(("t"."data")::jsonb' in sql


def test_the_key_array_spelling_casts_the_column_to_jsonb():
    """``#>`` has no ``text`` overload on any version; the cast supplies one.

    The rendered shape is ``("t"."data")::jsonb #>> ARRAY[...]``, so the cast
    belongs to the column and not to the array.
    """
    sql, _ = _text_expr(_dialect((11, 5, 0)), "function", path="$.a").to_sql()
    assert sql.startswith('("t"."data")::jsonb #>>')
    assert "ARRAY" in sql


def test_the_key_array_spelling_casts_to_json_on_nine_three():
    """9.3 predates ``jsonb``, so the cast target is ``json`` there.

    The spelling reaches back to 9.3 and ``#`` works on ``json`` just as it
    does on ``jsonb``; casting to a type the server does not have would turn
    a working query into an error on the oldest server the probe claims.
    """
    sql, _ = _text_expr(_dialect((9, 3, 0)), "function", path="$.a").to_sql()
    assert sql.startswith('("t"."data")::json #>>')
    assert "::jsonb" not in sql


# ---------------------------------------------------------------------------
# Probes that used to lie
# ---------------------------------------------------------------------------


def test_json_table_is_not_claimed():
    """PostgreSQL has no JSON_TABLE; the probe used to claim 12.0+."""
    assert _dialect((16, 2, 1)).supports_json_table() is False


def test_json_table_error_names_the_real_construct():
    dialect = _dialect()
    with pytest.raises(UnsupportedFeatureError) as excinfo:
        dialect.format_json_table_expression(None)
    assert "jsonb_to_recordset" in str(excinfo.value)


def test_jsonb_subscript_is_not_claimed():
    """There is no ``jsonb['key']`` in PostgreSQL."""
    assert _dialect((16, 2, 1)).supports_jsonb_subscript() is False


def test_probes_declared_twice_agree():
    """A probe may be declared in two mixins, but both must answer the same.

    ``supports_jsonb_subscript`` is declared by two protocols, so two mixins
    define it and the MRO picks one arbitrarily. That is only safe because both
    return the same thing; the previous version gates of 11.0 and 14.0 made the
    winner an accident of class ordering.
    """
    from rhosocial.activerecord.backend.impl.postgres.mixins.types.data_type import (
        PostgresDataTypeMixin,
    )
    from rhosocial.activerecord.backend.impl.postgres.mixins.types.jsonb_enhanced import (
        PostgresJSONBEnhancedMixin,
    )

    dialect = _dialect()
    answers = {
        PostgresDataTypeMixin.supports_jsonb_subscript(dialect),
        PostgresJSONBEnhancedMixin.supports_jsonb_subscript(dialect),
    }
    assert len(answers) == 1, f"probe answers disagree: {answers}"


def test_arrow_probe_matches_what_is_actually_emitted():
    """A dialect advertising arrows must emit them when ARROW is asked for.

    AUTO is deliberately not checked here: on this dialect it renders the path
    form instead, which is why the check lives on the explicit mode.
    """
    dialect = _dialect()
    assert dialect.supports_json_arrow_operators() is True
    assert "->>" in _text_expr(dialect, "arrow").to_sql()[0]


@pytest.mark.parametrize(
    "function, introduced",
    [
        ("json_extract_path", (9, 3, 0)),
        ("jsonb_extract_path", (9, 4, 0)),
        ("json_extract_path_text", (9, 5, 0)),
        ("jsonb_path_query", (12, 0, 0)),
        ("jsonb_path_query_first", (12, 0, 0)),
        ("jsonb_path_query_array", (12, 0, 0)),
    ],
)
def test_a_function_is_claimed_only_once_the_server_has_it(function, introduced):
    """The probe answered from a flat name list, so it claimed 12.0+ functions
    on 9.6. A caller asking whether ``jsonb_path_query_first`` exists got True
    and then shipped SQL the server rejects."""
    assert _dialect(introduced).supports_json_function(function) is True
    if introduced > (9, 3, 0):
        just_below = (introduced[0], introduced[1] - 1, 0)
        assert _dialect(just_below).supports_json_function(function) is False


def test_a_function_postgres_does_not_have_is_never_claimed():
    dialect = _dialect((19, 0, 0))
    for name in ("JSON_EXTRACT", "JSON_VALUE", "OPENJSON", "json_extract"):
        assert dialect.supports_json_function(name) is False, name


def test_operators_are_not_answered_as_functions():
    """``#>`` is an operator. A probe that takes a function name saying yes to
    one conflated the two, and it was the only reason ``#>>`` was in the list."""
    dialect = _dialect((19, 0, 0))
    assert dialect.supports_json_function("#>>") is False


@pytest.mark.parametrize(
    "probe",
    [
        "supports_json_type",
        "supports_jsonb",
        "supports_json_path",
        "supports_sql_json_path",
        "supports_json_arrow_operators",
        "supports_json_table",
        "format_json_expression",
    ],
)
def test_a_json_probe_is_declared_once_per_layer(probe):
    """One implementation, one core default, one Protocol declaration.

    ``supports_json_type`` resolved from four classes: this dialect's mixin, the
    core ``JSONMixin`` default, the core ``JSONSupport`` protocol and *this
    backend's* protocol, which restated a member the core protocol already
    declared. The fourth was an abstract ``...``, not a competing answer -- but
    it is a second declaration in one MRO, and PostgresJSONBEnhancedSupport now
    derives from JSONSupport instead of repeating it.
    """
    definitions = [
        klass.__name__
        for klass in PostgresDialect.__mro__
        if probe in vars(klass)
    ]
    assert len(definitions) <= 3, f"{probe} resolves from {len(definitions)}: {definitions}"


def test_the_enhanced_protocol_extends_the_core_one():
    """Derivation is what keeps the count at three, and it is also the shape
    every other derived protocol here already had."""
    assert issubclass(PostgresJSONBEnhancedSupport, JSONSupport)
    # One implementation of supports_json_type, and it is the one that wins.
    assert (
        PostgresDialect.supports_json_type
        is PostgresJSONBEnhancedMixin.supports_json_type
    )
    assert PostgresDialect.__mro__.index(PostgresJSONBEnhancedMixin) < (
        PostgresDialect.__mro__.index(JSONMixin)
    )


# ---------------------------------------------------------------------------
# The cast, executed
# ---------------------------------------------------------------------------


@pytest.fixture(params=LIVE_SCENARIOS or [None])
def live_json_scenario(request):
    """One enabled scenario, or a skip when none is configured."""
    if request.param is None:
        pytest.skip("no PostgreSQL scenario is enabled")
    return request.param


def test_a_text_column_reads_through_both_spellings(live_json_scenario):
    """The cast is not cosmetic: without it both spellings raise, live.

    12.0 and later fail with ``function jsonb_path_query_first(text,
    jsonpath) does not exist``; before 12.0 with ``operator does not exist:
    text #>> text[]``. Rendering cannot catch either -- the SQL is
    syntactically fine and the column is the argument that has no
    overload -- so this runs the statement the test suite renders and reads
    the value back. A ``text`` column is the strongest case: it is the one
    no version had an operator for.
    """
    backend_class, config = get_scenario(live_json_scenario)
    backend = backend_class(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()
    dialect = backend.dialect
    if not dialect.supports_json_path():
        backend.disconnect()
        pytest.skip(f"{live_json_scenario} has no JSON path operators")

    table = "json_path_probe"
    backend.execute(f'DROP TABLE IF EXISTS "{table}"')
    backend.execute(f'CREATE TABLE "{table}" (data TEXT)')
    try:
        backend.execute(
            f'INSERT INTO "{table}" (data) VALUES (%s)',
            ('{"language": "zh-CN", "tags": ["first", "second"]}',),
        )
        # Rooted path, rootless single accessor, and a subscript: the second
        # only has a meaning because the renderer roots it first.
        for path, expected in (
            ("$.language", "zh-CN"),
            ("language", "zh-CN"),
            ("$.tags[0]", "first"),
        ):
            expr = JSONTextExpression(
                dialect,
                Column(dialect, "data", table=table),
                path,
                "->>",
                alias="v",
            )
            sql, params = expr.to_sql()
            result = backend.execute(
                f"SELECT {sql} FROM {dialect.format_identifier(table)}",
                params,
                options=DQL,
            )
            assert result.data is not None and len(result.data) == 1
            assert result.data[0]["v"] == expected, (
                f"{live_json_scenario}: {path} rendered {sql!r}"
            )
    finally:
        try:
            backend.execute(f'DROP TABLE IF EXISTS "{table}"')
        finally:
            backend.disconnect()
