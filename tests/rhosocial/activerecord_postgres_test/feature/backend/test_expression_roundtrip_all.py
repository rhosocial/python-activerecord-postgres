# tests/rhosocial/activerecord_postgres_test/feature/backend/test_expression_roundtrip_all.py
"""
Functional serialization coverage: every registered expression class must
round-trip losslessly through all three encodings (dict / JSON string / XML).

Reconciled against core at **550ae740** ("check the quoting flags, because
annotations do not"), where an artifact is a bare ``{kind, params}`` node:
no envelope, no ``__expr__``/``__tuple__``/``__enum__``/``__dataclass__``/
``__b64__``/``__dur__`` markers, values as plain JSON scalars, and the kind
derived from the class name -- ``Literal`` the single exception, naming its
fourteen value kinds by what they hold. ``test_value_kind_plane`` asserts that
plane (wire name, payload type, round-trip) because the class matrix sees only
one row for it whatever the payloads are.

Current census, for the record: 533 registered classes, 531 constructible, 2
pinned unconstructible, 471 rendered, 37 refused as unsupported, 20 classified
non-renders, 3 with no formatter declared. The two gap tables
(``_CODEC_GAPS`` / ``_CODEC_RENDER_GAPS``) are the only places a parameter may
be lost, each pinned per class *and* channel with its exception type.

For each expression class that can be constructed:
  1. dict round-trip   : deserialize(serialize(e)).get_params() == e.get_params()
  2. JSON round-trip   : deserialize_json(serialize_json(e)).get_params() == ...
  3. XML round-trip    : deserialize_xml(serialize_xml(e)).get_params() == ...
  4. SQL consistency   : classified, never swallowed -- see below.

Why this matrix collects two packages, not one
=============================================

``rhosocial.activerecord.backend.impl.postgres.expression`` holds the classes this
backend defines. ``rhosocial.activerecord.backend.expression`` holds core's, and
core's statements are exactly the ones a backend override consumes: when a
PostgreSQL statement formats a core ``Table`` or a core ``CommentOnStatement``,
the defect that matters is in *this* dialect's reading of a core object. That is
why ``mixins/ddl/comment.py`` could read a field core had removed and stay green:
core's own classes were outside the only matrix in this repository, so a
PostgreSQL-side formatter misreading them was unobservable.

The matrix therefore walks both packages with ``collect_expression_classes``. It
deliberately does not read the deserialization index: that dict is
process-global and grows as sibling test modules import other backends, so a
registry-based matrix covers a different class set depending on import order.
Walking the packages is deterministic.

Why ``to_sql()`` is classified rather than caught
================================================

This matrix used to call the testsuite's ``sql_consistent``, whose body is::

    try:
        expected = instance.to_sql()
    except Exception:
        return

so every render failure was a green tick and the three SQL comparisons never
ran. Measured on this backend before the rewrite -- walk the PostgreSQL
expression package only, build each class once with ``make_instance``, render it
once, and bucket the outcome (``scripts/expr_matrix_probe.py`` does exactly that):
of **168** classes discovered, **125** reached a SQL comparison, **20** raised and
were swallowed, and **23** could not be built and became ``pytest.skip``. So 43 of
168 -- a quarter -- were never asserted at all, and every one of them reported
green.

Each outcome is now named and asserted:

* **renders** -- all three encodings must restore byte-identical SQL *and*
  byte-identical bind parameters.
* ``UnsupportedFeatureError`` from a probe inside a formatter -- this dialect
  does not model the feature. Asserted as exactly that type, so a different
  error cannot hide behind it.
* ``UnsupportedFeatureError`` from the dispatch -- the dialect declares no
  ``format_*`` method for the class at all. The method it names must be a row
  in :data:`UNMODELLED_FORMATTERS`, so the gap is a decision with a reason
  rather than an omission; see the next section.
* a member of :data:`LEGITIMATE_NON_RENDERS` -- a class that cannot render for a
  reason belonging to its own tree or to PostgreSQL's feature set. Each entry
  pins the exception type *and* a message fragment, so a class that started
  failing for a different reason fails here instead of staying quietly green.
* **anything else** -- a failure naming the class and the exception.

Why one ``UnsupportedFeatureError`` became two answers
======================================================

Core reports a missing formatter and a declined feature through the same
exception type, and this matrix allowed both identically -- which let one of
them hide. A class whose formatter this dialect never mixed in read as
"PostgreSQL lacks the feature" and passed. Core turning ``trim``/``lpad``/
``rpad``/``repeat`` into dedicated nodes made the hole load-bearing: a backend
that shipped no ``format_lpad_expression`` answered every ``.lpad(...)`` with
that refusal, and nothing here could tell it apart from a genuine "not on
this engine".

The two are told apart by *who* is refusing, and the exception itself records
that: a probe inside a formatter names the feature it lacks, while the
dispatch names the formatting method it could not find, framed
``the '<method>' statement`` -- see :func:`_dispatched_formatter`. Only the
second is a wiring fact about this backend, so only the second has to be
accounted for: the method it names must be in
:data:`UNMODELLED_FORMATTERS` with the reason the feature is absent, and
:meth:`TestMatrixIntegrity.test_unmodelled_formatter_list_is_exact` pins that
table in both directions. A method no class needs any more retires; a method
nobody listed fails the run.

Why some pins are themselves ``UnsupportedFeatureError``
=========================================================

``to_sql()`` reports a missing formatter as ``UnsupportedFeatureError``, so the
two buckets above can overlap: a class can be unrenderable for a reason that is
about PostgreSQL rather than about its own tree. ``TableSource``,
``Pivot`` and ``Unpivot`` are such classes, and they are
still named in :data:`LEGITIMATE_NON_RENDERS` on purpose. The bare type is always
allowed, so the type alone asserts nothing about *which* formatter is missing;
the entry adds the message fragment, and that fragment names the one formatter
the class dispatches on. Deleting the entry would have made the test pass by
saying less.

And what happens when a class cannot be constructed
==================================================

``make_instance(...) is None`` becomes a skip, but only for a class named in
:data:`UNCONSTRUCTIBLE`, and ``test_unconstructible_list_is_exact`` pins that
tuple in both directions. A class that starts needing an exemption fails CI
instead of turning into a skip, and a stale entry fails too.
"""

import inspect
from enum import Enum
from typing import Dict

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import graph as graph_mod
from rhosocial.activerecord.backend.expression.advanced_functions import (
    CaseExpression,
    WindowClause,
    WindowDefinition,
    WindowSpecification,
)
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.collation import CollateFunction
from rhosocial.activerecord.backend.expression.core import Column, Literal
from rhosocial.activerecord.backend.expression import declared
from rhosocial.activerecord.backend.expression.datetime import (
    DatePartExpression,
    DateTimeAddExpression,
    DateTimeDiffExpression,
    DateTimeSubtractExpression,
    DateTimeField,
    DateTruncExpression,
    ExtractExpression,
    ExtractFieldExpression,
    IntervalExpression,
    IntervalUnit,
    TemporalOptionsExpression,
    _ExtractBase,
)
from rhosocial.activerecord.backend.expression.functions.string import (
    LeftFunction,
    RightFunction,
    SubstringExpression,
)
from rhosocial.activerecord.backend.expression.operators import ArithmeticOperation
from rhosocial.activerecord.backend.expression.query_sources import SetOperation
from rhosocial.activerecord.backend.expression.statements.dql import QueryStatement
from rhosocial.activerecord.backend.expression.objects import (
    Database,
    EdgeTable as EdgeTableObject,
    Function,
    MaterializedView,
    NodeTable,
    PropertyGraph,
    Table,
    Trigger,
)
from rhosocial.activerecord.backend.expression.predicates import (
    LikeOperator,
    ComparisonPredicate,
    ILikePredicate as CoreILIKEExpression,
    LikePredicate,
)
from rhosocial.activerecord.backend.expression.query_parts import JoinClause
from rhosocial.activerecord.backend.expression.serialization import (
    default_registry,
    deserialize,
    deserialize_json,
    deserialize_xml,
    serialize,
    serialize_json,
    serialize_xml,
)
from rhosocial.activerecord.backend.expression.sources import NamedRelationRef
from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
    AddColumnAction,
    AddTableConstraintAction,
    ValidateConstraintAction,
)
from rhosocial.activerecord.backend.expression.statements.ddl_database import (
    AlterDatabaseAction,
    AlterDatabaseStatement,
)
from rhosocial.activerecord.backend.expression.statements.ddl_domain import (
    AddDomainCheckAction,
    DomainCheckConstraint,
    RenameDomainAction,
)
from rhosocial.activerecord.backend.expression.statements.ddl_partition import (
    PartitionClause,
    PartitionStrategy,
)
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    ColumnCheckConstraint,
    ColumnConstraint,
    ColumnDefinition,
    ColumnGeneratedConstraint,
    ForeignKeyConstraint,
    ReferencesClause,
    TableCheckConstraint,
    TableConstraint,
    ColumnNotNullConstraint,
    TableExcludeConstraint,
    TablePrimaryKeyConstraint,
)
from rhosocial.activerecord.backend.expression.statements.ddl_trigger import (
    CreateTriggerStatement,
    TriggerEvent,
    TriggerTiming,
)
from rhosocial.activerecord.backend.expression.statements.dml import (
    MergeAction,
    MergeActionType,
    MergeStatement,
)
from rhosocial.activerecord.backend.expression.types import (
    ArrayType,
    CustomType,
    EnumType,
    IntegerType,
    VarCharType,
)
from rhosocial.activerecord.backend.expression.xml import (
    XMLAttribute,
    XMLAttributesExpression,
    XMLConcatExpression,
    XMLForestExpression,
    XMLForestItem,
    XMLTableColumn,
    XMLTableExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.copy import (
    PostgresCopyToExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.column import (
    PostgresColumnDefinition,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.constraint import (
    PostgresAlterConstraintAction,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.domain import (
    PostgresAddDomainCheckAction,
    PostgresAlterDomainExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.exclude_constraint import (
    PostgresExcludeBranch,
    PostgresExcludeConstraint,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.index import (
    PostgresAlterIndexActionType,
    PostgresAlterIndexExpression,
    PostgresReindexExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.mv import (
    PostgresAlterMaterializedViewExpression,
    PostgresResetMaterializedViewPropertiesAction,
    PostgresSetMaterializedViewPropertiesAction,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresAttachPartitionExpression,
    PostgresCreatePartitionExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.pg_partman import (
    PostgresPgPartmanUpdateConfigExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.policy import (
    PostgresAlterPolicyExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.publication import (
    PostgresCreatePublicationExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.rls_config import (
    PostgresAlterTableRlsExpression,
    RlsConfigurationMode,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.table_settings import (
    LoggingMode,
    PostgresAlterTableSettingsExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.type import (
    PostgresEnumValuesExpression,
    PostgresBaseTypeDefinition,
    PostgresCompositeTypeAttribute,
    PostgresCompositeTypeDefinition,
    PostgresCreateEnumTypeExpression,
    PostgresEnumTypeDefinition,
    PostgresSetTypePropertiesAction,
)
from rhosocial.activerecord.backend.impl.postgres.expression.enum_ import (
    PostgresEnumType,
)
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresArrayType,
)
from rhosocial.activerecord.backend.impl.postgres.expression.values import (
    PostgresHstoreValueExpression,
    PostgresLtreeValueExpression,
    PostgresNetworkValueExpression,
    PostgresRangeValueExpression,
)
from rhosocial.activerecord.backend.impl.postgres.functions import (
    hstore as pg_hstore,
    ltree as pg_ltree,
    network as pg_network,
    range as pg_range,
)
from expression_roundtrip_harness import (
    collect_expression_classes,
    make_instance,
    register_all,
    register_special_constructor,
)


def assert_params_equal(a, b, path="params"):
    """Deep compare two get_params() dicts, treating nested BaseExpression
    instances as structurally equal when their get_params() match.

    A ``(str, Enum)`` member and the word it carries are the same recorded
    value: the artifact writes the word (JSON has no enum), and a constructor
    that keeps whatever it was handed -- ``GraphEdge.direction``,
    ``AlterDatabaseStatement.action`` -- reads the word back. Comparing their
    *types* would call that a loss when nothing was lost.
    """
    if isinstance(a, BaseExpression) and isinstance(b, BaseExpression):
        assert_params_equal(a.get_params(), b.get_params(), path + ".<expr>")
        return
    if isinstance(a, Enum) and isinstance(b, Enum):
        assert a == b, f"{path}: {a!r} != {b!r}"
        return
    if isinstance(a, Enum) != isinstance(b, Enum):
        # The enum side is compared by the value it carries: that is what the
        # artifact wrote, and a constructor may legitimately keep the word.
        enum_side, other = (a, b) if isinstance(a, Enum) else (b, a)
        value = enum_side.value if isinstance(enum_side.value, str) else str(enum_side.value)
        assert value == other, f"{path}: {value!r} != {other!r}"
        return
    assert type(a) is type(b) or (
        isinstance(a, (list, tuple, dict)) and isinstance(b, (list, tuple, dict))
    ), f"{path}: type mismatch {type(a).__name__} vs {type(b).__name__}"
    if isinstance(a, dict):
        assert set(a) == set(b), f"{path}: keys differ {set(a) ^ set(b)}"
        for k in a:
            assert_params_equal(a[k], b[k], f"{path}.{k}")
        return
    if isinstance(a, (list, tuple)):
        assert len(a) == len(b), f"{path}: length differ"
        for i, (x, y) in enumerate(zip(a, b)):
            assert_params_equal(x, y, f"{path}[{i}]")
        return
    assert a == b, f"{path}: {a!r} != {b!r}"


PG_EXPR_PKG = "rhosocial.activerecord.backend.impl.postgres.expression"
CORE_EXPR_PKG = "rhosocial.activerecord.backend.expression"


def _collect_matrix_classes():
    """Every concrete expression class this backend can be asked to render.

    Two packages, walked rather than read out of the kind registry -- see the
    module docstring. Registration is a separate concern handled beside the
    collection: the framework's default registry walks the core and built-in
    backend packages itself, and the classes this backend defines are entered
    by :func:`register_all` below.
    """
    collected: Dict[str, type] = {}
    for package in (PG_EXPR_PKG, CORE_EXPR_PKG):
        collected.update(collect_expression_classes(package))
    by_identity: Dict[int, str] = {}
    for fqn, cls in sorted(collected.items()):
        by_identity.setdefault(id(cls), fqn)
    return {
        fqn: cls
        for cls in collected.values()
        if not inspect.isabstract(cls)
        for fqn in [by_identity[id(cls)]]
    }


REGISTERED = _collect_matrix_classes()
register_all(REGISTERED)
# Node kinds only. The registry once carried enum and dataclass tables too,
# because an artifact named those by class path; an artifact is plain JSON
# now, so a value is named by its kind and a host object has no name at all --
# which is why the PostgreSQL composite type attribute became a mapping its own
# node writes and reads, rather than something registered here.


# ---------------------------------------------------------------------------
# Special constructors: a real value where the introspective guess is a lie
# ---------------------------------------------------------------------------
#
# ``make_instance`` reads each required parameter's annotation and guesses:
# ``"x"`` for a string, ``[]`` for a list, ``IntegerType()`` for a type. That is
# right for a name and wrong for every parameter that wants a catalogue object --
# a Table, an Index, an enum member -- because a bare ``"x"`` is not one, and the
# formatter now refuses it by name. It is also wrong for the containers that
# require at least one member (CASE, WINDOW, JOIN, EXCLUDE), for the predicates
# that must render without bind parameters (a DOMAIN CHECK is DDL), and for the
# handful of strings that are really enums spelled as a union of an enum class
# and ``str`` -- ``constraint_type``, ``action_type`` -- where the guess cannot
# know which member is legal.
#
# Every registration below replaces a guess that would otherwise have produced an
# instance the PostgreSQL dialect cannot render. Suffixes are spelled relative to
# the expression package because ``make_instance`` matches with ``str.endswith``
# and several modules export identically named classes.


def _table_obj(dialect, name="t"):
    """A table with a bare name and no namespace."""
    return Table(dialect, name)


def _column_predicate(dialect):
    """A predicate comparing two columns, so it renders with no bind parameters."""
    return ComparisonPredicate(dialect, "=", Column(dialect, "a"), Column(dialect, "b"))


def _inline_column_predicate(dialect):
    """The predicate a DDL condition needs: a comparison of two columns."""
    return ComparisonPredicate(dialect, "=", Column(dialect, "a"), Column(dialect, "b"))


def _graph_column(dialect, variable="n", name="id"):
    """One ``COLUMNS`` entry, whose variable must name a graph vertex."""
    return graph_mod.GraphColumn(variable, name)


def _one_column_query(dialect, table="t"):
    """A one-column SELECT over a named table: the smallest valid query side."""
    return QueryStatement(
        dialect, select=[Column(dialect, "a")], from_=_table_obj(dialect, table)
    )


def _integer_column(dialect, name="col"):
    """A column definition carrying a *dialect-bound* type.

    The binding is load-bearing, not cosmetic: ``to_sql()`` dispatches on the
    type through its own dialect, so an unbound ``IntegerType()`` raises
    ``ValueError: ... has no dialect bound`` the moment anything renders it.
    """
    return ColumnDefinition(dialect, name, IntegerType(dialect))


def _primary_key_constraint(dialect):
    """A table-level PRIMARY KEY, shared by three guesses that need one."""
    return TableConstraint(dialect, branch=TablePrimaryKeyConstraint(dialect, ["a"]), name="c")


def _domain_check(dialect):
    """A DOMAIN CHECK constraint, which renders with no bind parameters.

    A predicate comparing a column to a literal would have to become a bind
    parameter, and DDL carries none -- so the check compares two columns.
    """
    return DomainCheckConstraint(dialect, _column_predicate(dialect), name="chk")


def _merge_action(dialect):
    """One MERGE ``WHEN MATCHED`` arm."""
    return MergeAction(
        dialect,
        MergeActionType.UPDATE,
        {"a": Literal(dialect, 1)},
        _column_predicate(dialect),
        "matched",
    )


def _alter_constraint(dialect):
    """An ALTER CONSTRAINT action carrying the one kind it accepts.

    ``constraint_kind`` names the **branch** the keyword rides on -- a branch
    class or its spelling -- and only CHECK and FOREIGN KEY are legal here, so
    most guesses are refused by name. The enforcement keyword is mandatory in the
    action's grammar, so the pair is set explicitly (``enforced=True``) as well.
    PostgreSQL's action and core's two share this shape, so one constructor is
    registered under each name: ``make_instance`` picks by ``str.endswith``, so a
    single shared suffix would also shadow unrelated classes.
    """
    return PostgresAlterConstraintAction(
        dialect,
        constraint_name="c",
        name="c",
        constraint_kind=TableCheckConstraint,
        enforced=True,
    )


#: The ``constraint_kind`` family: PostgreSQL's ALTER CONSTRAINT action and
#: core's two. All three need ``_alter_constraint``.
ALTER_CONSTRAINT_SUFFIXES = (
    "ddl.constraint.PostgresAlterConstraintAction",
    "statements.ddl_alter.AlterConstraintAction",
    "statements.ddl_alter.AlterTableConstraint",
)

#: The VALIDATE CONSTRAINT family. ``PostgreSQLValidateConstraint`` has no node
#: of its own -- the grammar and the parameters are core's -- so this is core's
#: two names only.
VALIDATE_CONSTRAINT_SUFFIXES = (
    "statements.ddl_alter.ValidateConstraintAction",
    "statements.ddl_alter.ValidateTableConstraint",
)


def register_specials():
    """Replace every introspective guess that would cost a real assertion."""
    # -- nodes whose ``op`` is a closed word set, not a free string ----------
    # Both nodes refuse an ``op`` outside their set at construction, so the
    # introspective guess (which supplies a placeholder string) cannot build
    # them at all. Naming one real word is what lets the matrix exercise them.
    register_special_constructor(
        "operators.ArithmeticOperation",
        lambda d: ArithmeticOperation(d, "add", Column(d, "a"), Literal(d, 1)),
    )
    register_special_constructor(
        "query_sources.SetOperation",
        lambda d: SetOperation(
            d,
            op="union",
            left=_one_column_query(d),
            right=_one_column_query(d, "other"),
        ),
    )

    # -- the FROM side ------------------------------------------------------
    register_special_constructor(
        "sources.relation.NamedRelationRef",
        lambda d: NamedRelationRef(d, _table_obj(d)),
    )
    register_special_constructor(
        "query_parts.JoinClause",
        lambda d: JoinClause(
            d,
            left_table=NamedRelationRef(d, _table_obj(d)),
            right_table=NamedRelationRef(d, Table(d, "other")),
            condition=_column_predicate(d),
        ),
    )

    # -- core objects that must reach a formatter as objects ---------------
    # ``to_sql`` dispatches through the object's own ``format_method``, so a
    # plain string in one of these slots was rendered by whatever formatter the
    # string happened to name -- or refused. Either way the statement was never
    # the one the class describes.
    register_special_constructor(
        "statements.ddl_alter.AddColumnAction",
        lambda d: AddColumnAction(d, _integer_column(d)),
    )
    register_special_constructor(
        "statements.ddl_alter.AddTableConstraintAction",
        lambda d: AddTableConstraintAction(d, _primary_key_constraint(d)),
    )
    register_special_constructor(
        "statements.ddl_table.ColumnConstraint",
        lambda d: ColumnConstraint(d, branch=ColumnNotNullConstraint(d), name="c"),
    )
    register_special_constructor(
        "statements.ddl_table.TableConstraint",
        lambda d: _primary_key_constraint(d),
    )
    register_special_constructor(
        "statements.ddl_table.ForeignKeyConstraint",
        lambda d: ForeignKeyConstraint(
            d,
            columns=["a"],
            foreign_key_table=Table(d, "other"),
            foreign_key_columns=["b"],
            name="fk",
        ),
    )
    register_special_constructor(
        "statements.ddl_table.ReferencesClause",
        lambda d: ReferencesClause(d, Table(d, "other"), ["b"]),
    )
    register_special_constructor(
        "statements.ddl_database.AlterDatabaseStatement",
        lambda d: AlterDatabaseStatement(
            d,
            Database(d, "db"),
            action=AlterDatabaseAction.RENAME_TO,
            target="renamed_db",
        ),
    )
    register_special_constructor(
        "statements.ddl_trigger.CreateTriggerStatement",
        lambda d: CreateTriggerStatement(
            d,
            trigger=Trigger(d, "trg"),
            table=_table_obj(d),
            timing=TriggerTiming.BEFORE,
            events=[TriggerEvent.INSERT],
            function=Function(d, "fn"),
        ),
    )

    # -- DDL checks render with no bind parameters -------------------------
    # A DOMAIN CHECK is DDL. A predicate comparing a column to a literal would
    # have to become a bind parameter, and DDL carries none, so both the check
    # itself and the action that adds it need a column-to-column predicate.
    register_special_constructor(
        "statements.ddl_domain.DomainCheckConstraint",
        lambda d: _domain_check(d),
    )
    register_special_constructor(
        "statements.ddl_domain.AddDomainCheckAction",
        lambda d: AddDomainCheckAction(d, _domain_check(d)),
    )

    # -- the constraint families --------------------------------------------
    # ``constraint_kind`` is a union of a branch class and ``str``, so the guess
    # cannot know which one is legal; the VALIDATE family needs only a name.
    for suffix in ALTER_CONSTRAINT_SUFFIXES:
        register_special_constructor(suffix, lambda d: _alter_constraint(d))
    for suffix in VALIDATE_CONSTRAINT_SUFFIXES:
        register_special_constructor(
            suffix, lambda d: ValidateConstraintAction(d, constraint_name="c", name="c")
        )

    # -- PostgreSQL statements whose required slot is a validated enum -----
    # ``partition_type`` is checked against {RANGE, LIST, HASH}; ``target_type``
    # against the REINDEX targets; ``mode`` against the RLS modes. The guess
    # supplies a bare string and the formatter refuses it.
    register_special_constructor(
        "ddl.partition.PostgresCreatePartitionExpression",
        lambda d: PostgresCreatePartitionExpression(
            d,
            partition_name="p1",
            parent_table="t",
            partition_type="LIST",
            partition_values={"default": True},
        ),
    )
    register_special_constructor(
        "ddl.partition.PostgresAttachPartitionExpression",
        lambda d: PostgresAttachPartitionExpression(
            d,
            partition_name="p1",
            parent_table="t",
            partition_type="LIST",
            partition_values={"default": True},
        ),
    )
    register_special_constructor(
        "ddl_partition.PartitionClause",
        lambda d: PartitionClause(d, PartitionStrategy.RANGE, [Column(d, "a")]),
    )
    register_special_constructor(
        "ddl.index.PostgresAlterIndexExpression",
        lambda d: PostgresAlterIndexExpression(
            d,
            index_name="i",
            action_type=PostgresAlterIndexActionType.RENAME_TO,
            new_name="j",
        ),
    )
    register_special_constructor(
        "ddl.index.PostgresReindexExpression",
        lambda d: PostgresReindexExpression(d, target_type="INDEX", name="i"),
    )
    register_special_constructor(
        "ddl.rls_config.PostgresAlterTableRlsExpression",
        lambda d: PostgresAlterTableRlsExpression(
            d, table_name="t", mode=RlsConfigurationMode.ENABLE
        ),
    )
    register_special_constructor(
        "ddl.table_settings.PostgresAlterTableSettingsExpression",
        lambda d: PostgresAlterTableSettingsExpression(
            d, table_name="t", mode=LoggingMode.LOGGED
        ),
    )

    # -- PostgreSQL statements whose required slot is a non-empty member ----
    # Each of these refuses to be a statement without the member its name says it
    # modifies, so the introspective ``[]`` is refused by design.
    register_special_constructor(
        "ddl.exclude_constraint.PostgresExcludeConstraint",
        lambda d: PostgresExcludeConstraint(d, name="ex", elements=[(Column(d, "a"), "=")]),
    )
    # The EXCLUDE *branches* need the same non-empty member list the sugar does,
    # and there are two of them: core's and PostgreSQL's.
    register_special_constructor(
        "ddl_table.TableExcludeConstraint",
        lambda d: TableExcludeConstraint(d, ["a"], "="),
    )
    register_special_constructor(
        "ddl.exclude_constraint.PostgresExcludeBranch",
        lambda d: PostgresExcludeBranch(d, [(Column(d, "a"), "=")]),
    )
    register_special_constructor(
        "ddl.pg_partman.PostgresPgPartmanUpdateConfigExpression",
        lambda d: PostgresPgPartmanUpdateConfigExpression(
            d, parent_table="t", retention="7 days"
        ),
    )
    register_special_constructor(
        "ddl.policy.PostgresAlterPolicyExpression",
        lambda d: PostgresAlterPolicyExpression(
            d, name="p", table_name="t", new_name="q"
        ),
    )
    register_special_constructor(
        "ddl.publication.PostgresCreatePublicationExpression",
        lambda d: PostgresCreatePublicationExpression(
            d, name="pub", tables=["t"]
        ),
    )

    # -- PostgreSQL DDL type definitions ------------------------------------
    # ``values`` / ``labels`` / ``properties`` are keyword-only behind defaulted
    # positionals, so the introspective constructor skips them and the definition
    # declares nothing.
    register_special_constructor(
        "ddl.type.PostgresCreateEnumTypeExpression",
        lambda d: PostgresCreateEnumTypeExpression(d, name="mood", values=["sad", "ok"]),
    )
    register_special_constructor(
        "ddl.type.PostgresEnumValuesExpression",
        lambda d: PostgresEnumValuesExpression(d, values=["sad", "ok"]),
    )
    register_special_constructor(
        "ddl.type.PostgresEnumTypeDefinition",
        lambda d: PostgresEnumTypeDefinition(d, labels=["sad", "ok"]),
    )
    register_special_constructor(
        "ddl.type.PostgresSetTypePropertiesAction",
        lambda d: PostgresSetTypePropertiesAction(d, properties={"STORAGE": "plain"}),
    )
    # A base type is a shell type: it needs the two I/O functions and everything
    # else is optional, so the introspective constructor supplies nothing at all
    # because ``dialect`` is the only non-defaulted parameter.
    register_special_constructor(
        "ddl.type.PostgresBaseTypeDefinition",
        lambda d: PostgresBaseTypeDefinition(
            d, input_function="int4in", output_function="int4out", internallength=4
        ),
    )
    register_special_constructor(
        "ddl.type.PostgresCompositeTypeDefinition",
        lambda d: PostgresCompositeTypeDefinition(
            d,
            attributes=[
                PostgresCompositeTypeAttribute("a", IntegerType(d))
            ],
        ),
    )
    register_special_constructor(
        "enum_.PostgresEnumType",
        lambda d: PostgresEnumType(d, name="mood", values=["sad", "ok"]),
    )

    # -- columns, COPY, materialized views ---------------------------------
    # A PostgreSQL column definition's ``data_type`` is required and untyped in
    # the annotation, so the guess supplies nothing for it.
    register_special_constructor(
        "ddl.column.PostgresColumnDefinition",
        lambda d: PostgresColumnDefinition(d, "c", IntegerType(d)),
    )
    # COPY needs a source: either a query or a table. With neither, there is no
    # statement.
    register_special_constructor(
        "copy.PostgresCopyToExpression",
        lambda d: PostgresCopyToExpression(d, table_name="t"),
    )
    register_special_constructor(
        "ddl.mv.PostgresSetMaterializedViewPropertiesAction",
        lambda d: PostgresSetMaterializedViewPropertiesAction(
            d, properties={"autovacuum_enabled": "true"}
        ),
    )
    register_special_constructor(
        "ddl.mv.PostgresResetMaterializedViewPropertiesAction",
        lambda d: PostgresResetMaterializedViewPropertiesAction(d, parameters=["fillfactor"]),
    )
    register_special_constructor(
        "ddl.mv.PostgresAlterMaterializedViewExpression",
        lambda d: PostgresAlterMaterializedViewExpression(
            d,
            view=MaterializedView(d, "mv"),
            actions=[PostgresSetMaterializedViewPropertiesAction(
                d, properties={"fillfactor": "70"}
            )],
        ),
    )
    register_special_constructor(
        "ddl.domain.PostgresAddDomainCheckAction",
        lambda d: PostgresAddDomainCheckAction(d, _domain_check(d)),
    )
    register_special_constructor(
        "ddl.domain.PostgresAlterDomainExpression",
        lambda d: PostgresAlterDomainExpression(
            d, name="dom", action=RenameDomainAction(d, "dom2"),
        ),
    )

    # -- expressions that need at least one member --------------------------
    register_special_constructor(
        "advanced_functions.CaseExpression",
        lambda d: CaseExpression(
            d,
            cases=[(_column_predicate(d), Literal(d, 1))],
            else_result=Literal(d, 0),
        ),
    )
    register_special_constructor(
        "advanced_functions.WindowSpecification",
        lambda d: WindowSpecification(d, partition_by=["a"]),
    )
    register_special_constructor(
        "advanced_functions.WindowDefinition",
        lambda d: WindowDefinition(d, "w", WindowSpecification(d, partition_by=["a"])),
    )
    register_special_constructor(
        "advanced_functions.WindowClause",
        lambda d: WindowClause(
            d, [WindowDefinition(d, "w", WindowSpecification(d, partition_by=["a"]))]
        ),
    )
    # An empty options dict is refused by the formatter, so a time-travel clause
    # needs an actual option.
    register_special_constructor(
        "datetime.TemporalOptionsExpression",
        lambda d: TemporalOptionsExpression(d, {"as_of": "2020-01-01"}),
    )

    # -- predicates whose column slot is an expression ----------------------
    # ``column`` is annotated ``Any`` -- it is genuinely either a column or a
    # computed value -- so the guess supplies a string and the formatter calls
    # ``.to_sql()`` on it. ILIKE is the core node: PostgreSQL spells it by
    # overriding ``format_ilike_predicate``, not by redeclaring the kind.
    register_special_constructor(
        "predicates.ILikePredicate",
        lambda d: CoreILIKEExpression(d, Column(d, "a"), "x%"),
    )
    # LIKE carries the operator as a parameter, so the guess's ``"x"`` would
    # become the operator rather than the operand.
    register_special_constructor(
        "predicates.LikePredicate",
        lambda d: LikePredicate(d, LikeOperator.LIKE, Column(d, "a"), Literal(d, "x%")),
    )

    # -- COLLATE needs a collation PostgreSQL actually has -------------------
    # The formatter validates the name against the backend whitelist, and the
    # guess's ``"x"`` is not on it. ``und-x-icu`` needs PostgreSQL 10+, so ``C`` is
    # the one that is legal at every version.
    register_special_constructor(
        "collation.CollateFunction",
        lambda d: CollateFunction(d, Column(d, "a"), "C"),
    )

    # -- MERGE --------------------------------------------------------------
    register_special_constructor(
        "statements.dml.MergeAction",
        lambda d: _merge_action(d),
    )
    register_special_constructor(
        "statements.dml.MergeStatement",
        lambda d: MergeStatement(
            d,
            target_table=_table_obj(d),
            source=NamedRelationRef(d, Table(d, "src")),
            on_condition=_column_predicate(d),
            when_matched=[_merge_action(d)],
        ),
    )

    # -- SQL/XML ------------------------------------------------------------
    register_special_constructor(
        "xml.XMLAttributesExpression",
        lambda d: XMLAttributesExpression(d, [XMLAttribute(Literal(d, "v"), "a")]),
    )
    register_special_constructor(
        "xml.XMLForestExpression",
        lambda d: XMLForestExpression(d, [XMLForestItem(Literal(d, "v"), "a")]),
    )
    register_special_constructor(
        "xml.XMLConcatExpression",
        lambda d: XMLConcatExpression(d, [Literal(d, "a"), Literal(d, "b")]),
    )
    register_special_constructor(
        "xml.XMLTableExpression",
        lambda d: XMLTableExpression(
            d,
            row_pattern=Literal(d, "<r/>"),
            columns=[XMLTableColumn("a", "int")],
        ),
    )

    # -- property graphs ----------------------------------------------------
    def node_table(d):
        return NodeTable(d, "people")

    def edge_table(d):
        return EdgeTableObject(d, "knows")

    def path_pattern(d):
        return graph_mod.PathPattern(
            d, graph_mod.GraphVertex(d, "n", node_table(d))
        )

    register_special_constructor(
        "graph.GraphVertex", lambda d: graph_mod.GraphVertex(d, "n", node_table(d))
    )
    register_special_constructor(
        "graph.GraphEdge", lambda d: graph_mod.GraphEdge(d, "e", edge_table(d))
    )
    register_special_constructor(
        "graph.VertexTable",
        lambda d: graph_mod.VertexTable(d, node_table(d), key_columns=["id"]),
    )
    register_special_constructor(
        "graph.edge_table_definition",
        lambda d: graph_mod.EdgeTableDefinition(d, edge_table(d), ["src"], ["dst"]),
    )
    register_special_constructor(
        "graph.QuantifiedPath",
        lambda d: graph_mod.QuantifiedPath(
            d, graph_mod.GraphEdge(d, "e", edge_table(d)), min_repeats=1, max_repeats=3
        ),
    )
    register_special_constructor("graph.PathPattern", path_pattern)
    register_special_constructor(
        "graph.MatchClause", lambda d: graph_mod.MatchClause(d, path_pattern(d))
    )
    register_special_constructor(
        "graph.ColumnsClause",
        lambda d: graph_mod.ColumnsClause(d, _graph_column(d)),
    )
    register_special_constructor(
        "graph.GraphTableExpression",
        lambda d: graph_mod.GraphTableExpression(
            d,
            graph=PropertyGraph(d, "g"),
            match=graph_mod.MatchClause(d, path_pattern(d)),
            columns=graph_mod.ColumnsClause(d, _graph_column(d)),
        ),
    )
    register_special_constructor(
        "graph.CreatePropertyGraphStatement",
        lambda d: graph_mod.CreatePropertyGraphStatement(
            d, graph=PropertyGraph(d, "g"),
            vertex_tables=[graph_mod.VertexTable(d, node_table(d))],
        ),
    )
    # The formatter accepts "add"/"drop" against "vertex tables"/"edge tables"/
    # "tables"; anything else is refused.
    register_special_constructor(
        "graph.AlterPropertyGraphStatement",
        lambda d: graph_mod.AlterPropertyGraphStatement(
            d,
            graph=PropertyGraph(d, "g"),
            action="add",
            target="vertex tables",
            vertex_tables=[graph_mod.VertexTable(d, node_table(d))],
        ),
    )
    register_special_constructor(
        "graph.DropPropertyGraphStatement",
        lambda d: graph_mod.DropPropertyGraphStatement(d, graph=PropertyGraph(d, "g")),
    )

    # -- types --------------------------------------------------------------
    # The element type must be bound to the dialect, because ``to_sql`` dispatches
    # on it through its own dialect.
    register_special_constructor(
        "types.array.ArrayType",
        lambda d: ArrayType(d, element_type=VarCharType(d, 10)),
    )
    register_special_constructor(
        "types.PostgresArrayType",
        lambda d: PostgresArrayType(d, element_type=VarCharType(d, 10)),
    )
    register_special_constructor(
        "types.enum_.EnumType", lambda d: EnumType(d, values=["sad", "ok"])
    )
    # ``raw`` is keyword-only and names the SQL spelling, so the guess supplies
    # nothing and the type declares no name at all.
    register_special_constructor(
        "types.custom.CustomType", lambda d: CustomType(d, raw="mytype")
    )

    # -- core nodes with a required slot the guess fills with a string -------
    # ``op`` is a closed word set and refuses the placeholder; the two datetime
    # nodes take a field name the guess turns into a bare string; ``target`` /
    # ``start`` must be values, not text; ``table`` must be the relation object.
    register_special_constructor(
        "predicates.ComparisonPredicate",
        lambda d: ComparisonPredicate(d, "=", Column(d, "a"), Column(d, "b")),
    )
    register_special_constructor(
        "functions.string.SubstringExpression",
        lambda d: SubstringExpression(d, Literal(d, 1), Literal(d, 2)),
    )
    # ``count`` is compared with ``<`` when it is a stated literal, so a string
    # placeholder is refused at construction. A real integer is what the
    # grammar wants anyway.
    register_special_constructor(
        "functions.string.LeftFunction",
        lambda d: LeftFunction(d, Literal(d, "abc"), Literal(d, 2)),
    )
    register_special_constructor(
        "functions.string.RightFunction",
        lambda d: RightFunction(d, Literal(d, "abc"), Literal(d, 2)),
    )
    register_special_constructor(
        "datetime.ExtractExpression",
        lambda d: ExtractExpression(d, DateTimeField.YEAR, Column(d, "a")),
    )
    register_special_constructor(
        "datetime.ExtractFieldExpression",
        lambda d: ExtractFieldExpression(d, DateTimeField.YEAR, Column(d, "a")),
    )
    register_special_constructor(
        "datetime._ExtractBase",
        lambda d: _ExtractBase(d, DateTimeField.YEAR, Column(d, "a")),
    )
    register_special_constructor(
        "datetime.DatePartExpression",
        lambda d: DatePartExpression(d, DateTimeField.YEAR, Column(d, "a")),
    )
    register_special_constructor(
        "datetime.DateTruncExpression",
        lambda d: DateTruncExpression(d, DateTimeField.YEAR, Column(d, "a")),
    )
    register_special_constructor(
        "datetime.IntervalExpression",
        lambda d: IntervalExpression(d, 7, IntervalUnit.DAY),
    )
    register_special_constructor(
        "datetime.DateTimeDiffExpression",
        lambda d: DateTimeDiffExpression(
            d, IntervalUnit.DAY, Column(d, "a"), Column(d, "b")
        ),
    )
    register_special_constructor(
        "datetime.DateTimeSubtractExpression",
        lambda d: DateTimeSubtractExpression(
            d, Column(d, "a"), IntervalExpression(d, 1, IntervalUnit.DAY)
        ),
    )
    register_special_constructor(
        "datetime.DateTimeAddExpression",
        lambda d: DateTimeAddExpression(
            d, Column(d, "a"), IntervalExpression(d, 1, IntervalUnit.DAY)
        ),
    )
    register_special_constructor(
        "graph.EdgeTableDefinition",
        lambda d: graph_mod.EdgeTableDefinition(
            d,
            table=EdgeTableObject(d, "e"),
            source_key=["s"],
            destination_key=["d"],
        ),
    )
    # A check condition must be a predicate, and DDL takes no bind parameters,
    # so the values inside it are inline literals rather than bound ones.
    register_special_constructor(
        "ddl_table.ColumnCheckConstraint",
        lambda d: ColumnCheckConstraint(d, _inline_column_predicate(d)),
    )
    register_special_constructor(
        "ddl_table.TableCheckConstraint",
        lambda d: TableCheckConstraint(d, _inline_column_predicate(d)),
    )
    register_special_constructor(
        "ddl_table.ColumnGeneratedConstraint",
        lambda d: ColumnGeneratedConstraint(d, _inline_column_predicate(d)),
    )
    # A declared-result annotation wraps a value; the formatter also hoists an
    # alias from it, so the node it wraps has to be the aliasing kind. A
    # ``Literal`` has no ``alias`` and is not what a projection annotates.
    for name in (
        "ArrayAnnotation",
        "BinaryAnnotation",
        "BooleanAnnotation",
        "IntegerAnnotation",
        "JSONAnnotation",
        "NumberAnnotation",
        "TemporalAnnotation",
        "TextAnnotation",
        "UUIDAnnotation",
        "XMLAnnotation",
        "_Annotation",
    ):
        register_special_constructor(
            f"declared.{name}",
            lambda d, _n=name: getattr(declared, _n)(d, Column(d, "a")),
        )

    # -- the four value wrappers --------------------------------------------
    # Each wraps a FunctionCall and renders it verbatim, so the introspective
    # guess must hand it a real call: a string left in ``call`` reaches
    # ``to_sql`` as a string and raises AttributeError there instead of being
    # refused at the door. The factories below are the ones the classes own.
    register_special_constructor(
        "values.PostgresNetworkValueExpression",
        lambda d: PostgresNetworkValueExpression(d, pg_network.inet_client_addr(d)),
    )
    register_special_constructor(
        "values.PostgresRangeValueExpression",
        lambda d: PostgresRangeValueExpression(
            d,
            pg_range.range_contains(d, Column(d, "v"), Literal(d, 3)),
            element_type=None,
        ),
    )
    register_special_constructor(
        "values.PostgresHstoreValueExpression",
        # A dict is what ``hstore_literal`` casts to ``hstore``; handing it a
        # ``Literal`` would have it wrap a node in a second ``Literal``, which
        # has no value kind and therefore no artifact.
        lambda d: PostgresHstoreValueExpression(
            d, pg_hstore.hstore_akeys(d, pg_hstore.hstore_literal(d, {"a": "1"}))
        ),
    )
    register_special_constructor(
        "values.PostgresLtreeValueExpression",
        lambda d: PostgresLtreeValueExpression(
            d,
            pg_ltree.ltree_ancestor(
                d, Column(d, "v"), pg_ltree.ltree_literal(d, "a.b")
            ),
        ),
    )


register_specials()


# ---------------------------------------------------------------------------
# Lists that cannot grow or shrink silently
# ---------------------------------------------------------------------------

#: Classes the generic introspective constructor cannot build.
#:
#: Each entry is a real coverage gap, named here so it is visible rather than
#: lost. ``test_unconstructible_list_is_exact`` pins the tuple in both
#: directions, so a class that gains a constructor fails here until this entry is
#: removed, and a class that starts failing to build fails here too -- neither can
#: become a quiet skip.
#:
#: Registering a constructor is how an entry is retired.
#:
#: Sorted, because the integrity test compares this against a sorted tuple of what
#: it observes.
UNCONSTRUCTIBLE = (
    # Every class that gets here is a real coverage gap: the matrix cannot assert
    # anything about a statement it cannot build, and the skip it becomes looks
    # exactly like the skip a legitimately unsupported statement earns. Register a
    # constructor for any of them; that is the way to retire an entry.
    #
    # There are none left. That is not a claim about the classes -- it is what
    # ``test_unconstructible_list_is_exact`` observes right now, and it is pinned
    # in both directions so the first class that needs an exemption fails CI.
    #
    # Two are exempt, each for the same underlying reason: a required value
    # sits behind a defaulted parameter, and the filler skips defaulted ones.
    # (``CustomType`` used to be here for the same reason -- ``raw`` was
    # keyword-only -- and now has a constructor, so it is covered instead.)
    #
    # UUID constant. `which` is required, but the filler's "x" is refused at
    # construction because __init__ accepts only 'nil' and 'max'.
    "rhosocial.activerecord.backend.expression.uuid.UUIDConstantExpression",
    # POSTGRES ENUM COLUMN. type_name, schema and values all default to None, so
    # the filler supplies none of them and the type declares neither its name nor
    # its members.
    "rhosocial.activerecord.backend.impl.postgres.expression.types.PostgresEnumColumnType",
)

#: Classes that construct but cannot render, for a reason belonging to their own
#: tree or to PostgreSQL's feature set rather than to a defect. Each entry pins
#: the exception type and a message fragment, so a class that starts failing for
#: a *different* reason fails here.
#:
#: Three entries pin ``UnsupportedFeatureError`` itself, the type
#: :func:`assert_sql_roundtrip_classified` allows unconditionally; what earns them
#: a place here is the fragment, which names the specific formatter that is
#: missing. See the module docstring for why they were not simply deleted.
_NO_FORMATTER = "does not declare its dialect formatting method name"


def _no_such_formatter(formatter_name: str) -> str:
    """The message fragment ``to_sql()`` reports when the dialect lacks a formatter.

    ``BaseExpression.to_sql()`` resolves ``format_method`` against the bound
    dialect and, when the name resolves to nothing, raises
    ``UnsupportedFeatureError`` naming the method -- the same way every other
    capability gap in the tree is reported, and the exception both migration
    runners already catch. That lookup used to let the missing attribute escape
    as a bare ``AttributeError``, which is a statement about Python's object
    model rather than about the engine, and so said nothing about *why* the
    statement cannot be rendered.

    Taking the formatter name as an argument keeps each pin naming the one
    method its class actually dispatches on. A shared fragment would let
    ``TableSource``, ``Pivot`` and ``Unpivot`` start failing
    for each other's reason and still pass.
    """
    return f"does not support the '{formatter_name}' statement"

LEGITIMATE_NON_RENDERS = {
    # ---- a marker class that exists in order to refuse ------------------
    # ``_PostgresUnsupportedDomainAction`` is constructed precisely so an ALTER DOMAIN
    # action PostgreSQL cannot express gets a message naming the action. The
    # ValueError is the designed behaviour, not a defect -- and it is the only
    # thing this class does.
    "rhosocial.activerecord.backend.impl.postgres.expression.ddl.domain."
    "_PostgresUnsupportedDomainAction": (
        ValueError,
        "Unsupported ALTER DOMAIN action",
    ),

    # ---- a core type PostgreSQL spells another way ------------------------
    # Core's generic ENUM type is for engines that dispatch on the name ``enum``.
    # PostgreSQL has no such type -- ``CREATE TYPE ... AS ENUM`` goes through
    # ``PostgresEnumType`` -- so there is no ``format_data_type_enum`` to reach.
    "rhosocial.activerecord.backend.expression.types.enum_.EnumType": (
        TypeError,
        "does not support the generic type 'enum'",
    ),

    # ---- bases that name an expression category, not a renderable thing ----
    # Each of these is a base class that deliberately declares no
    # ``format_method``, so ``to_sql()`` reports that there is nothing to
    # dispatch. They are not ``inspect.isabstract`` -- they are concrete enough to
    # construct -- so the walk keeps them, and each is pinned to its exact message
    # so a base that started rendering fails here instead of passing quietly.
    "rhosocial.activerecord.backend.expression.bases.SQLPredicate": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    "rhosocial.activerecord.backend.expression.bases.SQLValue": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    # The roots of the object tree. Each concrete object overrides
    # ``format_method`` with its own ``format_<kind>_object``; the base names only
    # what every catalogue object has in common.
    "rhosocial.activerecord.backend.expression.objects.base.SchemaObject": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    "rhosocial.activerecord.backend.expression.objects.relation.RelationObject": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    "rhosocial.activerecord.backend.expression.objects.routine.RoutineObject": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    "rhosocial.activerecord.backend.expression.objects.type_.TypeObject": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    # Expression-category bases whose concrete members each name their own
    # formatter: an ALTER TABLE action, an INSERT row source, a temporal value, an
    # introspection query.
    "rhosocial.activerecord.backend.expression.statements.ddl_alter.AlterTableAction": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    "rhosocial.activerecord.backend.expression.statements.dml.InsertDataSource": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    "rhosocial.activerecord.backend.expression.transaction.TransactionExpression": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    "rhosocial.activerecord.backend.expression.datetime._TemporalValue": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_table.ColumnClause": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_table."
    "ColumnConstraintBranch": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    "rhosocial.activerecord.backend.expression.statements.ddl_table."
    "TableConstraintBranch": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    "rhosocial.activerecord.backend.expression.introspection.IntrospectionExpression": (
        NotImplementedError,
        _NO_FORMATTER,
    ),
    # The root of the type tree. Every concrete type declares its own generic
    # ``name``, which is what ``format_data_type`` dispatches on; the root declares
    # none, so there is nothing to dispatch. TypeError, not
    # UnsupportedFeatureError, because the class is incomplete rather than the
    # dialect being unable.
    "rhosocial.activerecord.backend.expression.types._base.DataType": (
        TypeError,
        "does not support the generic type None",
    ),

    # ---- the root of the row-source tree ---------------------------------
    # The root of the row-source tree. No concrete source renders through
    # ``format_table_source``; each overrides ``format_method``. Rendering the base
    # would mean inventing SQL for an object that carries nothing but an alias.
    #
    # Pinned as a dialect capability gap rather than as the ``AttributeError`` the
    # lookup used to leak, because that is what it is: PostgreSQL declares no
    # ``format_table_source`` and is not going to. ``UnsupportedFeatureError`` is
    # also what :func:`assert_sql_roundtrip_classified` treats as always allowed,
    # so this entry now adds the message fragment on top of the type rather than
    # substituting for it -- which is why it stays here instead of being deleted.
    "rhosocial.activerecord.backend.expression.sources.base.TableSource": (
        UnsupportedFeatureError,
        _no_such_formatter("format_table_source"),
    ),

    # ---- core types PostgreSQL has no formatter for ----------------------
    # Core defines these two generic types for engines that spell them VARBINARY
    # or BINARY. PostgreSQL has no such type -- bytea is modelled separately -- so
    # there is no ``format_data_type_binary`` to dispatch to. A dialect gap, not a
    # defect: the dialect is saying it cannot render them.
    "rhosocial.activerecord.backend.expression.types.binary.BinaryType": (
        TypeError,
        "does not support the generic type 'binary'",
    ),
    "rhosocial.activerecord.backend.expression.types.binary.VarBinaryType": (
        TypeError,
        "does not support the generic type 'varbinary'",
    ),

    # ---- a core base whose formatter reads a field it never declares -------
    # ``_Annotation`` renders through ``format_declared_result``, which hoists
    # the wrapped node's alias and the annotation's own. Core's ``_Annotation``
    # declares ``source`` only, so the base raises ``AttributeError`` for a
    # field that does not exist -- on every backend, not just this one. It is
    # pinned rather than skipped because the *type* is the finding.
    "rhosocial.activerecord.backend.expression.declared._Annotation": (
        AttributeError,
        "has no attribute 'alias'",
    ),

    # ---- SQL standard pivot, which is not PostgreSQL syntax --------------
    # Core models the SQL:2003 PIVOT / UNPIVOT for engines that spell it that way.
    # PostgreSQL has no PIVOT keyword, so the dialect declares no
    # ``format_pivot``. Typing PIVOT is done with a ``CASE`` /
    # ``generate_series`` plan, not with this expression, so the absence is a fact
    # about the engine rather than a gap in this backend. Each entry names its own
    # formatter so the two cannot pass for one another.
    "rhosocial.activerecord.backend.expression.pivot.Pivot": (
        UnsupportedFeatureError,
        _no_such_formatter("format_pivot"),
    ),
    "rhosocial.activerecord.backend.expression.pivot.Unpivot": (
        UnsupportedFeatureError,
        _no_such_formatter("format_unpivot"),
    ),
}


# ---------------------------------------------------------------------------
# The dispatch gap: a missing formatter is a decision, not an omission
# ---------------------------------------------------------------------------
#
# ``to_sql()`` reports a dialect that names no formatter for a class through
# the same ``UnsupportedFeatureError`` a capability probe uses, so the type
# alone cannot say "this backend never wired the feature in". The frame the
# dispatch puts around the method name can, and it is the only place in the
# tree that opens it -- see :func:`_dispatched_formatter`.

#: The two ends of the frame ``to_sql()`` puts around a formatting method name
#: in the ``feature_name`` of the ``UnsupportedFeatureError`` it raises for a
#: dialect that has no such formatter.
_DISPATCH_FEATURE_PREFIX = "the '"
_DISPATCH_FEATURE_SUFFIX = "' statement"


def _dispatched_formatter(exc):
    """The formatting method *exc* says the dialect does not have, or ``None``.

    ``None`` means *exc* is not the dispatch reporting a missing formatter --
    it is a probe inside a formatter that refused a feature, which is the
    ``"unsupported"`` branch and needs no row anywhere.

    Args:
        exc: An :class:`UnsupportedFeatureError` raised out of ``to_sql()``.

    Returns:
        The method name out of the ``feature_name`` frame, or ``None`` when
        *exc* came from a probe rather than from the dispatch.
    """
    feature = exc.feature_name
    if not feature.startswith(_DISPATCH_FEATURE_PREFIX):
        return None
    if not feature.endswith(_DISPATCH_FEATURE_SUFFIX):
        return None
    method = feature[len(_DISPATCH_FEATURE_PREFIX) : len(feature) - len(_DISPATCH_FEATURE_SUFFIX)]
    return method if method else None


#: Formatters PostgreSQL declares no implementation of, grouped by the feature
#: that is absent, each group naming every method the matrix's classes need.
#:
#: A class needing one of these fails with ``UnsupportedFeatureError`` naming
#: the method it wanted, and :func:`assert_sql_roundtrip_classified` asserts
#: that method is a row here. :meth:`TestMatrixIntegrity.\
#: test_unmodelled_formatter_list_is_exact` pins the table in both directions:
#: a formatter that starts existing moves its classes out of the table and the
#: pin fails until the entry goes, and a class that starts needing a method
#: outside the table fails outright instead of passing as "unsupported".
#:
#: These are not defects: they are features PostgreSQL does not have, which
#: the shared renderer reports by naming the method it could not find. The
#: grouping is the point -- "N methods" says nothing, "no PIVOT keyword, and
#: the row-source root is never rendered" says what the dialect is.
UNMODELLED_FORMATTERS = {
    "SQL:2003 PIVOT / UNPIVOT (PostgreSQL has no PIVOT keyword; pivoting is "
    "spelled as CASE + GROUP BY, or crosstab from the tablefunc extension)": (
        "format_pivot",
        "format_unpivot",
    ),
    "the TableSource root (every concrete row source overrides its formatter; "
    "the root carries nothing but an alias and is never rendered)": ("format_table_source",),
}


def _unmodelled_methods():
    """The flat set of formatter names in :data:`UNMODELLED_FORMATTERS`."""
    return {method for methods in UNMODELLED_FORMATTERS.values() for method in methods}


# ---------------------------------------------------------------------------
# The codec gap: classes whose artifact the *core* codec cannot write or read
# ---------------------------------------------------------------------------
# Every entry here is a defect in core, confirmed against the core tree of this
# baseline, and none of them can be fixed from a backend:
#
# * the XML codec writes a ``(str, Enum)`` parameter with ``str()`` (which is
#   ``Enum.__str__``, so ``"DateTimeField.YEAR"``) while the JSON codec writes
#   the value; the constructor then refuses the enum's *name*;
# * the JSON/dict/XML encoder refuses a *dataclass* parameter (``XMLAttribute``,
#   ``XMLForestItem``, ``XMLTableColumn``, ``ReferentialAction``,
#   ``TriggerEvent``, ``ViewOptions``), although §10 requires every parameter
#   to travel;
# * ``Literal`` nested inside another node's payload has no value kind when the
#   serializer meets it through ``Literal.value`` rather than through the node
#   (``XMLParseExpression.document_type`` aside, the hstore factory wrapped a
#   literal in a literal).
#
# They are pinned rather than skipped: the *exact* channel and the *exact*
# exception type are asserted, so a class that starts failing differently fails
# here, and one that starts passing fails here too (the entry is stale).
# Anything not listed must round-trip everywhere. This table is the list the
# core agent needs; it is not a licence to widen the matrix's blind spot.
#: The nodes whose *only* codec gap is the XML codec writing an enum member's
#: name -- those are the ones where asserting SQL on the XML round-trip would
#: assert the mangling rather than the node.
_XML_ENUM_NAMES = frozenset(
    {
        "rhosocial.activerecord.backend.expression.datetime.DatePartExpression",
        "rhosocial.activerecord.backend.expression.datetime.DateTimeAddExpression",
        "rhosocial.activerecord.backend.expression.datetime.DateTimeDiffExpression",
        "rhosocial.activerecord.backend.expression.datetime.DateTimeSubtractExpression",
        "rhosocial.activerecord.backend.expression.datetime.DateTruncExpression",
        "rhosocial.activerecord.backend.expression.datetime.ExtractExpression",
        "rhosocial.activerecord.backend.expression.datetime.ExtractFieldExpression",
        "rhosocial.activerecord.backend.expression.datetime.IntervalExpression",
        "rhosocial.activerecord.backend.expression.datetime._ExtractBase",
        "rhosocial.activerecord.backend.expression.xml.XMLParseExpression",
        "rhosocial.activerecord.backend.expression.xml.XMLSerializeExpression",
    }
)

#: Nodes whose artifact carries the parameters but whose decoded form will not
#: *render*: the codec flattened an enum to its word, and core's formatter reads
#: ``.value`` unconditionally. The parameters are intact -- the value is the same
#: word -- so this is a render-side defect in core's formatter, not a loss.
#: ``(fqn, channel)`` pairs, checked in the integrity test below.
_CODEC_RENDER_GAPS = frozenset(
    {
        ("rhosocial.activerecord.backend.expression.xml.XMLParseExpression", "dict"),
        ("rhosocial.activerecord.backend.expression.xml.XMLParseExpression", "json"),
        ("rhosocial.activerecord.backend.expression.xml.XMLParseExpression", "xml"),
        ("rhosocial.activerecord.backend.expression.xml.XMLSerializeExpression", "dict"),
        ("rhosocial.activerecord.backend.expression.xml.XMLSerializeExpression", "json"),
        ("rhosocial.activerecord.backend.expression.xml.XMLSerializeExpression", "xml"),
    }
)

_CODEC_GAPS = {
    # -- the XML codec spells an enum by its member name -------------------
    "rhosocial.activerecord.backend.expression.datetime.DatePartExpression": {
        "xml": ValueError,
    },
    "rhosocial.activerecord.backend.expression.datetime.DateTimeAddExpression": {
        "xml": ValueError,
    },
    "rhosocial.activerecord.backend.expression.datetime.DateTimeDiffExpression": {
        "xml": ValueError,
    },
    "rhosocial.activerecord.backend.expression.datetime.DateTimeSubtractExpression": {
        "xml": ValueError,
    },
    "rhosocial.activerecord.backend.expression.datetime.DateTruncExpression": {
        "xml": ValueError,
    },
    "rhosocial.activerecord.backend.expression.datetime.ExtractExpression": {
        "xml": ValueError,
    },
    "rhosocial.activerecord.backend.expression.datetime.ExtractFieldExpression": {
        "xml": ValueError,
    },
    "rhosocial.activerecord.backend.expression.datetime.IntervalExpression": {
        "xml": ValueError,
    },
    "rhosocial.activerecord.backend.expression.datetime._ExtractBase": {
        "xml": ValueError,
    },
    # ``document_type`` has no ``__str__`` override, so the XML codec writes the
    # member's *name* and the node comes back with a name instead of its word.
    "rhosocial.activerecord.backend.expression.xml.XMLParseExpression": {
        "xml": "document_type",
    },
    "rhosocial.activerecord.backend.expression.xml.XMLSerializeExpression": {
        "xml": "document_type",
    },
    # -- a dataclass parameter has no language-neutral encoding ------------
    "rhosocial.activerecord.backend.expression.statements.ddl_table.ForeignKeyConstraint": {
        "dict": TypeError,
        "json": TypeError,
        "xml": TypeError,
    },
    "rhosocial.activerecord.backend.expression.statements.ddl_trigger.CreateTriggerStatement": {
        "dict": TypeError,
        "json": TypeError,
        "xml": TypeError,
    },
    "rhosocial.activerecord.backend.expression.statements.ddl_view.CreateViewStatement": {
        "dict": TypeError,
        "json": TypeError,
        "xml": TypeError,
    },
    "rhosocial.activerecord.backend.expression.xml.XMLAttributesExpression": {
        "dict": TypeError,
        "json": TypeError,
        "xml": TypeError,
    },
    "rhosocial.activerecord.backend.expression.xml.XMLForestExpression": {
        "dict": TypeError,
        "json": TypeError,
        "xml": TypeError,
    },
    "rhosocial.activerecord.backend.expression.xml.XMLTableExpression": {
        "dict": TypeError,
        "json": TypeError,
        "xml": TypeError,
    },
    # ``GraphColumn`` / ``GeneratedColumnType`` are host objects, not values:
    # the artifact has no shape for them at all.
    "rhosocial.activerecord.backend.expression.graph.ColumnsClause": {
        "dict": TypeError,
        "json": TypeError,
        "xml": TypeError,
    },
    "rhosocial.activerecord.backend.expression.graph.GraphTableExpression": {
        "dict": TypeError,
        "json": TypeError,
        "xml": TypeError,
    },
    "rhosocial.activerecord.backend.expression.statements.ddl_table.ColumnGeneratedConstraint": {
        "dict": TypeError,
        "json": TypeError,
        "xml": TypeError,
    },
    # ``CreateTableCloneMode`` is a plain ``Enum`` (no ``str`` base, no
    # ``__str__``), so the word it travels with is refused by the constructor.
    "rhosocial.activerecord.backend.expression.statements.ddl_table.CreateTableCloneStatement": {
        "json": TypeError,
        "xml": TypeError,
    },
}


def _channels_that_lose_params(fqn, instance, dialect):
    """The encodings whose round-trip does not restore ``get_params()``.

    Returns ``{channel: why}``. An empty dict means the class round-trips
    everywhere, which every class in the matrix must do -- there is no exemption
    list, because there has never been a reason for one.

    A *decode* that raises is reported too, and that is deliberate: a channel
    that cannot read its own artifact has lost the parameters more thoroughly
    than one that reads them back changed. It used to escape the fixture and
    abort the whole matrix run, which hid every later class behind the first
    one that failed.
    """
    original = instance.get_params()
    lost = {}
    for channel, enc, dec in (
        ("dict", serialize, deserialize),
        ("json", serialize_json, deserialize_json),
        ("xml", serialize_xml, deserialize_xml),
    ):
        try:
            decoded = dec(enc(instance), dialect)
        except Exception as exc:  # noqa: BLE001
            lost[channel] = f"{type(exc).__name__}: {exc}"
            continue
        try:
            assert_params_equal(decoded.get_params(), original, fqn)
        except AssertionError as exc:
            lost[channel] = str(exc).splitlines()[0]
    return lost


# ---------------------------------------------------------------------------
# The local SQL assertion: classify the outcome instead of swallowing it
# ---------------------------------------------------------------------------


def assert_sql_roundtrip_classified(fqn, instance, dialect):
    """Assert an expression's SQL survives the round-trip, or say precisely why not.

    Four outcomes, each asserted:

    * **renders** -- every encoding that preserved the parameters must restore
      byte-identical SQL *and* byte-identical bind parameters.
    * ``UnsupportedFeatureError`` -- the dialect does not model the feature.
      Asserted as exactly that type, so a formatter raising it for an unrelated
      reason is still visible as that type rather than as a pass. Checked first,
      before :data:`LEGITIMATE_NON_RENDERS`, so a class pinned with that same
      type is classified here and its fragment is enforced by
      ``test_pinned_non_render_really_does_not_render`` instead.
    * ``UnsupportedFeatureError`` naming a formatting method -- the dispatch
      found nothing, which is a wiring fact about PostgreSQL rather than a
      capability answer. The method must be in
      :data:`UNMODELLED_FORMATTERS`; the table entry is the reason.
    * a member of :data:`LEGITIMATE_NON_RENDERS` -- unrenderable by design,
      asserted as its exact type *and* message fragment.
    * **anything else** -- a failure naming the class and the exception.

    Returns a short string naming the branch taken.

    Raises:
        AssertionError: On a round-trip mismatch, on an unexpected exception type,
            or when a class's rendering outcome changed.
    """
    try:
        expected_sql, expected_params = instance.to_sql()
    except UnsupportedFeatureError as exc:
        assert type(exc) is UnsupportedFeatureError, fqn
        method = _dispatched_formatter(exc)
        if method is not None:
            assert method in _unmodelled_methods(), (
                f"{fqn}: to_sql() reported that {exc.dialect_name} declares no "
                f"{method!r}, which is not in UNMODELLED_FORMATTERS.\n"
                f"  Either PostgreSQL now needs that formatter -- in which case "
                f"the class should render and this entry should go -- or the "
                f"feature is absent and the method belongs in that table with "
                f"its reason."
            )
            return "unmodelled"
        return "unsupported"
    except Exception as exc:
        if fqn not in LEGITIMATE_NON_RENDERS:
            raise AssertionError(
                f"{fqn}: to_sql() raised {type(exc).__name__}, which is neither a "
                f"render nor a classified non-render, and this is a defect.\n"
                f"  UnsupportedFeatureError means the dialect lacks the feature and "
                f"is always allowed.\n"
                f"  A class that cannot render for a reason belonging to its own "
                f"tree or to PostgreSQL's feature set belongs in "
                f"LEGITIMATE_NON_RENDERS.\n"
                f"  Exception: {exc}"
            ) from exc
        expected_type, fragment = LEGITIMATE_NON_RENDERS[fqn]
        assert type(exc) is expected_type, (
            f"{fqn}: LEGITIMATE_NON_RENDERS pins this class as a legitimate "
            f"non-render raising {expected_type.__name__}, but it raised "
            f"{type(exc).__name__}: {exc}"
        )
        assert fragment in str(exc), (
            f"{fqn}: expected {expected_type.__name__} and was expected to say "
            f"{fragment!r}, but it said: {exc}"
        )
        return "non-render"

    gaps = _CODEC_GAPS.get(fqn, {})
    for channel, enc, dec in (
        ("dict", serialize, deserialize),
        ("json", serialize_json, deserialize_json),
        ("xml", serialize_xml, deserialize_xml),
    ):
        if channel in gaps or (fqn, channel) in _CODEC_RENDER_GAPS:
            # The artifact cannot carry this node's parameters (``_CODEC_GAPS``),
            # or it carries them but core's formatter cannot read them back
            # (``_CODEC_RENDER_GAPS``). Both are pinned; asserting SQL on a tree
            # the codec mangled would assert the mangling.
            continue
        decoded = dec(enc(instance), dialect)
        decoded_sql, decoded_params = decoded.to_sql()
        assert decoded_sql == expected_sql, (
            f"{fqn}: {channel} round-trip changed the SQL.\n"
            f"  original: {expected_sql!r}\n"
            f"  {channel}: {decoded_sql!r}"
        )
        assert list(decoded_params) == list(expected_params), (
            f"{fqn}: {channel} round-trip changed the bind parameters.\n"
            f"  original: {expected_params!r}\n"
            f"  {channel}: {decoded_params!r}"
        )
    return "rendered"


@pytest.fixture(
    params=[fqn for fqn in sorted(REGISTERED)], ids=sorted(REGISTERED)
)
def expr_case(request, postgres_dialect):
    fqn = request.param
    cls = REGISTERED[fqn]
    instance, source = make_instance(cls, postgres_dialect)
    if instance is None:
        assert fqn in UNCONSTRUCTIBLE, (
            f"{fqn} cannot be built by the generic constructor ({source}) and is "
            f"not in UNCONSTRUCTIBLE. Either register a special constructor for "
            f"it or add it to the tuple with a reason -- do not let it disappear "
            f"into a skip."
        )
        pytest.skip(
            f"{fqn}: pinned in UNCONSTRUCTIBLE, cannot be constructed ({source})"
        )
    return fqn, instance


class TestExpressionRoundtripAll:
    """All constructible expression classes round-trip through all encodings."""

    def test_get_params_roundtrip_across_encodings(self, expr_case, postgres_dialect):
        fqn, instance = expr_case
        lost = _channels_that_lose_params(fqn, instance, postgres_dialect)
        pinned = _CODEC_GAPS.get(fqn, {})
        assert sorted(lost) == sorted(pinned), (
            f"{fqn} lost {sorted(lost)} but _CODEC_GAPS pins {sorted(pinned)}.\n"
            + "\n".join(f"  {channel}: {why}" for channel, why in lost.items())
        )
        for channel, expected in pinned.items():
            marker = expected if isinstance(expected, str) else expected.__name__
            assert marker in lost[channel], (
                f"{fqn}: the {channel} gap is pinned as {marker}, "
                f"but it failed as: {lost[channel]}"
            )

    def test_to_sql_roundtrip_classified(self, expr_case, postgres_dialect):
        """A render must survive the round-trip; a non-render must be classified."""
        fqn, instance = expr_case
        assert_sql_roundtrip_classified(fqn, instance, postgres_dialect)


class TestMatrixIntegrity:
    """Guards on the matrix and its lists, so neither can quietly change."""

    def test_unconstructible_list_is_exact(self, postgres_dialect):
        """Pin the unconstructible tuple against what the constructor really skips.

        Two directions are checked. A class named here that now builds has gained a
        constructor and the entry is stale; a class that fails to build without
        being named would become a silent skip. Both fail here.
        """
        dialect = postgres_dialect
        actual = tuple(
            sorted(
                fqn
                for fqn in REGISTERED
                if make_instance(REGISTERED[fqn], dialect)[0] is None
            )
        )
        assert actual == tuple(sorted(UNCONSTRUCTIBLE)), (
            "the set of expression classes the generic constructor cannot build "
            "changed.\n"
            f"  now skipped but not named: "
            f"{sorted(set(actual) - set(UNCONSTRUCTIBLE))}\n"
            f"  named but now built: "
            f"{sorted(set(UNCONSTRUCTIBLE) - set(actual))}\n"
            "Each new entry needs a reason in the comment above UNCONSTRUCTIBLE."
        )

    def test_unconstructible_entries_are_real_classes(self):
        """Every entry names a class that was actually collected.

        A typo in the tuple would otherwise exempt nothing while still reading as
        a deliberate decision.
        """
        unknown = set(UNCONSTRUCTIBLE) - set(REGISTERED)
        assert not unknown, (
            f"UNCONSTRUCTIBLE names classes that were not registered: {sorted(unknown)}"
        )

    def test_legitimate_non_renders_are_real_classes(self):
        """Every pinned non-render names a class that was actually collected."""
        unknown = set(LEGITIMATE_NON_RENDERS) - set(REGISTERED)
        assert not unknown, (
            f"LEGITIMATE_NON_RENDERS names classes that were not registered: "
            f"{sorted(unknown)}"
        )

    def test_pinned_non_render_really_does_not_render(self, postgres_dialect):
        """Each pinned entry still raises what it claims, for the stated reason.

        Without this, an entry could sit in the dict for a class that renders
        perfectly well, and the matrix would be asserting nothing about it.
        """
        for fqn, (expected_type, fragment) in LEGITIMATE_NON_RENDERS.items():
            instance, source = make_instance(REGISTERED[fqn], postgres_dialect)
            assert instance is not None, (
                f"{fqn} is pinned as a non-render but could not be constructed "
                f"({source})"
            )
            with pytest.raises(expected_type) as exc_info:
                instance.to_sql()
            assert fragment in str(exc_info.value), (
                f"{fqn}: expected the message to mention {fragment!r}, got: "
                f"{exc_info.value}"
            )

    def test_unmodelled_formatter_list_is_exact(self, postgres_dialect):
        """Pin the unmodelled-formatter table against what the dialect lacks.

        Observed rather than assumed: for each class the matrix covers, either
        it renders or it fails, and every dispatch refusal is attributed to the
        method it named. Then the table is compared with the set observed, in
        both directions, so:

        * a method in the table that PostgreSQL now implements fails here,
          because its classes render and are no longer attributed to it --
          which is the moment to delete the entry rather than leave a lie in
          the table;
        * a class that starts needing a method outside the table fails the
          per-class assertion in :func:`assert_sql_roundtrip_classified`
          instead of being absorbed into "unsupported".

        This is the regression test for the pad/trim incident. Core turned
        ``trim``/``lpad``/``rpad``/``repeat`` into dedicated nodes, and a
        backend that mixed in no formatter for one of them would have shipped
        a silent ``UnsupportedFeatureError`` on every ``.lpad(...)`` while this
        matrix stayed green, because the blanket ``unsupported`` branch
        allowed the dispatch's refusal exactly as it allows a probe's. The
        dispatch names the method it wanted; this test makes that name
        load-bearing on both sides of the table.
        """
        observed = set()
        for fqn in sorted(REGISTERED):
            instance, source = make_instance(REGISTERED[fqn], postgres_dialect)
            if instance is None:
                continue
            try:
                instance.to_sql()
            except UnsupportedFeatureError as exc:
                method = _dispatched_formatter(exc)
                if method is not None:
                    observed.add(method)
                continue
            except Exception:
                continue

        declared = _unmodelled_methods()
        assert not (observed - declared), (
            "classes need formatters that are not in UNMODELLED_FORMATTERS: "
            f"{sorted(observed - declared)}. Add each with the feature that is "
            f"absent and why -- or mix the formatter in, in which case the "
            f"classes render and the entry is not needed."
        )
        assert not (declared - observed), (
            "UNMODELLED_FORMATTERS names formatters no class actually needs: "
            f"{sorted(declared - observed)}. PostgreSQL may have gained one of "
            f"these, in which case the classes needing it now render and the "
            f"entry should go."
        )

    def test_matrix_covers_both_packages_completely(self):
        """The matrix covers every concrete class both packages define.

        Re-walked here rather than trusting the module-level collection, so a class
        that appeared after import is caught. The package walk is used rather
        than the registry because the registry also holds whatever backends
        other test modules happened to import.
        """
        expected = set(_collect_matrix_classes())
        stray = [
            fqn
            for fqn in REGISTERED
            if not fqn.startswith((f"{PG_EXPR_PKG}.", f"{CORE_EXPR_PKG}."))
        ]
        assert not stray, f"classes outside the two packages are in the matrix: {stray}"
        assert expected == set(REGISTERED), (
            "the set of classes the two packages define changed after collection.\n"
            f"  now defined but not covered: {sorted(expected - set(REGISTERED))}\n"
            f"  covered but no longer defined: {sorted(set(REGISTERED) - expected)}"
        )

    def test_value_kind_plane(self, postgres_dialect):
        """Every value kind travels as a plain JSON scalar, and only those.

        ``Literal`` is one class over fourteen kinds, so the class matrix says
        nothing about the value plane: it would see one row and pass whatever
        the payloads were. This asserts the plane itself -- each kind's wire
        name, its payload's JSON type, and the round-trip that restores the
        host value -- and pins the set in both directions, because a value
        kind added to core and not exercised here is a kind whose spelling no
        backend has looked at.

        The non-finite floats are the reason the plane is worth asserting:
        ``float_value`` carries a JSON *number*, so NaN and both infinities
        travel under kinds of their own, as text.
        """
        import datetime
        import decimal
        import math
        import uuid as uuid_module

        from rhosocial.activerecord.backend.expression.values import LiteralKind

        dialect = postgres_dialect
        cases = {
            "null_value": (None, type(None)),
            "boolean_value": (True, bool),
            "integer_value": (7, int),
            "float_value": (1.5, float),
            "nan_value": (float("nan"), float),
            "infinity_value": (float("inf"), float),
            "decimal_value": (decimal.Decimal("1.25"), decimal.Decimal),
            "string_value": ("x", str),
            "binary_value": (b"\x01\x02", bytes),
            "timestamp_value": (datetime.datetime(2026, 10, 20, 1, 2, 3), datetime.datetime),
            "date_value": (datetime.date(2026, 10, 20), datetime.date),
            "time_value": (datetime.time(1, 2, 3), datetime.time),
            "interval_value": (datetime.timedelta(days=1, seconds=2), datetime.timedelta),
            "list_value": ([1, 2], list),
            "tuple_value": ((1, 2), tuple),
            "uuid_value": (
                uuid_module.UUID("01234567-89ab-cdef-0123-456789abcdef"),
                uuid_module.UUID,
            ),
        }

        assert set(cases) == {member.wire for member in LiteralKind}, (
            "the value-kind plane and LiteralKind disagree: "
            f"{sorted(set(cases) ^ {member.wire for member in LiteralKind})}"
        )

        for wire, (host, _host_type) in cases.items():
            literal = Literal(dialect, host)
            assert literal.value_kind.wire == wire, (
                f"{wire}: built a literal of kind {literal.value_kind.wire}"
            )
            artifact = serialize(literal)
            assert artifact["kind"] == wire, artifact
            payload = artifact["params"]["value"]
            if wire in ("nan_value", "infinity_value"):
                assert isinstance(payload, str), f"{wire} must travel as text"
            elif wire == "decimal_value":
                assert isinstance(payload, str), "a Decimal must not be rounded"
            elif wire == "binary_value":
                assert isinstance(payload, str), "bytes travel as base64 text"
            elif wire == "interval_value":
                assert isinstance(payload, str), "a timedelta travels as ISO 8601"
            elif wire in ("list_value", "tuple_value"):
                assert isinstance(payload, list), artifact
            elif wire == "null_value":
                assert payload is None, artifact
            elif wire in ("boolean_value", "integer_value", "float_value"):
                assert isinstance(payload, (bool, int, float)) and not isinstance(
                    payload, str
                ), artifact
            else:
                assert isinstance(payload, str), artifact

            for channel, enc, dec in (
                ("dict", serialize, deserialize),
                ("json", serialize_json, deserialize_json),
                ("xml", serialize_xml, deserialize_xml),
            ):
                restored = dec(enc(literal), dialect)
                assert restored.value_kind.wire == wire, f"{wire}: {channel}"
                if wire == "nan_value":
                    assert math.isnan(restored.value), f"{wire}: {channel}"
                else:
                    assert restored.value == literal.value, f"{wire}: {channel}"

        # A payload that contradicts its own kind is refused at the value layer,
        # not rendered as something else.
        with pytest.raises(ValueError, match="float_value cannot carry"):
            LiteralKind.FLOAT.unpack(float("nan"))

    def test_matrix_collects_core_expressions_too(self):
        """Core's own expression classes are in the matrix, not only ours.

        This is the specific gap that let ``mixins/ddl/comment.py`` read a field
        core had removed and stay green: core's classes were outside the only
        matrix in this repository. Removing this assertion would restore that
        blindness silently.
        """
        core_classes = [
            fqn for fqn in REGISTERED if fqn.startswith(f"{CORE_EXPR_PKG}.")
        ]
        assert core_classes, (
            "the matrix collects no core expression classes; core statements "
            "rendered by this dialect would go unasserted"
        )

    def test_every_covered_class_is_registered_for_deserialization(self):
        """A class in the matrix can be found again when deserializing.

        Deserialisation resolves a node's kind through the framework's
        registry whitelist, so a class the matrix renders but the registry
        cannot resolve would round-trip into the wrong thing or nothing at
        all.
        """
        registry = default_registry()
        unresolved = []
        for fqn, cls in sorted(REGISTERED.items()):
            try:
                registry.resolve_node(cls.kind())
            except Exception:
                unresolved.append(fqn)
        assert not unresolved, (
            f"the matrix covers classes the kind whitelist cannot "
            f"resolve: {unresolved}"
        )

    def test_coverage_report(self, postgres_dialect):
        """Surface what the matrix covers, so coverage stays transparent."""
        census: Dict[str, int] = {}
        constructible = []
        for fqn in sorted(REGISTERED):
            instance, source = make_instance(REGISTERED[fqn], postgres_dialect)
            if instance is None:
                census["pinned-unconstructible"] = census.get("pinned-unconstructible", 0) + 1
                continue
            constructible.append(fqn)
            branch = assert_sql_roundtrip_classified(fqn, instance, postgres_dialect)
            census[branch] = census.get(branch, 0) + 1
        assert constructible
        summary = ", ".join(f"{count} {name}" for name, count in sorted(census.items()))
        print(
            f"\nexpression matrix: {len(constructible)} constructible, "
            f"{len(UNCONSTRUCTIBLE)} pinned-unconstructible, "
            f"{len(REGISTERED)} registered; {summary}"
        )
        for fqn in UNCONSTRUCTIBLE:
            print(f"  not constructible: {fqn}")
        for feature, methods in sorted(UNMODELLED_FORMATTERS.items()):
            print(f"  not modelled: {feature} ({len(methods)} formatters)")
