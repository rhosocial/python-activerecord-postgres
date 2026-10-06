# tests/rhosocial/activerecord_postgres_test/feature/backend/test_expression_roundtrip_all.py
"""
Functional serialization coverage: every registered expression class must
round-trip losslessly through all three encodings (dict / JSON string / XML).

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
PostgreSQL statement formats a core ``Table`` or a core ``CommentOnExpression``,
the defect that matters is in *this* dialect's reading of a core object. That is
why ``mixins/ddl/comment.py`` could read a field core had removed and stay green:
core's own classes were outside the only matrix in this repository, so a
PostgreSQL-side formatter misreading them was unobservable.

The matrix therefore walks both packages with ``collect_expression_classes``. It
deliberately does not read ``ExpressionRegistry._registry``: that dict is
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
* ``UnsupportedFeatureError`` -- this dialect does not model the feature.
  Asserted as exactly that type, so a different error cannot hide behind it.
* a member of :data:`LEGITIMATE_NON_RENDERS` -- a class that cannot render for a
  reason belonging to its own tree or to PostgreSQL's feature set. Each entry
  pins the exception type *and* a message fragment, so a class that started
  failing for a different reason fails here instead of staying quietly green.
* **anything else** -- a failure naming the class and the exception.

Why some pins are themselves ``UnsupportedFeatureError``
=========================================================

``to_sql()`` reports a missing formatter as ``UnsupportedFeatureError``, so the
two buckets above can overlap: a class can be unrenderable for a reason that is
about PostgreSQL rather than about its own tree. ``TableSource``,
``PivotExpression`` and ``UnpivotExpression`` are such classes, and they are
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
from rhosocial.activerecord.backend.expression.collation import CollateExpression
from rhosocial.activerecord.backend.expression.core import Column, Literal
from rhosocial.activerecord.backend.expression.datetime import (
    TemporalOptionsExpression,
)
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
    ComparisonPredicate,
    ILIKEExpression as CoreILIKEExpression,
)
from rhosocial.activerecord.backend.expression.query_parts import JoinClause
from rhosocial.activerecord.backend.expression.serialization import (
    ExpressionRegistry,
    deserialize,
    deserialize_json,
    deserialize_xml,
    serialize,
    serialize_json,
    serialize_xml,
)
from rhosocial.activerecord.backend.expression.sources import NamedRelationRef
from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
    AddColumn,
    AddTableConstraint,
)
from rhosocial.activerecord.backend.expression.statements.ddl_database import (
    AlterDatabaseAction,
    AlterDatabaseExpression,
)
from rhosocial.activerecord.backend.expression.statements.ddl_domain import (
    AddDomainCheckAction,
    DomainCheckConstraint,
    RenameDomainAction,
)
from rhosocial.activerecord.backend.expression.statements.ddl_partition import (
    PartitionStrategy,
)
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    ColumnConstraint,
    ColumnConstraintType,
    ColumnDefinition,
    ForeignKeyConstraint,
    ReferencesClause,
    TableConstraint,
    TableConstraintType,
)
from rhosocial.activerecord.backend.expression.statements.ddl_trigger import (
    CreateTriggerExpression,
    TriggerEvent,
    TriggerTiming,
)
from rhosocial.activerecord.backend.expression.statements.dml import (
    MergeAction,
    MergeActionType,
    MergeExpression,
)
from rhosocial.activerecord.backend.expression.types import (
    ArrayType,
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
    PostgresAlterConstraint,
    PostgresValidateConstraint,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.domain import (
    PostgresAddDomainCheckAction,
    PostgresAlterDomainExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.exclude_constraint import (
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
    PostgresPartitionClause,
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
    CreateEnumTypeExpression,
    EnumValuesExpression,
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
from rhosocial.activerecord.backend.impl.postgres.expression.ilike import (
    ILIKEExpression as PostgresILIKEExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresArrayType,
)
from rhosocial.activerecord.testsuite.utils.expression import (
    collect_expression_classes,
    make_instance,
    register_all,
    register_special_constructor,
)


def assert_params_equal(a, b, path="params"):
    """Deep compare two get_params() dicts, treating nested BaseExpression
    instances as structurally equal when their get_params() match."""
    if isinstance(a, BaseExpression) and isinstance(b, BaseExpression):
        assert_params_equal(a.get_params(), b.get_params(), path + ".<expr>")
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

    Two packages, walked rather than read out of the registry -- see the module
    docstring. ``ExpressionRegistry._auto_register_builtins`` is still called
    because deserialisation needs the core classes registered; that is a separate
    concern from deciding which classes to test.
    """
    ExpressionRegistry._auto_register_builtins()
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


def _integer_column(dialect, name="col"):
    """A column definition carrying a *dialect-bound* type.

    The binding is load-bearing, not cosmetic: ``to_sql()`` dispatches on the
    type through its own dialect, so an unbound ``IntegerType()`` raises
    ``ValueError: ... has no dialect bound`` the moment anything renders it.
    """
    return ColumnDefinition(dialect, name, IntegerType(dialect))


def _primary_key_constraint(dialect):
    """A table-level PRIMARY KEY, shared by three guesses that need one."""
    return TableConstraint(
        dialect, TableConstraintType.PRIMARY_KEY, name="c", columns=["a"]
    )


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
    """An ALTER CONSTRAINT action carrying the one required enum member.

    ``constraint_type`` is typed ``ColumnConstraintType | TableConstraintType |
    str``, so the introspective constructor cannot know which member is legal --
    and only CHECK and FOREIGN KEY are accepted for this statement, so most
    guesses are refused by name. PostgreSQL's own four subclasses and core's three
    share this shape, so one constructor is registered under each name:
    ``make_instance`` picks by ``str.endswith``, so a single shared suffix would
    also shadow unrelated classes.
    """
    return PostgresAlterConstraint(
        dialect,
        constraint_name="c",
        name="c",
        constraint_type=TableConstraintType.CHECK,
    )


def _validate_constraint(dialect):
    """A VALIDATE CONSTRAINT action, which needs only a name.

    The guess leaves the name ``None`` and the class refuses a nameless
    constraint.
    """
    return PostgresValidateConstraint(
        dialect, constraint_name="c", name="c"
    )


#: The ``constraint_type`` family: PostgreSQL's four ALTER CONSTRAINT classes and
#: core's three. All seven need ``_alter_constraint``.
ALTER_CONSTRAINT_SUFFIXES = (
    "ddl.constraint.PostgresAlterConstraint",
    "ddl.constraint.PostgresAlterConstraintAction",
    "ddl.constraint.PostgresAlterConstraintExpression",
    "ddl.constraint.PostgresAlterTableConstraint",
    "statements.ddl_alter.AlterConstraint",
    "statements.ddl_alter.AlterConstraintAction",
    "statements.ddl_alter.AlterTableConstraint",
)

#: The VALIDATE CONSTRAINT family, same shape, same reason. Seven classes.
VALIDATE_CONSTRAINT_SUFFIXES = (
    "ddl.constraint.PostgresValidateConstraint",
    "ddl.constraint.PostgresValidateConstraintAction",
    "ddl.constraint.PostgresValidateConstraintExpression",
    "ddl.constraint.PostgresValidateTableConstraint",
    "statements.ddl_alter.ValidateConstraint",
    "statements.ddl_alter.ValidateConstraintAction",
    "statements.ddl_alter.ValidateTableConstraint",
)


def register_specials():
    """Replace every introspective guess that would cost a real assertion."""
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
        "statements.ddl_alter.AddColumn",
        lambda d: AddColumn(d, _integer_column(d)),
    )
    register_special_constructor(
        "statements.ddl_alter.AddTableConstraint",
        lambda d: AddTableConstraint(d, _primary_key_constraint(d)),
    )
    register_special_constructor(
        "statements.ddl_table.ColumnConstraint",
        lambda d: ColumnConstraint(d, ColumnConstraintType.NOT_NULL, name="c"),
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
        "statements.ddl_database.AlterDatabaseExpression",
        lambda d: AlterDatabaseExpression(
            d,
            Database(d, "db"),
            action=AlterDatabaseAction.RENAME_TO,
            target="renamed_db",
        ),
    )
    register_special_constructor(
        "statements.ddl_trigger.CreateTriggerExpression",
        lambda d: CreateTriggerExpression(
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
    # ``constraint_type`` is a union of an enum class and ``str``, so the guess
    # cannot know which member is legal; the VALIDATE family needs only a name.
    for suffix in ALTER_CONSTRAINT_SUFFIXES:
        register_special_constructor(suffix, lambda d: _alter_constraint(d))
    for suffix in VALIDATE_CONSTRAINT_SUFFIXES:
        register_special_constructor(suffix, lambda d: _validate_constraint(d))

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
        "ddl.partition.PostgresPartitionClause",
        lambda d: PostgresPartitionClause(
            d, PartitionStrategy.RANGE, [Column(d, "a")]
        ),
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
        "ddl.type.CreateEnumTypeExpression",
        lambda d: CreateEnumTypeExpression(d, name="mood", values=["sad", "ok"]),
    )
    register_special_constructor(
        "ddl.type.EnumValuesExpression",
        lambda d: EnumValuesExpression(d, values=["sad", "ok"]),
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
    # ``.to_sql()`` on it. Core and PostgreSQL spell ILIKE as two classes with the
    # same parameter shape, so both need the answer.
    register_special_constructor(
        "predicates.ILIKEExpression",
        lambda d: CoreILIKEExpression(d, Column(d, "a"), "x%"),
    )
    register_special_constructor(
        "ilike.ILIKEExpression",
        lambda d: PostgresILIKEExpression(d, Column(d, "a"), "x%"),
    )

    # -- COLLATE needs a collation PostgreSQL actually has -------------------
    # The formatter validates the name against the backend whitelist, and the
    # guess's ``"x"`` is not on it. ``und-x-icu`` needs PostgreSQL 10+, so ``C`` is
    # the one that is legal at every version.
    register_special_constructor(
        "collation.CollateExpression",
        lambda d: CollateExpression(d, Column(d, "a"), "C"),
    )

    # -- MERGE --------------------------------------------------------------
    register_special_constructor(
        "statements.dml.MergeAction",
        lambda d: _merge_action(d),
    )
    register_special_constructor(
        "statements.dml.MergeExpression",
        lambda d: MergeExpression(
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
        "graph.EdgeTable",
        lambda d: graph_mod.EdgeTable(d, edge_table(d), ["src"], ["dst"]),
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
        lambda d: graph_mod.ColumnsClause(d, graph_mod.GraphColumn("n", "id")),
    )
    register_special_constructor(
        "graph.GraphTableExpression",
        lambda d: graph_mod.GraphTableExpression(
            d,
            graph=PropertyGraph(d, "g"),
            match=graph_mod.MatchClause(d, path_pattern(d)),
            columns=graph_mod.ColumnsClause(d, graph_mod.GraphColumn("n", "id")),
        ),
    )
    register_special_constructor(
        "graph.CreatePropertyGraphExpression",
        lambda d: graph_mod.CreatePropertyGraphExpression(
            d, graph=PropertyGraph(d, "g"),
            vertex_tables=[graph_mod.VertexTable(d, node_table(d))],
        ),
    )
    # The formatter accepts "add"/"drop" against "vertex tables"/"edge tables"/
    # "tables"; anything else is refused.
    register_special_constructor(
        "graph.AlterPropertyGraphExpression",
        lambda d: graph_mod.AlterPropertyGraphExpression(
            d,
            graph=PropertyGraph(d, "g"),
            action="add",
            target="vertex tables",
            vertex_tables=[graph_mod.VertexTable(d, node_table(d))],
        ),
    )
    register_special_constructor(
        "graph.DropPropertyGraphExpression",
        lambda d: graph_mod.DropPropertyGraphExpression(d, graph=PropertyGraph(d, "g")),
    )

    # -- types --------------------------------------------------------------
    # The element type must be bound to the dialect, because ``to_sql`` dispatches
    # on it through its own dialect.
    register_special_constructor(
        "types.array.ArrayType", lambda d: ArrayType(d, VarCharType(d, 10))
    )
    register_special_constructor(
        "types.PostgresArrayType", lambda d: PostgresArrayType(d, VarCharType(d, 10))
    )
    register_special_constructor(
        "types.enum_.EnumType", lambda d: EnumType(d, values=["sad", "ok"])
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
    ``TableSource``, ``PivotExpression`` and ``UnpivotExpression`` start failing
    for each other's reason and still pass.
    """
    return f"does not support the '{formatter_name}' statement"

LEGITIMATE_NON_RENDERS = {
    # ---- a marker class that exists in order to refuse ------------------
    # ``_UnsupportedDomainAction`` is constructed precisely so an ALTER DOMAIN
    # action PostgreSQL cannot express gets a message naming the action. The
    # ValueError is the designed behaviour, not a defect -- and it is the only
    # thing this class does.
    "rhosocial.activerecord.backend.impl.postgres.expression.ddl.domain."
    "_UnsupportedDomainAction": (
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
    "rhosocial.activerecord.backend.expression.bases.SQLValueExpression": (
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
    "rhosocial.activerecord.backend.expression.datetime._TemporalValueExpression": (
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
        "does not declare a valid generic type name",
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

    # ---- SQL standard pivot, which is not PostgreSQL syntax --------------
    # Core models the SQL:2003 PIVOT / UNPIVOT for engines that spell it that way.
    # PostgreSQL has no PIVOT keyword, so the dialect declares no
    # ``format_pivot_expression``. Typing PIVOT is done with a ``CASE`` /
    # ``generate_series`` plan, not with this expression, so the absence is a fact
    # about the engine rather than a gap in this backend. Each entry names its own
    # formatter so the two cannot pass for one another.
    "rhosocial.activerecord.backend.expression.pivot.PivotExpression": (
        UnsupportedFeatureError,
        _no_such_formatter("format_pivot_expression"),
    ),
    "rhosocial.activerecord.backend.expression.pivot.UnpivotExpression": (
        UnsupportedFeatureError,
        _no_such_formatter("format_unpivot_expression"),
    ),
}


def _channels_that_lose_params(fqn, instance, dialect):
    """The encodings whose round-trip does not restore ``get_params()``.

    Returns ``{channel: why}``. An empty dict means the class round-trips
    everywhere, which every class in the matrix must do -- there is no exemption
    list, because there has never been a reason for one.
    """
    original = instance.get_params()
    lost = {}
    for channel, decoded in (
        ("dict", deserialize(serialize(instance), dialect)),
        ("json", deserialize_json(serialize_json(instance), dialect)),
        ("xml", deserialize_xml(serialize_xml(instance), dialect)),
    ):
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

    for channel, decoded in (
        ("dict", deserialize(serialize(instance), dialect)),
        ("json", deserialize_json(serialize_json(instance), dialect)),
        ("xml", deserialize_xml(serialize_xml(instance), dialect)),
    ):
        decoded_sql, decoded_params = decoded.to_sql()
        assert decoded_sql == expected_sql, (
            f"{fqn}: {channel} round-trip changed the SQL.\n"
            f"  original: {expected_sql!r}\n"
            f"  {channel}: {decoded_sql!r}"
        )
        assert decoded_params == expected_params, (
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
        assert not lost, (
            f"{fqn} does not survive the round-trip through {sorted(lost)}.\n"
            + "\n".join(f"  {channel}: {why}" for channel, why in lost.items())
            + "\nThe serialiser encodes dataclasses field by field and BaseExpression "
            "nodes structurally. A payload that is neither is written out as null, "
            "so a round-trip silently drops it. Make it a dataclass -- the "
            "constructor signature is what get_params() reads, so it does not have "
            "to change."
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

    def test_matrix_covers_both_packages_completely(self):
        """The matrix covers every concrete class both packages define.

        Re-walked here rather than trusting the module-level collection, so a class
        that appeared after import is caught. The package walk is used rather than
        the registry because the registry also holds whatever backends other test
        modules happened to import.
        """
        ExpressionRegistry._auto_register_builtins()
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

        Deserialisation looks the class up by name, so a class the matrix renders
        but the registry cannot resolve would round-trip into the wrong thing or
        nothing at all.
        """
        ExpressionRegistry._auto_register_builtins()
        unresolved = sorted(set(REGISTERED) - set(ExpressionRegistry._registry))
        assert not unresolved, (
            f"the matrix covers classes the registry cannot resolve: {unresolved}"
        )

    def test_coverage_report(self, postgres_dialect):
        """Surface what the matrix covers, so coverage stays transparent."""
        ExpressionRegistry._auto_register_builtins()
        constructible = [
            fqn
            for fqn in REGISTERED
            if make_instance(REGISTERED[fqn], postgres_dialect)[0] is not None
        ]
        assert constructible
        print(
            f"\nexpression matrix: {len(constructible)} constructible, "
            f"{len(UNCONSTRUCTIBLE)} pinned-unconstructible, "
            f"{len(REGISTERED)} registered"
        )
        for fqn in UNCONSTRUCTIBLE:
            print(f"  not constructible: {fqn}")