# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_kind_hygiene.py
"""The PostgreSQL expression package's kinds: what they are and how they are named.

The organisation's rule (expression-kinds.md §三, expression-dialect §3 and
§11.1) says three things about a backend's own expressions, and each is
checkable from the running code rather than from a hand-kept list:

* **A kind core has issued is not re-declared.** A backend may override how a
  core kind is rendered; it may not put a second class behind that kind, because
  an artifact would then have to choose between them. The check is a set
  comparison against core's own kind set -- read from the registry core builds,
  not from a table in this file.
* **A kind core does not have is the backend's own, and says so.** Its name
  starts with ``postgres_``, and the ``PostgreSQL`` spelling is refused
  outright: a run of capitals is one word, so ``PostgreSQLX`` derives
  ``postgre_sql_x`` -- a different kind from ``postgres_x``.
* **Class name, kind and formatter entry are one identity.** For a node this
  backend declares for itself, the formatter is ``format_<kind>``. Nodes that
  *derive from* a core node keep core's entry (that is the override), so they
  are exempt -- the derived node's kind still carries the backend name.

The enums are checked too: a closed value set is a ``(str, Enum)`` whose
``__str__`` is ``str.__str__``, because the member's *word* is what the
artifact and every f-string carry.
"""

import importlib
import inspect
import pkgutil
from enum import Enum

import pytest

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.serialization import default_registry

PG_EXPR_PKG = "rhosocial.activerecord.backend.impl.postgres.expression"
CORE_EXPR_PKG = "rhosocial.activerecord.backend.expression"

#: A derived node keeps the core formatter it overrides, so its entry is not
#: ``format_<backend kind>``. This is the list of *base classes* that make a
#: class a derivation rather than a declaration -- a class whose first base is
#: one of these is overriding something core already names, and its kind must
#: still carry the backend prefix (checked separately).
CORE_BASES = (
    BaseExpression,
)


def _walk(package):
    """Every concrete expression class the package defines, by FQN."""
    pkg = importlib.import_module(package)
    found = {}
    for _, modname, _ in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
        try:
            mod = importlib.import_module(modname)
        except Exception:  # pragma: no cover - an unimportable module is its own defect
            raise
        for name in dir(mod):
            obj = getattr(mod, name, None)
            if (
                isinstance(obj, type)
                and obj.__module__ == modname
                and issubclass(obj, BaseExpression)
                and obj is not BaseExpression
                and not inspect.isabstract(obj)
            ):
                found[f"{modname}.{name}"] = obj
    return found


@pytest.fixture(scope="module")
def backend_classes():
    return _walk(PG_EXPR_PKG)


@pytest.fixture(scope="module")
def core_kinds():
    """Core's kind set, read from the registry core assembles for itself."""
    return set(default_registry().node_kinds())


def test_no_core_kind_is_redeclared(backend_classes, core_kinds):
    """A kind core has issued resolves to exactly one class, and it is core's.

    Two classes behind one kind is the failure this catches: an artifact names
    the kind, and nothing in it would say which class to build.
    """
    collisions = {}
    for fqn, cls in sorted(backend_classes.items()):
        kind = cls.kind()
        if kind in core_kinds:
            collisions.setdefault(kind, []).append(fqn)
    assert not collisions, (
        "these PostgreSQL classes claim a kind core already issues; a backend "
        "may override the rendering of a core kind but not redeclare its node:\n"
        + "\n".join(f"  {kind}: {fqns}" for kind, fqns in sorted(collisions.items()))
    )


def test_backend_kinds_carry_the_backend_name(backend_classes, core_kinds):
    """A kind core does not have is the backend's, and is spelled ``postgres_*``.

    ``PostgreSQL`` is refused as a prefix: the derivation reads a run of
    capitals as one word, so ``PostgreSQLRowCountWildcard`` would produce
    ``postgre_sql_row_count_wildcard`` while every other class here produces
    ``postgres_*``. Two spellings of one backend is two namespaces.
    """
    wrong = {
        fqn: cls.kind()
        for fqn, cls in sorted(backend_classes.items())
        if cls.kind() not in core_kinds
        and not cls.kind().lstrip("_").startswith("postgres")
    }
    assert not wrong, (
        "these kinds are PostgreSQL's own but are not named for the backend:\n"
        + "\n".join(f"  {fqn} -> {kind}" for fqn, kind in wrong.items())
    )


#: Nodes whose formatter entry is a *family* entry rather than ``format_<kind>``.
#:
#: Each of these is one of several nodes rendered by one formatter: five
#: ``ALTER MATERIALIZED VIEW`` actions share the action formatter, the two enum
#: statements delegate to core's ``format_*_type_statement``, and the four value
#: wrappers render through core's ``format_function_call`` because their whole
#: job is to render the call they hold. §3 asks class/kind/entry to be one
#: identity; where a family necessarily shares an entry, the deviation is
#: recorded here with its reason instead of being left to look accidental.
FAMILY_FORMATTERS = {
    "rhosocial.activerecord.backend.impl.postgres.expression.ddl.mv."
    "PostgresChangeMaterializedViewOwnerAction": (
        "one of five ALTER MATERIALIZED VIEW actions sharing the action formatter"
    ),
    "rhosocial.activerecord.backend.impl.postgres.expression.ddl.mv."
    "PostgresRenameMaterializedViewAction": "one of five ALTER MATERIALIZED VIEW actions",
    "rhosocial.activerecord.backend.impl.postgres.expression.ddl.mv."
    "PostgresResetMaterializedViewPropertiesAction": "one of five ALTER MATERIALIZED VIEW actions",
    "rhosocial.activerecord.backend.impl.postgres.expression.ddl.mv."
    "PostgresSetMaterializedViewPropertiesAction": "one of five ALTER MATERIALIZED VIEW actions",
    "rhosocial.activerecord.backend.impl.postgres.expression.ddl.mv."
    "PostgresSetMaterializedViewSchemaAction": "one of five ALTER MATERIALIZED VIEW actions",
    "rhosocial.activerecord.backend.impl.postgres.expression.ddl.type."
    "PostgresAlterEnumTypeAddValueExpression": "a CREATE/ALTER/DROP TYPE statement, rendered by core's type formatter",
    "rhosocial.activerecord.backend.impl.postgres.expression.ddl.type."
    "PostgresDropEnumTypeExpression": "a DROP TYPE statement, rendered by core's type formatter",
    "rhosocial.activerecord.backend.impl.postgres.expression.values."
    "PostgresHstoreValueExpression": "renders the FunctionCall it wraps, deliberately verbatim",
    "rhosocial.activerecord.backend.impl.postgres.expression.values."
    "PostgresLtreeValueExpression": "renders the FunctionCall it wraps, deliberately verbatim",
    "rhosocial.activerecord.backend.impl.postgres.expression.values."
    "PostgresNetworkValueExpression": "renders the FunctionCall it wraps, deliberately verbatim",
    "rhosocial.activerecord.backend.impl.postgres.expression.values."
    "PostgresRangeValueExpression": "renders the FunctionCall it wraps, deliberately verbatim",
}


def test_formatter_entry_matches_kind_for_declared_nodes(backend_classes):
    """A node this backend declares for itself names ``format_<kind>``.

    A *derived* node (its first base is a core node) keeps the core formatter
    it overrides -- that is what an override is -- so only declarations are
    checked here. The families that share one entry are named in
    :data:`FAMILY_FORMATTERS`, and the table is checked in both directions: a
    mismatch outside it fails, and an entry that is no longer a mismatch fails
    as stale.
    """
    mismatched = {}
    for fqn, cls in sorted(backend_classes.items()):
        kind = cls.kind()
        if not kind.lstrip("_").startswith("postgres"):
            continue  # a private helper, or a derived node; checked above
        if fqn in FAMILY_FORMATTERS:
            continue
        first_base = cls.__bases__[0]
        if first_base.__module__.startswith(CORE_EXPR_PKG):
            continue  # derives from a core node; core's entry is the override
        prop = None
        for klass in cls.__mro__:
            if "format_method" in vars(klass):
                prop = vars(klass)["format_method"]
                break
        assert prop is not None, f"{fqn} declares no format_method"
        inst = object.__new__(cls)
        entry = prop.fget(inst)
        if entry != f"format_{kind}":
            mismatched[fqn] = (kind, entry)
    assert not mismatched, (
        "class name, kind and formatter entry disagree (expression-dialect §3). "
        "Either rename the class so the kind matches the entry, or add the row "
        "to FAMILY_FORMATTERS with the reason it must share one:\n"
        + "\n".join(
            f"  {fqn}: kind={kind!r} formatter={entry!r}"
            for fqn, (kind, entry) in mismatched.items()
        )
    )


def test_family_formatter_table_is_exact(backend_classes):
    """Every FAMILY_FORMATTERS row still describes a real mismatch."""
    stale = sorted(set(FAMILY_FORMATTERS) - set(backend_classes))
    assert not stale, f"FAMILY_FORMATTERS names classes that do not exist: {stale}"
    no_longer = []
    for fqn in FAMILY_FORMATTERS:
        cls = backend_classes[fqn]
        kind = cls.kind()
        prop = next(
            vars(klass)["format_method"]
            for klass in cls.__mro__
            if "format_method" in vars(klass)
        )
        entry = prop.fget(object.__new__(cls))
        if entry == f"format_{kind}":
            no_longer.append(fqn)
    assert not no_longer, (
        "these FAMILY_FORMATTERS rows now match their kind, so the exemption is "
        f"stale and should go: {no_longer}"
    )


def test_backend_enums_are_string_enums():
    """A closed value set is a ``(str, Enum)`` that stringifies as its word.

    ``(str, Enum)`` alone is not enough: ``Enum.__str__`` wins over the ``str``
    base, so ``str(LoggingMode.LOGGED)`` would be ``"LoggingMode.LOGGED"`` and
    the artifact -- which carries the word, JSON having no enum -- could not be
    read back. The walk is limited to this package (not the backends it is
    composed with), which is where a PostgreSQL enum can live.
    """
    pkg = importlib.import_module(PG_EXPR_PKG)
    wrong = {}
    for _, modname, _ in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
        try:
            mod = importlib.import_module(modname)
        except Exception:  # pragma: no cover
            continue
        for name in dir(mod):
            obj = getattr(mod, name, None)
            if (
                isinstance(obj, type)
                and issubclass(obj, Enum)
                and obj.__module__ == modname
                and obj.__members__
            ):
                member = next(iter(obj))
                if not isinstance(member, str) or str(member) != member.value:
                    wrong[f"{modname}.{name}"] = repr(str(member))
    assert not wrong, (
        "these enums do not stringify as the word they carry; the artifact "
        "holds the word, so a reader could not rebuild them:\n"
        + "\n".join(f"  {fqn}: str() == {text}" for fqn, text in wrong.items())
    )
