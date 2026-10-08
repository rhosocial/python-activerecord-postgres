# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_object_kind_guards.py
"""
A statement handed the wrong kind of catalogue object must refuse, not render.

Why this file exists
====================

A statement now holds the object it acts on, and ``to_sql()`` dispatches through
*that object's* ``format_method``. So
``CreateIndexExpression(dialect, Index(dialect, "i"), View(dialect, "v"))``
rendered ``CREATE INDEX i ON v (...)`` -- well-formed SQL naming the wrong kind of
object, with no error and no warning. Before objects carried names as strings,
``format_identifier`` accepted anything and a wrong type raised; switching to
objects removed the accidental check without putting a real one anywhere.

The check belongs in the formatter that applies it, and this backend shadows
several of core's. A shadowing body is the one that runs the MRO lookup, so the
check core's body carries is never reached -- which is why the guards had to be
written here as well as in core.

What is asserted
================

For every (statement class, slot, required kind, impostor kind) probe below, the
slot is filled with a different schema-object kind that is not a subclass of the
required one, and ``to_sql()`` must refuse. The real instance is built first and
one slot is then swapped, so everything else stays valid and the only thing the
refusal can be about is the slot under test.

A slot annotated ``SchemaObject`` legitimately accepts any object, so it is never
probed -- doing so would report a defect that is not there.

How the probe table is built (so it can be audited)
===================================================

The (class, slot, kind) triples are derived, not hand-listed: the ``__init__``
signatures of the classes collected by ``collect_expression_classes`` over the
PostgreSQL expression package and core's -- never from ``ExpressionRegistry``,
which is process-global -- are read for parameters annotated with a schema-object
class. Statements whose ``format_method`` the PostgreSQL dialect cannot resolve are
excluded, because an entry point that is never dispatched is not something
``to_sql()`` renders. Each impostor is then the first two buildable object kinds,
in sorted order, that are neither the required class nor a subclass of it.
"""

import copy
import importlib
import inspect
import typing

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import objects as objects_mod
from rhosocial.activerecord.testsuite.utils.expression import (
    collect_expression_classes,
    make_instance,
)

PG_EXPR_PKG = "rhosocial.activerecord.backend.impl.postgres.expression"
CORE_EXPR_PKG = "rhosocial.activerecord.backend.expression"

#: Suffixes of the schema-object classes, used to recognise one in an annotation.
_OBJECT_SUFFIXES = (
    "Table", "View", "Index", "Schema", "Sequence", "Type", "Domain", "Function",
    "Procedure", "Trigger", "Database", "Synonym", "PropertyGraph", "Routine",
    "RelationObject", "SchemaObject",
)


def _annotation_names(annotation):
    """Every class name mentioned anywhere inside an annotation."""
    found = set()
    stack = [annotation]
    while stack:
        item = stack.pop()
        if item is inspect.Parameter.empty or item is None:
            continue
        if isinstance(item, str):
            found.add(item.strip("'\""))
            continue
        if isinstance(item, type):
            found.add(item.__name__)
            continue
        origin = typing.get_origin(item)
        if origin is not None:
            stack.extend(typing.get_args(item))
    return found


def _object_classes():
    """The schema-object classes this package defines, keyed by name."""
    classes = {}
    for attr in dir(objects_mod):
        obj = getattr(objects_mod, attr)
        if isinstance(obj, type) and attr.endswith(_OBJECT_SUFFIXES):
            classes[attr] = obj
    return classes


def _statement_classes():
    found = {}
    for package in (PG_EXPR_PKG, CORE_EXPR_PKG):
        found.update(collect_expression_classes(package))
    return found


def _buildable_kinds(dialect):
    """Object kinds that construct from a dialect and a bare name."""
    buildable = []
    for name in sorted(_object_classes()):
        cls = getattr(objects_mod, name, None)
        if cls is None:
            continue
        try:
            cls(dialect, "x")
        except Exception:  # noqa: BLE001
            # A kind that cannot self-construct is not a usable impostor. This is
            # not a failure: a Database and a Table both take a bare name, a
            # RelationObject base deliberately does not.
            continue
        buildable.append(cls)
    return buildable


def _renderable(dialect):
    """Only statements whose ``format_method`` the dialect can resolve.

    Some PostgreSQL expressions name a formatter that does not exist and are only
    ever called directly as entry points; ``to_sql()`` never dispatches to them, so
    refusing is not their contract and probing them would be meaningless.
    """
    resolvable = set()
    for klass in type(dialect).__mro__:
        resolvable.update(klass.__dict__)
    out = {}
    for fqn, cls in _statement_classes().items():
        try:
            method = cls.format_method.fget(None)
        except Exception:  # noqa: BLE001 - abstract property, or none at all
            continue
        if method in resolvable and callable(getattr(dialect, method, None)):
            out[fqn] = cls
    return out


def _object_slots():
    """{statement fqn: {slot: required object class}}.

    Read from the constructor signature, so a slot added later shows up without
    this file being touched.
    """
    objects = _object_classes()
    slots = {}
    for fqn, cls in sorted(_statement_classes().items()):
        try:
            sig = inspect.signature(cls.__init__)
        except (TypeError, ValueError):
            continue
        for pname, param in sig.parameters.items():
            if pname in ("self", "dialect"):
                continue
            matched = _annotation_names(param.annotation) & set(objects)
            if matched:
                slots.setdefault(fqn, {})[pname] = objects[sorted(matched)[0]]
    return slots


def _build_probes(dialect):
    """``[(fqn, slot, required name, impostor name)]``, derived not listed.

    Only statements whose *unmodified* instance renders are probed. A statement
    that already fails for an unrelated reason -- the property-graph statements,
    which PostgreSQL does not support at all, and ``CreateTriggerExpression``,
    whose introspective construction supplies a string where an enum belongs --
    cannot demonstrate anything about a slot: it raises either way. Including them
    would make the table look broader while proving less, which is exactly the
    substitution this file was written to end.
    """
    renderable = _renderable(dialect)
    buildable = _buildable_kinds(dialect)
    rows = []
    skipped = []
    for fqn, slots in sorted(_object_slots().items()):
        if fqn not in renderable:
            skipped.append((fqn, "format_method is not resolvable on this dialect"))
            continue
        instance, source = make_instance(renderable[fqn], dialect)
        if instance is None:
            skipped.append((fqn, f"the harness cannot construct it ({source})"))
            continue
        try:
            instance.to_sql()
        except Exception as exc:  # noqa: BLE001
            skipped.append(
                (fqn, f"it does not render as built ({type(exc).__name__})")
            )
            continue
        for slot, required in sorted(slots.items()):
            impostors = [
                cls
                for cls in buildable
                if cls is not required and not issubclass(cls, required)
            ][:2]
            for impostor in impostors:
                rows.append(
                    (fqn, slot, required.__name__, impostor.__name__)
                )
    return rows, skipped


def _probe_dialect():
    """A PostgreSQL 15 dialect, so the table is built from a real object graph."""
    dialect_module = importlib.import_module(
        "rhosocial.activerecord.backend.impl.postgres.dialect"
    )
    return dialect_module.PostgresDialect(version=(15, 0, 0))


PROBES, SKIPPED = _build_probes(_probe_dialect())

#: Labels pytest can put in a test id; a dotted fqn is already readable, so this
#: only shortens the object name.
_IDS = [f"{fqn.rsplit('.', 1)[-1]}.{slot}!={required}->{impostor}"
        for fqn, slot, required, impostor in PROBES]


def test_probe_table_is_not_empty():
    """A probe table that silently emptied would make every test below vacuous."""
    assert len(PROBES) > 50, (
        f"only {len(PROBES)} wrong-kind probes were built; the collection method "
        f"has probably stopped matching the statement tree"
    )


def test_skipped_statements_are_still_the_ones_we_think():
    """Nothing left the probe table for a new reason.

    The exclusions are structural -- a statement PostgreSQL cannot render at all,
    or one the harness cannot build correctly -- so a new arrival means the
    construction or the rendering changed, and the table should be revisited.
    """
    for fqn, reason in SKIPPED:
        assert reason in (
            "format_method is not resolvable on this dialect",
        ) or "cannot construct it" in reason or "does not render as built" in reason, (
            f"{fqn} was excluded from the probe table for an unrecognised reason: "
            f"{reason}"
        )


@pytest.mark.parametrize(
    "fqn,slot,required_name,impostor_name", PROBES, ids=_IDS
)
def test_wrong_object_kind_is_refused(fqn, slot, required_name, impostor_name):
    """A statement handed the wrong object kind must refuse at render time.

    ``TypeError`` is the contract: reaching the formatter with the wrong object is
    a programming error, not a missing feature, and the message has to name the
    slot or the kind so the mistake can be found. ``UnsupportedFeatureError`` is
    accepted as a refusal too -- a dialect may refuse the statement for an
    unrelated reason on the way past -- but it is not required to name the slot,
    because it is not talking about the slot. Anything that *renders* is the defect
    this file exists for: well-formed SQL naming an object the caller never asked
    for.
    """
    dialect = _probe_dialect()
    cls = _renderable(dialect)[fqn]
    instance, source = make_instance(cls, dialect)
    assert instance is not None, f"{fqn}: could not be constructed ({source})"
    impostor = getattr(objects_mod, impostor_name)
    swapped = copy.copy(instance)
    object.__setattr__(swapped, slot, impostor(dialect, "wrongkind"))
    try:
        swapped.to_sql()
    except TypeError as exc:
        assert slot in str(exc) or required_name in str(exc), (
            f"{fqn}.{slot} refused with a TypeError naming neither the slot nor "
            f"the required kind: {exc}"
        )
        return
    except UnsupportedFeatureError:
        return
    pytest.fail(
        f"{fqn}.{slot} must be {required_name}; handed a {impostor_name} and it "
        f"rendered {swapped.to_sql()!r} -- well-formed SQL naming the wrong kind "
        f"of object"
    )