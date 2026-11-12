# src/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_family_census.py
"""Ask every introspection family the same question, on a real server.

This module exists because of a defect a fake executor cannot catch. On
2026-11-11 a census over every server in the reference matrix found that
`list_unused_indexes` filtered `indisvalid` off `pg_stat_user_indexes` — a view
that carries no such column — so the statement was refused by every release
this backend targets (`column i.indisvalid does not exist`). Every unit test
passed: the fake executor answers whatever the fixture scripts and never parses
the SQL. Only a real server refuses a statement that names a column the release
does not serve.

The census generalises that lesson into a standing guard:

* every sync introspection method is asked, with arguments synthesized from its
  signature, so a contract that grows a parameter does not silently lose
  coverage — a hand-written table of calls goes stale, a signature walk cannot;
* the object names are deliberately non-existent, because what is under test is
  whether the *statement* is valid: the server parses the whole statement before
  it looks for the object, which is exactly the property a fake cannot check;
* a family either answers or refuses with a typed not-supported error. A server
  error — "column does not exist", "relation does not exist" — is a defect, and
  the failure message groups by (family, first line of the error) so a
  version-skew defect shows as "one family, some releases" rather than noise.

Nothing here asserts data. No assertion names a table a particular server may or
may not hold, so the suite is the same test everywhere it runs.

Read-only: it asks the catalogue and never creates, alters or drops an object.

Two notes on running it. The live half needs the scenario plumbing this repo
already uses (`postgres_backend`, whose params come from
`tests/config/postgres_scenarios.yaml`); with no scenario reachable it runs
nothing, which is the same skip every other suite in this directory has. And
the argument synthesis is exercised by the server-free test at the bottom of
this module, so the walk's shape is checked without a database.
"""
import inspect
from collections import defaultdict
from typing import Any, Dict


from rhosocial.activerecord.backend.introspection import IntrospectionNotSupportedError


# The names deliberately do not exist. A server validates the statement before
# it looks for the object, so a bogus name asks the same question the real one
# asks — "is this statement well-formed against this release" — without
# depending on what any particular database happens to hold.
PROBE_TABLE = "probe_table_that_does_not_exist"
PROBE_COLUMN = "probe_column_that_does_not_exist"
PROBE_OBJECT = "probe_object_that_does_not_exist"
PROBE_LIMIT = 5


def _probe_arguments_for(method) -> Dict[str, Any]:
    """Choose a probe value for every parameter of *method*.

    A recognized parameter gets the probe value. One no rule covers keeps its
    default — or None when it has none — because `list_tables(include_system=
    False)` is still a catalogue read whose statement must parse, and dropping
    the whole family would leave that read unchecked. Nothing is guessed at: a
    value the rules cannot name is never invented.
    """
    kwargs = {}
    for name, parameter in inspect.signature(method).parameters.items():
        if name == "self" or parameter.kind in (
            parameter.VAR_POSITIONAL,
            parameter.VAR_KEYWORD,
        ):
            continue
        lowered = name.lower()
        if "schema" in lowered:
            kwargs[name] = None
        elif "column" in lowered:
            kwargs[name] = PROBE_COLUMN
        elif "table" in lowered:
            kwargs[name] = PROBE_TABLE
        elif "limit" in lowered:
            kwargs[name] = PROBE_LIMIT
        elif "type" in lowered:
            kwargs[name] = "table"
        elif "name" in lowered:
            kwargs[name] = PROBE_OBJECT
        else:
            # A parameter no rule covers is left at its default rather than
            # dropping the whole question: `list_tables(include_system=False)`
            # is still a catalogue read whose statement must parse, and skipping
            # the family would leave it unchecked. The default is the smallest
            # question the contract accepts, which is what the census wants.
            kwargs[name] = parameter.default if parameter.default is not parameter.empty else None
    return kwargs


def _first_line(error: Exception) -> str:
    text = str(error).splitlines()
    return text[0] if text else type(error).__name__


def _refusal_name(error: Exception) -> str:
    """Name the refusal, so one typed refusal is not confused with another."""
    return type(error).__name__


def test_every_family_answers_or_refuses_by_name(postgres_backend):
    """Ask every family, and accept exactly two kinds of answer.

    An answer, or a typed refusal naming the family and the catalog the answer
    would come from. Anything else — above all a server error — is a statement
    this release does not accept, which is the defect this module exists to
    catch.
    """
    introspector = postgres_backend.introspector

    answered = []
    refused = defaultdict(list)
    skipped = []
    server_errors = defaultdict(list)

    for name in sorted(dir(introspector)):
        if not name.startswith(("list_", "get_")):
            continue
        method = getattr(introspector, name)
        if not callable(method):
            continue
        kwargs = _probe_arguments_for(method)
        if kwargs is None:
            skipped.append(name)
            continue
        try:
            method(**kwargs)
        except IntrospectionNotSupportedError as error:
            # A refusal is an answer: this engine does not serve the family, and
            # it says so by name.
            refused[_refusal_name(error)].append(name)
        except Exception as error:  # noqa: BLE001 — the census classifies, not swallows
            # Grouped by (family, first line): a version-skew defect then reads
            # as one family across a subset of releases, not as noise.
            server_errors[(name, _first_line(error))].append(
                getattr(postgres_backend, "scenario_name", "?")
            )
        else:
            answered.append(name)

    # Nothing may be silently un-askable. A parameter this walk cannot
    # name a probe value for is a gap in the walk, not a gap in the backend — it is reported
    # so the next contract change makes the walk grow rather than skip.
    assert not skipped, (
        "these families were not asked because no probe value names their "
        "parameters; extend _probe_arguments_for rather than leaving them "
        f"unvisited: {sorted(skipped)}"
    )

    assert not server_errors, (
        "a catalog statement named something this release does not serve. "
        "A fake executor cannot catch this class of defect — it answers whatever "
        "the fixture scripts and never parses SQL. Grouped by "
        "(family, first line of error, servers): "
        + "; ".join(
            f"{family}: {message} [{', '.join(servers)}]"
            for (family, message), servers in sorted(server_errors.items())
        )
    )

    # The walk must have visited the contract, not a corner of it: an
    # introspector that exposes nothing is not a passing census.
    assert answered or refused, (
        "no introspection family was asked — the walk is broken, not the backend"
    )


def test_argument_synthesis_asks_the_smallest_question():
    """The walk is checked without a server, so it cannot rot silently.

    A signature with a schema, a table, a column, a limit and a name asks five
    things; one with a parameter no rule covers is skipped rather than guessed
    at — an unknown shape is a question this walk does not know how to ask, and
    skipping is honest where guessing could ask a different one than the
    contract means.
    """
    def smallest(schema=None, table_name="", column_name="", limit=0, name=""):
        pass

    assert _probe_arguments_for(smallest) == {
        "schema": None,
        "table_name": PROBE_TABLE,
        "column_name": PROBE_COLUMN,
        "limit": PROBE_LIMIT,
        "name": PROBE_OBJECT,
    }

    def unaskable(scope, include_system=False):
        pass

    # A parameter no rule covers keeps its default rather than dropping the
    # family: the read still has to parse. A required parameter with no default
    # answers None — an absence the server can read, rather than a value the
    # rules would have had to invent.
    assert _probe_arguments_for(unaskable) == {
        "scope": None,
        "include_system": False,
    }

    def required_and_unknown(scope, table_name):
        pass

    assert _probe_arguments_for(required_and_unknown) == {
        "scope": None,
        "table_name": PROBE_TABLE,
    }

    def optional_everything(include_system=False, table_name=""):
        pass

    # Optional parameters are asked too: the contract's `schema=None` precedes
    # `table_name=""` in most signatures, so skipping defaults would ask nothing
    # at all — the census would pass by never asking a question.
    assert _probe_arguments_for(optional_everything) == {
        "include_system": False,
        "table_name": PROBE_TABLE,
    }

    def all_recognizable(schema=None, table_name="", column_name="", limit=0, name=""):
        pass

    assert _probe_arguments_for(all_recognizable) == {
        "schema": None,
        "table_name": PROBE_TABLE,
        "column_name": PROBE_COLUMN,
        "limit": PROBE_LIMIT,
        "name": PROBE_OBJECT,
    }
