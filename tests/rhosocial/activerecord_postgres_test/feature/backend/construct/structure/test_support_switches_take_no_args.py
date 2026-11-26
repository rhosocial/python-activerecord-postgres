# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/structure/test_support_switches_take_no_args.py
"""``supports_*`` is a support switch: no arguments, one bool.

A capability flag answers "does this dialect configuration have capability k",
so its only input is the dialect instance, and its only output is a bool.
Anything that needs a name or a kind is either a separate switch (closed
vocabulary) or a format-time validation (open vocabulary) -- never an argument
to ``supports_*``. A name-keyed catalogue is a suggestion and belongs under
``suggest_*``, where it may answer with a mapping.

The walk is over this backend's ``src/**/*.py``, so a new protocol or mixin
that reintroduces an argument-taking probe, or a probe whose annotation is not
``bool``, fails here rather than at the call site that no longer expects one.
"""

import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[7]
SRC = PROJECT_ROOT / "src"
SOURCE_FILES = sorted(SRC.rglob("*.py"))


def _parents(tree):
    """Map each node to its parent, to tell a method from a module function."""
    parent = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parent[child] = node
    return parent


def _required_arguments(func, is_method):
    """Parameters that must be passed, excluding ``self``/``cls``."""
    args = func.args
    positional = list(args.args)
    start = 1 if is_method else 0
    stop = len(positional) - len(args.defaults)
    required = positional[start:stop] if stop >= start else []
    for arg, default in zip(args.kwonlyargs, args.kw_defaults):
        if default is None:
            required.append(arg)
    return [a.arg for a in required]


def _annotates_bool(func):
    """A switch declares ``-> bool``; anything else has no answer to give.

    A quoted ``"bool"`` is the same annotation written as a forward reference,
    so it counts. A missing annotation does not: the guard is what pins the
    answer shape, and "unannotated" would leave it open again.
    """
    annotation = func.returns
    if isinstance(annotation, ast.Name) and annotation.id == "bool":
        return True
    if isinstance(annotation, ast.Constant) and annotation.value == "bool":
        return True
    return False


def _violations(tree, label):
    """Every ``supports_*`` that is not a parameterless bool switch."""
    parent = _parents(tree)
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not node.name.startswith("supports_"):
            continue
        is_method = isinstance(parent.get(node), ast.ClassDef)
        missing = _required_arguments(node, is_method)
        if missing:
            found.append(("argument", f"{label}:{node.lineno} {node.name}({', '.join(missing)})"))
        if not _annotates_bool(node):
            found.append(("annotation", f"{label}:{node.lineno} {node.name} does not return bool"))
    return found


def _scan():
    violations = []
    for path in SOURCE_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        violations.extend(_violations(tree, str(path.relative_to(PROJECT_ROOT))))
    return violations


def _of_kind(kind):
    return [message for violation_kind, message in _scan() if violation_kind == kind]


def _probe(tmp_path, source):
    probe = tmp_path / "_probe.py"
    probe.write_text(source, encoding="utf-8")
    return ast.parse(probe.read_text(encoding="utf-8"), filename=str(probe))


class TestSupportSwitchesTakeNoArguments:
    def test_source_tree_is_visible(self):
        """The walk has to find the tree, or the guard proves nothing."""
        assert len(SOURCE_FILES) > 50, f"only found {len(SOURCE_FILES)} files under {SRC}"

    def test_every_support_switch_takes_no_required_argument(self):
        offenders = _of_kind("argument")
        assert not offenders, "supports_* with a required argument:\n" + "\n".join(offenders)

    def test_every_support_switch_annotates_bool(self):
        offenders = _of_kind("annotation")
        assert not offenders, "supports_* whose annotation is not bool:\n" + "\n".join(offenders)

    def test_the_scan_flags_a_planted_violation(self, tmp_path):
        tree = _probe(
            tmp_path,
            "class P:\n" "    def supports_thing(self, name):\n" "        return False\n",
        )
        assert _violations(tree, "probe")

    def test_the_scan_flags_a_planted_keyword_only_violation(self, tmp_path):
        tree = _probe(
            tmp_path,
            "class P:\n"
            "    def supports_thing(self, *, name) -> bool:\n"
            "        return False\n",
        )
        assert any(kind == "argument" for kind, _ in _violations(tree, "probe"))

    def test_the_scan_flags_a_non_bool_annotation(self, tmp_path):
        tree = _probe(
            tmp_path,
            "from typing import Dict\n"
            "class P:\n"
            "    def supports_thing(self) -> Dict[str, bool]:\n"
            "        return {}\n",
        )
        message = dict(_violations(tree, "probe"))["annotation"]
        assert "does not return bool" in message

    def test_the_scan_flags_a_missing_annotation(self, tmp_path):
        tree = _probe(
            tmp_path,
            "class P:\n" "    def supports_thing(self):\n" "        return False\n",
        )
        assert "annotation" in {kind for kind, _ in _violations(tree, "probe")}

    def test_the_scan_accepts_a_defaulted_parameter(self, tmp_path):
        tree = _probe(
            tmp_path,
            "class P:\n"
            "    def supports_thing(self, name=None) -> bool:\n"
            "        return False\n",
        )
        assert not [v for v in _violations(tree, "probe") if v[0] == "argument"]

    def test_the_scan_accepts_a_quoted_bool_annotation(self, tmp_path):
        tree = _probe(
            tmp_path,
            'class P:\n    def supports_thing(self) -> "bool":\n        return False\n',
        )
        assert not _violations(tree, "probe")
