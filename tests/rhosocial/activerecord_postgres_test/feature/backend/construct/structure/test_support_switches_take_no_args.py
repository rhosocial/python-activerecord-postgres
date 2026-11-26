# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/structure/test_support_switches_take_no_args.py
"""``supports_*`` is a support switch: no arguments, one bool.

A capability flag answers "does this dialect configuration have capability k",
so its only input is the dialect instance. Anything that needs a name or a kind
is either a separate switch (closed vocabulary) or a format-time validation
(open vocabulary) -- never an argument to ``supports_*``.

The walk is over this backend's ``src/**/*.py``, so a new protocol or mixin
that reintroduces an argument-taking probe fails here rather than at the call
site that no longer passes one.
"""

import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[7]
SRC = PROJECT_ROOT / "src"
SOURCE_FILES = sorted(SRC.rglob("*.py"))


def _required_arguments(func):
    """Parameters that must be passed, excluding ``self``."""
    args = func.args
    positional = list(args.args)
    required = positional[1 : len(positional) - len(args.defaults)]
    for arg, default in zip(args.kwonlyargs, args.kw_defaults):
        if default is None:
            required.append(arg)
    return [a.arg for a in required]


def _offenders(tree, label):
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        for item in node.body:
            if not isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not item.name.startswith("supports_"):
                continue
            missing = _required_arguments(item)
            if missing:
                found.append(f"{label}:{item.lineno} {item.name}({', '.join(missing)})")
    return found


def _relative(path):
    return str(path.relative_to(PROJECT_ROOT))


class TestSupportSwitchesTakeNoArguments:
    def test_source_tree_is_visible(self):
        """The walk has to find the tree, or the guard proves nothing."""
        assert len(SOURCE_FILES) > 50, f"only found {len(SOURCE_FILES)} files under {SRC}"

    def test_every_support_switch_takes_no_required_argument(self):
        offenders = []
        for path in SOURCE_FILES:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            offenders.extend(_offenders(tree, _relative(path)))
        assert not offenders, "supports_* with a required argument:\n" + "\n".join(offenders)

    def test_the_scan_flags_a_planted_violation(self, tmp_path):
        probe = tmp_path / "_probe.py"
        probe.write_text(
            "class P:\n"
            "    def supports_thing(self, name):\n"
            "        return False\n",
            encoding="utf-8",
        )
        tree = ast.parse(probe.read_text(encoding="utf-8"))
        assert _offenders(tree, "probe")

    def test_the_scan_flags_a_planted_keyword_only_violation(self, tmp_path):
        probe = tmp_path / "_probe.py"
        probe.write_text(
            "class P:\n"
            "    def supports_thing(self, *, name):\n"
            "        return False\n",
            encoding="utf-8",
        )
        tree = ast.parse(probe.read_text(encoding="utf-8"))
        assert _offenders(tree, "probe")

    def test_the_scan_accepts_a_defaulted_parameter(self, tmp_path):
        probe = tmp_path / "_probe.py"
        probe.write_text(
            "class P:\n"
            "    def supports_thing(self, name=None):\n"
            "        return False\n",
            encoding="utf-8",
        )
        tree = ast.parse(probe.read_text(encoding="utf-8"))
        assert not _offenders(tree, "probe")
