# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/structure/test_src_has_no_re.py
"""The backend's ``src`` tree must not use the :mod:`re` module.

The project rule is that production code stays free of regular expressions:
every pattern is a character-level scan, kept in
``postgres/_scan.py`` and the parsers that use it. Tests may keep regexes --
they assert generated SQL -- but ``src`` may not.

Two checks run over every ``src/**/*.py`` file:

* an ``ast`` walk fails on ``import re`` and ``from re import ...``;
* an ``ast`` walk fails on any attribute access whose base is the name ``re``
   (``re.compile``, ``re.match``, ...), and a text scan repeats the check so a
   line that the parser cannot see -- a string, a comment or a ``getattr``
   spelling -- still fails.

The checks are blunt on purpose: anything cleverer would be a hole a later
change could walk through.
"""

import ast
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[7]
SRC = PROJECT_ROOT / "src"
SOURCE_FILES = sorted(SRC.rglob("*.py"))

#: ``re.`` used as a module attribute. The lookbehind keeps ordinary words
#: ending in "re" (``store.``, ``score.``) out of the result; the pattern
#: itself is in a test, where regexes are allowed.
_RE_ATTRIBUTE = re.compile(r"(?<![\w.])re\.[A-Za-z_]")


def _imported_re(tree):
    """Return True when the module imports ``re`` in any form."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name == "re" for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module == "re":
                return True
    return False


def _used_re_attribute(tree):
    """Return True when ``re`` is read as the base of an attribute access."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            if node.value.id == "re":
                return True
    return False


def _relative(path):
    return str(path.relative_to(PROJECT_ROOT))


class TestSourceHasNoRegex:
    """The rule: no regex use anywhere under ``src``."""

    def test_source_tree_is_visible(self):
        """The walk has to find the tree, or the guard proves nothing."""
        assert len(SOURCE_FILES) > 50, f"only found {len(SOURCE_FILES)} files under {SRC}"

    def test_no_module_imports_re(self):
        offenders = []
        for path in SOURCE_FILES:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            if _imported_re(tree):
                offenders.append(_relative(path))
        assert not offenders, "src imports re: " + ", ".join(offenders)

    def test_no_module_level_re_use(self):
        offenders = []
        for path in SOURCE_FILES:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            if _used_re_attribute(tree):
                offenders.append(_relative(path))
        assert not offenders, "src reads re attributes: " + ", ".join(offenders)

    def test_no_textual_re_attribute(self):
        """The text scan repeats the attribute check without the parser.

        A line that mentions ``re.compile`` outside code (a doctest, a comment,
        a string handed to ``getattr``) is still a place a regex could hide.
        """
        offenders = []
        for path in SOURCE_FILES:
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if _RE_ATTRIBUTE.search(line):
                    offenders.append(f"{_relative(path)}:{lineno}: {line.strip()}")
        assert not offenders, "src mentions re attributes: " + "; ".join(offenders)
