# src/rhosocial/activerecord/backend/impl/postgres/_scan.py
"""Character-level scanning helpers for the PostgreSQL backend.

The backend's ``src`` tree stays free of the :mod:`re` module, so every
pattern the code needs is spelled as a hand-written scan.  The helpers here
are the shared vocabulary those scans use -- word-boundary prefix tests,
digit runs, balanced-parenthesis walks, quote-aware splitting -- kept in one
place so each module does not grow its own copy.

Every helper is a pure function over ``str``: no state, no configuration, and
no behaviour that depends on a compiled pattern object.
"""

from typing import List, Optional, Sequence, Tuple

_HEX_DIGITS = "0123456789abcdefABCDEF"
_SIGNS = ("+", "-")

#: A word character in the sense of the ``\\b`` assertion: alphanumeric or
#: underscore.  Python's ``\\w`` is Unicode-aware, and so is this.
_WORD_UNDERSCORE = "_"


def is_word_char(char: str) -> bool:
    """Return whether ``char`` is a word character in the ``\\b`` sense."""
    return char.isalnum() or char == _WORD_UNDERSCORE


def starts_with_word(text: str, words: Sequence[str]) -> Optional[str]:
    """Return the first of ``words`` that begins ``text`` at a word boundary.

    ``words`` is the alternation of a ``^(?:A|B)\\b`` test: the earliest
    spelling whose remaining text is not a word character wins, so ``INT``
    does not answer for ``INTEGER``.
    """
    for word in words:
        if not text.startswith(word):
            continue
        end = len(word)
        if end == len(text) or not is_word_char(text[end]):
            return word
    return None


def read_digits(text: str, index: int) -> Tuple[str, int]:
    """Read the run of decimal digits starting at ``index``.

    The second element is the index just past the run.  ``str.isdecimal`` is
    the predicate ``\\d`` uses for a ``str`` pattern.
    """
    start = index
    while index < len(text) and text[index].isdecimal():
        index += 1
    return text[start:index], index


def is_digit_run(text: str) -> bool:
    """Return whether ``text`` is a non-empty run of decimal digits."""
    return bool(text) and all(char.isdecimal() for char in text)


def digit_runs(text: str) -> Tuple[str, ...]:
    """Return every maximal run of decimal digits in ``text``, in order."""
    runs: List[str] = []
    index = 0
    while index < len(text):
        run, index = read_digits(text, index)
        if run:
            runs.append(run)
        else:
            index += 1
    return tuple(runs)


def first_digit_run(text: str) -> Optional[str]:
    """Return the first maximal run of decimal digits in ``text``, if any."""
    runs = digit_runs(text)
    return runs[0] if runs else None


def last_digit_run(text: str) -> Optional[str]:
    """Return the last maximal run of decimal digits in ``text``, if any."""
    runs = digit_runs(text)
    return runs[-1] if runs else None


def find_ci(text: str, needle: str, start: int = 0) -> int:
    """Find ``needle`` in ``text`` ignoring case, or return ``-1``.

    Only the comparison is case-folded, not the offsets, so the returned
    index is usable against the original text.  Case-folding a needle whose
    fold changes length (``ß``) cannot match, which is why this compares
    slices rather than folding the whole haystack.
    """
    if not needle:
        return start if 0 <= start <= len(text) else -1
    folded = needle.lower()
    length = len(needle)
    index = max(start, 0)
    limit = len(text) - length
    while index <= limit:
        if text[index : index + length].lower() == folded:
            return index
        index += 1
    return -1


def is_hex_run(text: str) -> bool:
    """Return whether ``text`` is a non-empty run of hexadecimal digits."""
    return bool(text) and all(char in _HEX_DIGITS for char in text)


def is_hex_text(text: str) -> bool:
    """Return whether ``text`` is hexadecimal, optionally carrying ``0x``.

    This is the ``^(?:0[xX])?[0-9A-Fa-f]+$`` test: the prefix is optional and
    the digit run after it must not be empty.
    """
    if len(text) >= 2 and text[:2] in ("0x", "0X"):
        text = text[2:]
    return is_hex_run(text)


def is_int_text(text: str) -> bool:
    """Return whether ``text`` is a whole number with an optional sign."""
    if text[:1] in _SIGNS:
        text = text[1:]
    return is_digit_run(text)


def is_number_text(text: str) -> bool:
    """Return whether ``text`` is a decimal number, optional sign and exponent.

    Accepts the shapes PostgreSQL numeric literals take: ``12``, ``12.``,
    ``12.5``, ``.5``, each with an optional ``+``/``-`` and an optional
    ``e``/``E`` exponent whose marker must be followed by digits.
    """
    if text[:1] in _SIGNS:
        text = text[1:]
    whole, index = read_digits(text, 0)
    if not whole:
        if not text.startswith("."):
            return False
        fraction, index = read_digits(text, 1)
        if not fraction:
            return False
    elif index < len(text) and text[index] == ".":
        index += 1
        _, index = read_digits(text, index)
    if index < len(text) and text[index] in ("e", "E"):
        index += 1
        if index < len(text) and text[index] in _SIGNS:
            index += 1
        exponent, index = read_digits(text, index)
        if not exponent:
            return False
    return index == len(text)


def has_ascii_letter_start(text: str) -> bool:
    """Return whether ``text`` begins with an ASCII letter or underscore."""
    if not text:
        return False
    first = text[0]
    return first.isascii() and (first.isalpha() or first == _WORD_UNDERSCORE)


def is_identifier(text: str, extra: str = "") -> bool:
    """Return whether ``text`` matches ``[A-Za-z_][A-Za-z0-9_<extra>]*``."""
    if not has_ascii_letter_start(text):
        return False
    for char in text[1:]:
        if char.isascii() and (char.isalnum() or char == _WORD_UNDERSCORE or char in extra):
            continue
        return False
    return True


def is_ascii_identifier(text: str) -> bool:
    """Return whether ``text`` matches ``[A-Za-z_][A-Za-z0-9_]*``."""
    return is_identifier(text)


def is_pg_identifier(text: str) -> bool:
    """Return whether ``text`` matches PostgreSQL's ``[A-Za-z_][A-Za-z0-9_$]*``."""
    return is_identifier(text, extra="$")


def balanced_content(text: str, open_index: int) -> Optional[Tuple[str, int]]:
    """Return the content inside the parenthesis at ``open_index``.

    The walk honours SQL quoting, so a parenthesis inside a string literal or
    a quoted identifier does not close the group and a doubled quote is an
    escape.  The second element is the index of the closing parenthesis; the
    whole result is ``None`` when the group never closes.
    """
    if open_index < 0 or open_index >= len(text) or text[open_index] != "(":
        return None
    depth = 0
    quote: Optional[str] = None
    index = open_index
    while index < len(text):
        char = text[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(text) and text[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in ("'", '"'):
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return text[open_index + 1 : index], index
        index += 1
    return None


def split_top_level(text: str) -> List[str]:
    """Split a comma-separated SQL fragment outside parentheses and quotes."""
    parts: List[str] = []
    current: List[str] = []
    depth = 0
    quote: Optional[str] = None
    for char in text:
        if quote:
            current.append(char)
            if char == quote:
                quote = None
        elif char in ("'", '"'):
            quote = char
            current.append(char)
        elif char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth = max(depth - 1, 0)
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if current:
        parts.append("".join(current).strip())
    return [part for part in parts if part]


def collapse_non_alnum(text: str, placeholder: str = "_") -> str:
    """Replace every run of non-ASCII-alphanumeric characters with ``placeholder``.

    The run collapses to one placeholder, so ``"a--b"`` becomes ``"a_b"``.
    Note that only ASCII letters and digits count as alphanumeric, exactly as
    ``[0-9A-Za-z]`` does; ``placeholder`` itself is not protected from
    collapsing into a neighbouring placeholder, which is why callers strip
    the result.
    """
    pieces: List[str] = []
    in_run = False
    for char in text:
        if char.isascii() and char.isalnum():
            pieces.append(char)
            in_run = False
        elif not in_run:
            pieces.append(placeholder)
            in_run = True
    return "".join(pieces)


_POSTGRES_VERSION_MARKER = "PostgreSQL "


def parse_postgres_version(text: str) -> Optional[Tuple[int, int, int]]:
    """Read ``(major, minor, patch)`` out of a ``SELECT version()`` answer.

    The spelling this replaces was a search for ``PostgreSQL (\\d+)`` with an
    optional ``.(\\d+)`` and a trailing ``[^\\d]*(\\d+)``.  The trailing group
    is reproduced faithfully: the patch is the first digit run after a
    non-digit run that follows the minor (or the major, when there is no
    minor), so ``PostgreSQL 9.6.24 on x86_64`` answers ``24``.  Reading it as
    the last run instead would answer ``64`` -- the x86_64 build tag -- which
    no caller has ever seen.  The purpose of this helper is to change how the
    answer is read, not what it says.
    """
    start = text.find(_POSTGRES_VERSION_MARKER)
    if start < 0:
        return None
    index = start + len(_POSTGRES_VERSION_MARKER)
    major, index = read_digits(text, index)
    if not major:
        return None
    minor = 0
    if index < len(text) and text[index] == ".":
        minor_run, after_minor = read_digits(text, index + 1)
        if minor_run:
            minor = int(minor_run)
            index = after_minor
    patch_run = first_digit_run(text[index:])
    patch = int(patch_run) if patch_run else 0
    return int(major), minor, patch


__all__ = [
    "balanced_content",
    "collapse_non_alnum",
    "digit_runs",
    "find_ci",
    "first_digit_run",
    "has_ascii_letter_start",
    "is_ascii_identifier",
    "is_digit_run",
    "is_hex_run",
    "is_hex_text",
    "is_identifier",
    "is_int_text",
    "is_number_text",
    "is_pg_identifier",
    "is_word_char",
    "last_digit_run",
    "parse_postgres_version",
    "read_digits",
    "split_top_level",
    "starts_with_word",
]
