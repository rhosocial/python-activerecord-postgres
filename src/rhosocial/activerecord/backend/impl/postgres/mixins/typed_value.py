# src/rhosocial/activerecord/backend/impl/postgres/mixins/typed_value.py
"""PostgreSQL's spellings for the typed value nodes whose literal is its own.

PostgreSQL agrees with the base on the tier-1 kinds -- NULL, booleans
(TRUE/FALSE is the keyword form PostgreSQL itself uses), numbers, strings and
dates -- so none of those is overridden here. Three value types differ because
PostgreSQL's grammar says so:

* **binary** -- ``'\\x0102'::bytea``, the hex bytea literal. PostgreSQL
  parses a quoted string whose first two characters are ``\\x`` as
  hexadecimal, and the cast says the target type out loud. Quoting the bytes
  as text would give a column whose value reads back as text. The hex input
  format needs PostgreSQL 9.0 or newer; every version this backend declares
  satisfies that, and the oldest configured scenario server measured 13.23.
* **UUID** -- ``'...'::uuid``, the same shape the dialect's ``UUID_SQL``
  table already spells for the UUID constants (see ``mixins/uuid.py``): a
  bare quoted canonical form would be an untyped string literal, and a
  DEFAULT position needs the cast to say the column's type.
* **INTERVAL** -- ``INTERVAL '...'`` with the span decomposed into days,
  hours, minutes and seconds, every emitted component carrying the sign.
  Per-component signs are not decoration. Measured live: the compact time
  form with one leading minus, ``INTERVAL '-2 days 03:04:05.000006'``, parses
  as *minus two days plus 03:04:05* -- the sign does not carry into the time
  part -- so signing every component is the only spelling that says what a
  negative ``timedelta`` means. A zero span renders ``0 seconds``.

Measured 2026-10-17/18 against live servers reporting ``13.23 (Debian
13.23-1.pgdg13+1)``, ``17.11 (Debian 17.11-1.pgdg13+2)``, ``18.6 (Debian
18.6-1.pgdg13+2)`` and ``19beta4 (Debian 19~beta4-1.pgdg13+1)``: all four
accepted the three spellings above, and a ``CREATE TABLE`` whose bytea,
uuid and interval columns carried them as ``DEFAULT`` values round-tripped
byte-exactly through ``INSERT ... DEFAULT VALUES`` + ``SELECT`` on all four.
"""

import datetime
from typing import TYPE_CHECKING, Tuple

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.values import TypedValue

#: One day, one hour, one minute -- in microseconds, the unit a
#: ``timedelta`` already normalizes to internally.
_DAY_US = 86_400_000_000
_HOUR_US = 3_600_000_000
_MINUTE_US = 60_000_000
_SECOND_US = 1_000_000


class PostgresTypedValueMixin:
    """PostgreSQL's tier-2 value literals.

    Stands alone rather than inheriting :class:`TypedValueMixin`, exactly like
    the SQLite mixin it mirrors: the dialect composes this class ahead of the
    core one, and these three methods win by that position. Inheriting would
    put the base ahead of the other generic mixins and make the ordering the
    dialect has to reason about depend on this class instead.
    """

    def format_binary_value(self, expr: "TypedValue") -> Tuple[str, tuple]:
        """A bytea literal: ``'\\x0102'::bytea``, lowercase hex.

        Rendered from the bytes the node holds. The artifact carries base64
        (wire section 5) and PostgreSQL cannot decode base64 in SQL, so the
        conversion has to happen somewhere, and the dialect is the only layer
        that knows which literal form its engine accepts.
        """
        if not type(expr).inline_params:
            return self._bind_value(expr)
        return f"'\\x{bytes(expr.value).hex()}'::bytea", ()

    def format_uuid_value(self, expr: "TypedValue") -> Tuple[str, tuple]:
        """A UUID literal: the quoted canonical form with a ``::uuid`` cast.

        ``str(uuid.UUID)`` is the canonical lowercase text, and the escape
        helper owns the quoting; the cast is PostgreSQL's, the same shape as
        the UUID constants in ``mixins/uuid.py``.
        """
        if not type(expr).inline_params:
            return self._bind_value(expr)
        return f"{self.format_literal(str(expr.value))}::uuid", ()

    def format_interval_value(self, expr: "TypedValue") -> Tuple[str, tuple]:
        """An interval literal: ``INTERVAL '...'``, every component signed.

        A ``timedelta`` cannot say months or years, and PostgreSQL's interval
        happily takes the same span in several textual shapes, so the emitter
        fixes one: days, hours, minutes, then seconds with a six-digit
        fraction when microseconds are present, each preceded by its own
        minus sign when the span is negative. See the module docstring for the
        live-measured reason the sign cannot live on the first component
        alone.
        """
        if not type(expr).inline_params:
            return self._bind_value(expr)
        # The body text is generated from digits and separators alone -- no
        # host-controlled string reaches it, so quote-doubling has nothing
        # to double.
        return f"INTERVAL '{self._interval_text(expr.value)}'", ()

    @staticmethod
    def _interval_text(value: datetime.timedelta) -> str:
        """Decompose *value* into PostgreSQL's per-component interval text.

        The sign is decided once, on the whole span, and then written on
        every component: measured live, PostgreSQL applies a minus sign only
        to the quantity it precedes, so a single leading minus would leave
        the time part positive.
        """
        total = (value.days * _DAY_US) + (value.seconds * _SECOND_US) + value.microseconds
        negative = total < 0
        if negative:
            total = -total
        days, remainder = divmod(total, _DAY_US)
        hours, remainder = divmod(remainder, _HOUR_US)
        minutes, remainder = divmod(remainder, _MINUTE_US)
        seconds, microseconds = divmod(remainder, _SECOND_US)

        parts = []
        if days:
            parts.append(f"{days} days")
        if hours:
            parts.append(f"{hours} hours")
        if minutes:
            parts.append(f"{minutes} minutes")
        # Seconds are emitted when they carry something, and always for an
        # all-zero span so the literal never degenerates to an empty string.
        if seconds or microseconds or not parts:
            if microseconds:
                parts.append(f"{seconds}.{microseconds:06d} seconds")
            else:
                parts.append(f"{seconds} seconds")
        if negative:
            parts = [f"-{part}" for part in parts]
        return " ".join(parts)


__all__ = ["PostgresTypedValueMixin"]
