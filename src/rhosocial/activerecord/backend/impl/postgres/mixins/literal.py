# src/rhosocial/activerecord/backend/impl/postgres/mixins/literal.py
"""PostgreSQL's tier-2 inline literal spellings.

``SQLDialectBase.format_literal`` is the single entry point for writing a value
as SQL text, and it resolves in three tiers: the portable scalars it knows, the
types a backend spells for itself, and the literal-construction protocol. This
module is PostgreSQL's second tier. PostgreSQL has three such types and no
more:

* **binary** -- ``'\\x0102'::bytea``, the hex bytea literal. PostgreSQL parses
  a quoted string whose first two characters are ``\\x`` as hexadecimal, and
  the cast says the target type out loud. Quoting the bytes as text would give
  a column whose value reads back as text. The hex input format needs
  PostgreSQL 9.0 or newer; every version this backend declares satisfies
  that, and the oldest configured scenario server measured 13.23.
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
  negative ``timedelta`` means. A zero span renders ``INTERVAL '0 seconds'``.

Everything else falls through to the base implementation, which is where the
portable forms and the protocol live. Nothing here is PostgreSQL-specific
except the three spellings above.

Measured 2026-10-17/18 against live servers reporting ``13.23 (Debian
13.23-1.pgdg13+1)``, ``17.11 (Debian 17.11-1.pgdg13+2)``, ``18.6 (Debian
18.6-1.pgdg13+2)`` and ``19beta4 (Debian 19~beta4-1.pgdg13+1)``: all four
accepted the three spellings, and a ``CREATE TABLE`` whose bytea, uuid and
interval columns carried them as ``DEFAULT`` values round-tripped byte-exactly
through ``INSERT ... DEFAULT VALUES`` + ``SELECT`` on all four.
"""

import datetime
import uuid
from typing import Any

#: One day, one hour, one minute -- in microseconds, the unit a
#: ``timedelta`` already normalizes to internally.
_DAY_US = 86_400_000_000
_HOUR_US = 3_600_000_000
_MINUTE_US = 60_000_000
_SECOND_US = 1_000_000


class PostgresLiteralValueMixin:
    """PostgreSQL's tier-2 branches of :meth:`format_literal`.

    Stands ahead of ``SQLDialectBase`` in the dialect's base list, unlike
    every other mixin: those add methods the base does not have, so their
    position is a readability choice, while this one overrides a method the
    base declares -- and a mixin placed after the class it overrides never
    wins the lookup.
    """

    def format_literal(self, value: Any) -> str:
        """Render *value* as inline SQL text, PostgreSQL's spelling where it has one.

        Args:
            value: The host value to write into the statement.

        Returns:
            The escaped literal text, without a bind parameter.

        Raises:
            TypeError: The base implementation refuses a type PostgreSQL has
                no literal form for, and this is the same refusal.
        """
        if isinstance(value, (bytes, bytearray)):
            # Lowercase hex is PostgreSQL's own bytea output form; the input
            # grammar accepts either case.
            return f"'\\x{bytes(value).hex()}'::bytea"
        if isinstance(value, uuid.UUID):
            # The base owns the quoting through the escape helper; the cast is
            # PostgreSQL's, the same shape as the UUID constants in
            # mixins/uuid.py.
            return f"{super().format_literal(str(value))}::uuid"
        if isinstance(value, datetime.timedelta):
            # The body text is generated from digits and separators alone -- no
            # host-controlled string reaches it, so quote-doubling has nothing
            # to double.
            return f"INTERVAL '{self._postgres_interval_text(value)}'"
        return super().format_literal(value)

    @staticmethod
    def _postgres_interval_text(value: datetime.timedelta) -> str:
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


__all__ = ["PostgresLiteralValueMixin"]
