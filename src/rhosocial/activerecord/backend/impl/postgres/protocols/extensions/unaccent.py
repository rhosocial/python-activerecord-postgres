# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/unaccent.py
"""unaccent extension protocol definition."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresUnaccentSupport(Protocol):
    """unaccent text normalization support.

    Extension Information:
    - Extension name: unaccent
    - Install command: CREATE EXTENSION unaccent;
    - Documentation: https://www.postgresql.org/docs/current/unaccent.html
    """

    def supports_unaccent(self) -> bool:
        """Whether the ``unaccent()`` function is available."""
        ...
