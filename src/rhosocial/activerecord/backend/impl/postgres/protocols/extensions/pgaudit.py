# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pgaudit.py
"""pgaudit extension protocol definition.

This module defines the protocol for pgaudit audit logging
functionality in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgauditSupport(Protocol):
    """pgaudit audit logging extension protocol.

    Feature Source: Extension support (requires pgaudit extension)

    pgaudit provides session audit logging:
    - Role-based auditing
    - Statement logging
    - DDL auditing

    Extension Information:
    - Extension name: pgaudit
    - Install command: CREATE EXTENSION pgaudit;
    - Minimum version: 1.0
    - Documentation: https://github.com/pgaudit/pgaudit
    """

    def supports_pgaudit(self) -> bool:
        """Whether pgaudit extension is available."""
        ...

    def supports_pgaudit_session(self) -> bool:
        """Whether pgaudit supports session auditing."""
        ...