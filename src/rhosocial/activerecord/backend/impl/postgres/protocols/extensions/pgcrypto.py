# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pgcrypto.py
"""pgcrypto extension protocol definition.

This module defines the protocol for pgcrypto cryptographic functions
in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPgcryptoSupport(Protocol):
    """pgcrypto cryptographic functions protocol.

    Feature Source: Extension support (requires pgcrypto extension)

    pgcrypto provides cryptographic functions:
    - Hashing (gen_salt, crypt)
    - Encryption (encrypt, decrypt)
    - Random bytes

    Extension Information:
    - Extension name: pgcrypto
    - Install command: CREATE EXTENSION pgcrypto;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/pgcrypto.html
    """

    def supports_pgcrypto(self) -> bool:
        """Whether pgcrypto is available."""
        ...