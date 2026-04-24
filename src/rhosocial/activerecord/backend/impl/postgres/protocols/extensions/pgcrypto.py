# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pgcrypto.py
"""pgcrypto extension protocol definition.

This module defines the protocol for pgcrypto cryptographic functions
in PostgreSQL.
"""

from typing import Protocol, Tuple, runtime_checkable


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

    def format_gen_salt(self, algorithm: str = "md5") -> str:
        """Format salt generation."""
        ...

    def format_crypt(self, password: str, salt: str) -> str:
        """Format password hashing."""
        ...

    def format_encrypt_decrypt(
        self,
        data: str,
        key: str,
        mode: str = "encrypt",
        algorithm: str = "aes",
    ) -> str:
        """Format encryption/decryption."""
        ...

    def format_random_bytes(self, length: int) -> str:
        """Format random bytes generation."""
        ...

    def format_hmac(self, data: str, key: str, algorithm: str = "sha256") -> str:
        """Format HMAC calculation."""
        ...

    def format_digest(self, data_expr: str, algorithm: str = "sha256") -> Tuple[str, tuple]:
        """Format digest function for hashing."""
        ...

    def format_pgp_encrypt(self, data_expr: str, key_expr: str) -> Tuple[str, tuple]:
        """Format pgp_sym_encrypt function."""
        ...

    def format_pgp_decrypt(self, data_expr: str, key_expr: str) -> Tuple[str, tuple]:
        """Format pgp_sym_decrypt function."""
        ...