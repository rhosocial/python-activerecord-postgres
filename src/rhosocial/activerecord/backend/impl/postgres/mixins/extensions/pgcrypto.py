# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pgcrypto.py
"""
pgcrypto cryptographic functionality implementation.

This module provides the PostgresPgcryptoMixin class that adds support for
pgcrypto extension features.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class PostgresPgcryptoMixin:
    """pgcrypto cryptographic functionality implementation."""

    def supports_pgcrypto(self) -> bool:
        """Check if pgcrypto extension is available."""
        return self.is_extension_installed("pgcrypto")

    def format_gen_salt(self, algorithm: str = "md5") -> str:
        """Format salt generation.

        Args:
            algorithm: Hash algorithm (md5, sha256, etc.)

        Returns:
            SQL_gen_salt call
        """
        return f"gen_salt('{algorithm}')"

    def format_crypt(self, password: str, salt: str) -> str:
        """Format password hashing.

        Args:
            password: Password string
            salt: Salt value

        Returns:
            SQL crypt call
        """
        return f"crypt('{password}', '{salt}')"

    def format_encrypt_decrypt(
        self,
        data: str,
        key: str,
        mode: str = "encrypt",
        algorithm: str = "aes",
    ) -> str:
        """Format encryption/decryption.

        Args:
            data: Data to encrypt/decrypt
            key: Encryption key
            mode: Mode (encrypt/decrypt)
            algorithm: Cipher algorithm

        Returns:
            SQL encrypt/decrypt call
        """
        if mode == "encrypt":
            return f"encrypt('{data}'::bytea, '{key}'::bytea, '{algorithm}')"
        return f"decrypt('{data}'::bytea, '{key}'::bytea, '{algorithm}')"

    def format_random_bytes(self, length: int) -> str:
        """Format random bytes generation.

        Args:
            length: Number of bytes

        Returns:
            SQL gen_random_bytes call
        """
        return f"gen_random_bytes({length})"

    def format_hmac(self, data: str, key: str, algorithm: str = "sha256") -> str:
        """Format HMAC calculation.

        Args:
            data: Data to hash
            key: HMAC key
            algorithm: Hash algorithm

        Returns:
            SQL hmac call
        """
        return f"hmac('{data}', '{key}', '{algorithm}')"