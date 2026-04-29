# src/rhosocial/activerecord/backend/impl/postgres/functions/pgcrypto.py
"""
PostgreSQL pgcrypto Extension Functions.

This module provides SQL expression generators for PostgreSQL pgcrypto
extension functions. All functions return Expression objects (FunctionCall,
BinaryExpression) that integrate with the expression-dialect architecture.

The pgcrypto extension provides cryptographic functions for PostgreSQL.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/pgcrypto.html

The pgcrypto extension must be installed:
    CREATE EXTENSION IF NOT EXISTS pgcrypto;

Supported functions:
- Salt generation: gen_salt
- Password hashing: crypt
- Encryption/Decryption: encrypt, decrypt
- PGP Encryption: pgp_sym_encrypt, pgp_sym_decrypt
- Random data: gen_random_bytes
- Hashing: digest, hmac

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
"""

from typing import Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports strings and existing BaseExpression objects.

    Args:
        dialect: The SQL dialect instance
        expr: Value to convert

    Returns:
        BaseExpression representing the value
    """
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif isinstance(expr, str):
        return core.Literal(dialect, expr)
    else:
        return core.Literal(dialect, expr)


# ============== Salt Generation ==============

def gen_salt(
    dialect: "SQLDialectBase",
    algorithm: str = "md5",
) -> core.FunctionCall:
    """Generate a random salt string for use with crypt().

    Generates a new random salt string for use in crypt(). The salt
    string also tells crypt() which algorithm to use.

    Supported algorithms:
    - 'bf': Blowfish (recommended)
    - 'md5': MD5
    - 'xdes': Extended DES
    - 'des': Traditional DES

    Args:
        dialect: The SQL dialect instance
        algorithm: Hash algorithm to use (default: 'md5')

    Returns:
        FunctionCall for gen_salt(algorithm)

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> gen_salt(dialect, 'bf')
        >>> gen_salt(dialect)  # uses md5
    """
    return core.FunctionCall(
        dialect, "gen_salt",
        _convert_to_expression(dialect, algorithm),
    )


# ============== Password Hashing ==============

def crypt(
    dialect: "SQLDialectBase",
    password: Union[str, "bases.BaseExpression"],
    salt: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Hash a password using the crypt() function.

    When salt is a gen_salt() result, this performs password hashing.
    When salt is an existing hash, this verifies the password.

    Args:
        dialect: The SQL dialect instance
        password: The password to hash (plaintext string or expression)
        salt: The salt string (typically from gen_salt()) or existing hash

    Returns:
        FunctionCall for crypt(password, salt)

    Example:
        >>> crypt(dialect, 'mypassword', gen_salt(dialect, 'bf'))
        >>> # For verification: crypt(dialect, 'mypassword', stored_hash)
    """
    return core.FunctionCall(
        dialect, "crypt",
        _convert_to_expression(dialect, password),
        _convert_to_expression(dialect, salt),
    )


# ============== Encryption/Decryption ==============

def encrypt(
    dialect: "SQLDialectBase",
    data: Union[str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
    algorithm: str = "aes",
) -> core.FunctionCall:
    """Encrypt data using the encrypt() function.

    Encrypts data using the specified cipher algorithm and key.
    The result is binary data (bytea).

    Supported algorithms:
    - 'aes': AES (Rijndael-128)
    - '3des': Triple DES
    - 'bf': Blowfish
    - 'cast5': CAST5
    - 'xtea': eXTEA

    Algorithm variants (append to algorithm name):
    - '-cbc': CBC mode (default)
    - '-ecb': ECB mode (not recommended)

    Args:
        dialect: The SQL dialect instance
        data: The data to encrypt (string or expression)
        key: The encryption key
        algorithm: Cipher algorithm (default: 'aes')

    Returns:
        FunctionCall for encrypt(data, key, algorithm)

    Example:
        >>> encrypt(dialect, 'secret data', 'mykey')
        >>> encrypt(dialect, 'secret data', 'mykey', 'bf')
        >>> encrypt(dialect, 'secret data', 'mykey', 'aes-cbc')
    """
    return core.FunctionCall(
        dialect, "encrypt",
        _convert_to_expression(dialect, data),
        _convert_to_expression(dialect, key),
        _convert_to_expression(dialect, algorithm),
    )


def decrypt(
    dialect: "SQLDialectBase",
    data: Union[str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
    algorithm: str = "aes",
) -> core.FunctionCall:
    """Decrypt data using the decrypt() function.

    Decrypts data that was encrypted with encrypt(). The algorithm
    must match the one used for encryption.

    Args:
        dialect: The SQL dialect instance
        data: The encrypted data (bytea or expression)
        key: The decryption key (must match encryption key)
        algorithm: Cipher algorithm (default: 'aes', must match encryption)

    Returns:
        FunctionCall for decrypt(data, key, algorithm)

    Example:
        >>> decrypt(dialect, encrypted_data, 'mykey')
        >>> decrypt(dialect, encrypted_data, 'mykey', 'bf')
    """
    return core.FunctionCall(
        dialect, "decrypt",
        _convert_to_expression(dialect, data),
        _convert_to_expression(dialect, key),
        _convert_to_expression(dialect, algorithm),
    )


# ============== Random Data ==============

def gen_random_bytes(
    dialect: "SQLDialectBase",
    length: Union[int, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate random bytes.

    Returns length cryptographically random bytes.

    Args:
        dialect: The SQL dialect instance
        length: Number of bytes to generate (positive integer)

    Returns:
        FunctionCall for gen_random_bytes(length)

    Example:
        >>> gen_random_bytes(dialect, 16)
        >>> gen_random_bytes(dialect, 32)
    """
    return core.FunctionCall(
        dialect, "gen_random_bytes",
        _convert_to_expression(dialect, length),
    )


# ============== Hash Functions ==============

def hmac(
    dialect: "SQLDialectBase",
    data: Union[str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
    algorithm: str = "sha256",
) -> core.FunctionCall:
    """Compute HMAC (Hash-based Message Authentication Code).

    Calculates hashed MAC for data with the given key.
    This is similar to digest() but can only be recalculated
    with the same key.

    Supported algorithms:
    - 'md5': MD5
    - 'sha1': SHA-1
    - 'sha224': SHA-224
    - 'sha256': SHA-256
    - 'sha384': SHA-384
    - 'sha512': SHA-512

    Args:
        dialect: The SQL dialect instance
        data: The data to hash
        key: The key for HMAC computation
        algorithm: Hash algorithm (default: 'sha256')

    Returns:
        FunctionCall for hmac(data, key, algorithm)

    Example:
        >>> hmac(dialect, 'message', 'secret_key')
        >>> hmac(dialect, 'message', 'secret_key', 'sha512')
    """
    return core.FunctionCall(
        dialect, "hmac",
        _convert_to_expression(dialect, data),
        _convert_to_expression(dialect, key),
        _convert_to_expression(dialect, algorithm),
    )


def digest(
    dialect: "SQLDialectBase",
    data: Union[str, "bases.BaseExpression"],
    algorithm: str = "sha256",
) -> core.FunctionCall:
    """Compute a hash digest.

    Computes a binary hash of the given data. Use this for
    general-purpose hashing when HMAC is not needed.

    Supported algorithms:
    - 'md5': MD5
    - 'sha1': SHA-1
    - 'sha224': SHA-224
    - 'sha256': SHA-256
    - 'sha384': SHA-384
    - 'sha512': SHA-512

    Args:
        dialect: The SQL dialect instance
        data: The data to hash
        algorithm: Hash algorithm (default: 'sha256')

    Returns:
        FunctionCall for digest(data, algorithm)

    Example:
        >>> digest(dialect, 'hello world')
        >>> digest(dialect, 'hello world', 'sha512')
    """
    return core.FunctionCall(
        dialect, "digest",
        _convert_to_expression(dialect, data),
        _convert_to_expression(dialect, algorithm),
    )


# ============== PGP Encryption ==============

def pgp_sym_encrypt(
    dialect: "SQLDialectBase",
    data: Union[str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Encrypt data with a symmetric PGP key.

    Encrypts data using a symmetric key following the OpenPGP standard.
    This is the recommended encryption method as it provides better
    security than the raw encrypt() function.

    The result is binary data containing both the encrypted data
    and metadata needed for decryption.

    Args:
        dialect: The SQL dialect instance
        data: The data to encrypt
        key: The symmetric encryption key

    Returns:
        FunctionCall for pgp_sym_encrypt(data, key)

    Example:
        >>> pgp_sym_encrypt(dialect, 'secret message', 'my_passphrase')
    """
    return core.FunctionCall(
        dialect, "pgp_sym_encrypt",
        _convert_to_expression(dialect, data),
        _convert_to_expression(dialect, key),
    )


def pgp_sym_decrypt(
    dialect: "SQLDialectBase",
    data: Union[str, "bases.BaseExpression"],
    key: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Decrypt data with a symmetric PGP key.

    Decrypts data that was encrypted with pgp_sym_encrypt().
    The key must match the one used for encryption.

    Args:
        dialect: The SQL dialect instance
        data: The encrypted data (bytea from pgp_sym_encrypt)
        key: The symmetric decryption key (must match encryption key)

    Returns:
        FunctionCall for pgp_sym_decrypt(data, key)

    Example:
        >>> pgp_sym_decrypt(dialect, encrypted_data, 'my_passphrase')
    """
    return core.FunctionCall(
        dialect, "pgp_sym_decrypt",
        _convert_to_expression(dialect, data),
        _convert_to_expression(dialect, key),
    )


__all__ = [
    # Salt generation
    "gen_salt",
    # Password hashing
    "crypt",
    # Encryption/Decryption
    "encrypt",
    "decrypt",
    # Random data
    "gen_random_bytes",
    # Hash functions
    "hmac",
    "digest",
    # PGP encryption
    "pgp_sym_encrypt",
    "pgp_sym_decrypt",
]
