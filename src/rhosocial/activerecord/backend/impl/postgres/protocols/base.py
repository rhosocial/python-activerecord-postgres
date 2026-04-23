# src/rhosocial/activerecord/backend/impl/postgres/protocols/base.py
"""Base dataclasses and common types for PostgreSQL protocols."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class PostgresExtensionInfo:
    """PostgreSQL extension information.

    Attributes:
        name: Extension name
        installed: Whether the extension is installed (enabled in database)
        available: Whether the extension is available (can be installed)
        version: Extension version number (only if installed)
        schema: Schema where the extension is installed
    """

    name: str
    installed: bool = False
    available: bool = False
    version: Optional[str] = None
    schema: Optional[str] = None
