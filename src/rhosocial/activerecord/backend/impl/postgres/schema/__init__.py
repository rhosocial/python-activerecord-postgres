# src/rhosocial/activerecord/backend/impl/postgres/schema/__init__.py
"""PostgreSQL schema differ."""

from .differ import PostgresSchemaDiffer

__all__ = ["PostgresSchemaDiffer"]
