# src/rhosocial/activerecord/backend/impl/postgres/adapters/hstore.py
"""
PostgreSQL hstore type adapter.

This module provides the PostgresHstoreAdapter for bidirectional
conversion between Python PostgresHstore objects and PostgreSQL
hstore string representations.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/hstore.html
"""

from typing import Any, Dict, Optional, Type, Union, List

from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter
from rhosocial.activerecord.backend.impl.postgres.types.hstore import PostgresHstore


class PostgresHstoreAdapter(BaseSQLTypeAdapter):
    """Adapter for converting between PostgresHstore and database values.

    Supports conversion from:
    - PostgresHstore objects → hstore literal string
    - Dict[str, Optional[str]] → hstore literal string (convenience)
    - str → passthrough (already formatted)
    - None → None

    And conversion to:
    - hstore string → PostgresHstore object
    - PostgresHstore → passthrough
    - None → None
    """

    def __init__(self):
        super().__init__()
        self._register_type(PostgresHstore, str)
        self._register_type(dict, str)

    def _do_to_database(
        self,
        value: Union[PostgresHstore, Dict[str, Optional[str]], str],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        if isinstance(value, PostgresHstore):
            return value.to_postgres_string()

        if isinstance(value, dict):
            hstore = PostgresHstore(data=value)
            return hstore.to_postgres_string()

        if isinstance(value, str):
            return value

        raise TypeError(
            f"Cannot convert {type(value).__name__} to hstore. "
            f"Expected PostgresHstore, dict, str, or None."
        )

    def _do_from_database(
        self,
        value: Union[str, PostgresHstore, Dict],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None,
    ) -> Optional[PostgresHstore]:
        if isinstance(value, PostgresHstore):
            return value

        if isinstance(value, str):
            return PostgresHstore.from_postgres_string(value)

        if isinstance(value, dict):
            return PostgresHstore(data=value)

        raise TypeError(
            f"Cannot convert {type(value).__name__} from database to PostgresHstore. "
            f"Expected str, dict, PostgresHstore, or None."
        )

    def to_database_batch(
        self,
        values: List[Union[PostgresHstore, Dict[str, Optional[str]], str, None]],
        target_type: Type,
    ) -> List[Optional[str]]:
        """Convert a batch of Python values to database format."""
        return [self.to_database(v, target_type) for v in values]

    def from_database_batch(
        self,
        values: List[Union[str, PostgresHstore, Dict, None]],
        source_type: Type,
    ) -> List[Optional[PostgresHstore]]:
        """Convert a batch of database values to Python PostgresHstore objects."""
        return [self.from_database(v, source_type) for v in values]
