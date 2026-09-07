# src/rhosocial/activerecord/backend/impl/postgres/mixins/sequence.py
"""PostgreSQL sequence feature support implementation."""

from typing import Tuple

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError


class PostgresSequenceMixin:
    """PostgreSQL sequence override implementation.

    All features are native, using version number for detection.
    """

    def supports_create_sequence(self) -> bool:
        return True

    def supports_drop_sequence(self) -> bool:
        return True

    def format_nextval(self, expr) -> Tuple[str, tuple]:
        """Format ``nextval('seq')`` with the sequence name inlined.

        Sequence functions in DDL contexts (``DEFAULT nextval(...)``) cannot
        be bind parameters, so the sequence name is formatted as a safely
        quoted SQL literal by the dialect.
        """
        if self.version < (10, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "sequence nextval",
                suggestion=(
                    f"PostgreSQL {self.version} — sequence expressions are "
                    "available on all supported versions; upgrade if this "
                    "check trips."
                ),
            )
        quoted = self._escape_sql_string(expr.sequence)
        return f"nextval('{quoted}')", ()

    def format_currval(self, expr) -> Tuple[str, tuple]:
        """Format ``currval('seq')`` with the sequence name inlined."""
        if self.version < (10, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                "sequence currval",
                suggestion=(
                    f"PostgreSQL {self.version} — sequence expressions are "
                    "available on all supported versions; upgrade if this "
                    "check trips."
                ),
            )
        quoted = self._escape_sql_string(expr.sequence)
        return f"currval('{quoted}')", ()
