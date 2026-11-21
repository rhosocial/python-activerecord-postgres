# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_trgm.py
"""PostgreSQL pg_trgm capability probes.

Index creation is an expression concern: use ``functions.pg_trgm.trgm_index``,
which returns a ``PostgresCreateIndexExpression`` rendered by the dialect's
single ``format_create_index_statement`` formatter. A dialect method takes the
expression, never a bag of scalar arguments.
"""

class PostgresPgTrgmMixin:
    """pg_trgm capability implementation."""

    def supports_pg_trgm_similarity(self) -> bool:
        """Check if pg_trgm supports similarity functions."""
        return self.check_extension_feature("pg_trgm", "similarity")

    def supports_pg_trgm_index(self) -> bool:
        """Check if pg_trgm supports trigram index."""
        return self.check_extension_feature("pg_trgm", "index")
