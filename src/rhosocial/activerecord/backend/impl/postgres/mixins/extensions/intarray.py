# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/intarray.py
"""PostgreSQL intarray capability probes.

For SQL expression generation use ``functions/intarray.py``; for index DDL use
``functions.intarray.intarray_index``, which returns an expression node.
"""


class PostgresIntarrayMixin:
    """intarray integer array implementation."""

    def supports_intarray_operators(self) -> bool:
        """Check if intarray supports operators."""
        return self.check_extension_feature("intarray", "operators")

    def supports_intarray_functions(self) -> bool:
        """Check if intarray supports functions."""
        return self.check_extension_feature("intarray", "functions")

    def supports_intarray_index(self) -> bool:
        """Check if intarray supports index."""
        return self.check_extension_feature("intarray", "index")
