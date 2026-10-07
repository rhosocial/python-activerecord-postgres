# src/rhosocial/activerecord/backend/impl/postgres/mixins/cte.py
"""PostgreSQL cte feature support implementation."""


class PostgresCTEMixin:
    """PostgreSQL cte override implementation.

    ``AS [NOT] MATERIALIZED`` arrived in PostgreSQL 12. The boundary is
    measured, not assumed: the live servers in this repository's matrix
    reject both spellings with a syntax error through 11 and accept them
    from 12 on (9.6, 10, 11 reject; 12 through 19 accept, measured).
    """

    def supports_basic_cte(self) -> bool:
        return True

    def supports_recursive_cte(self) -> bool:
        return True

    def supports_materialized_cte(self) -> bool:
        """Whether the ``AS [NOT] MATERIALIZED`` CTE hint can be used.

        ``True`` from PostgreSQL 12 on. PostgreSQL 11 and older have no hint
        and reject both spellings with ``syntax error at or near
        "MATERIALIZED"`` (measured), so those versions must answer ``False``
        or core's ``format_cte_expression`` renders a hint the server refuses.
        """
        return self.version >= (12, 0, 0)
