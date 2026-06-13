# src/rhosocial/activerecord/backend/impl/postgres/mixins/features.py
"""PostgreSQL features feature support implementation."""


class PostgresFeaturesMixin:
    """PostgreSQL features override implementation.

    All features are native, using version number for detection.
    """

    def supports_generated_columns(self) -> bool:
        return self.version >= (12, 0, 0)

    def supports_cte_search_cycle(self) -> bool:
        return self.version >= (14, 0, 0)

    def supports_fetch_with_ties(self) -> bool:
        return self.version >= (13, 0, 0)

    def supports_call_statement(self) -> bool:
        return self.version >= (11, 0, 0)

    def supports_stored_procedure_transaction_control(self) -> bool:
        return self.version >= (11, 0, 0)

    def supports_sql_body_functions(self) -> bool:
        return self.version >= (14, 0, 0)

    def supports_nulls_not_distinct_unique(self) -> bool:
        return self.version >= (15, 0, 0)

    def supports_regexp_like(self) -> bool:
        return self.version >= (16, 0, 0)

    def supports_random_normal(self) -> bool:
        return self.version >= (16, 0, 0)

    def supports_json_table_nested_path(self) -> bool:
        return self.version >= (17, 0, 0)

    def supports_merge_with_cte(self) -> bool:
        return self.version >= (17, 0, 0)

    def supports_update_returning_old(self) -> bool:
        return self.version >= (17, 0, 0)
