# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/tablefunc.py
"""tablefunc capability probes and typed-query renderers."""

from typing import Tuple

from ...expression.tablefunc import (
    PostgresConnectbyFullStatement,
    PostgresCrosstabWithDefinitionStatement,
)


class PostgresTablefuncMixin:
    """tablefunc table functions implementation."""

    def supports_tablefunc_crosstab(self) -> bool:
        return self.check_extension_feature("tablefunc", "crosstab")

    def supports_tablefunc_connectby(self) -> bool:
        return self.check_extension_feature("tablefunc", "connectby")

    def supports_tablefunc_normal_rand(self) -> bool:
        return self.check_extension_feature("tablefunc", "normal_rand")

    def format_postgres_crosstab_with_definition_statement(
        self, expr: PostgresCrosstabWithDefinitionStatement
    ) -> Tuple[str, tuple]:
        if expr.categories_sql:
            return (
                f"SELECT * FROM crosstab('{expr.source_sql}', "
                f"'{expr.categories_sql}') AS ct({expr.output_columns})",
                (),
            )
        return (
            f"SELECT * FROM crosstab('{expr.source_sql}') "
            f"AS ct({expr.output_columns})",
            (),
        )

    def format_postgres_connectby_full_statement(
        self, expr: PostgresConnectbyFullStatement
    ) -> Tuple[str, tuple]:
        return (
            f"SELECT * FROM connectby('{expr.table_name}', '{expr.key_column}', "
            f"'{expr.parent_column}', '{expr.start_value}', {expr.max_depth}, "
            f"'{expr.branch_delim}') "
            "AS t(keyid text, parent_keyid text, level int, branch text)",
            (),
        )
