# src/rhosocial/activerecord/backend/impl/postgres/mixins/upsert.py
"""PostgreSQL upsert feature support implementation."""


class PostgresUpsertMixin:
    """PostgreSQL upsert override implementation.

    All features are native, using version number for detection.
    """

    def supports_upsert(self) -> bool:
        return self.version >= (9, 5, 0)

    def supports_on_conflict_clause(self) -> bool:
        return True

    def supports_multiple_on_conflict_clauses(self) -> bool:
        """PostgreSQL grammar allows only a single ON CONFLICT clause per INSERT."""
        return False

    def format_on_conflict_clause(self, expr) -> tuple:
        """Format ON CONFLICT clause for PostgreSQL.

        Overrides the base implementation to handle EXCLUDED pseudo-table
        references without quoting, as EXCLUDED is a special PostgreSQL
        keyword in ON CONFLICT context and must not be double-quoted.

        - ``expr.conflict_target`` — optional list of column names / expressions.
        - ``expr.do_nothing`` — ``DO NOTHING``.
        - ``expr.update_assignments`` — dict of ``{column: expression}`` for ``DO UPDATE SET``.
        - ``expr.update_where`` — optional WHERE predicate for the update.

        Args:
            expr: OnConflictClause expression instance

        Returns:
            Tuple of (SQL string, params tuple)

        """
        from rhosocial.activerecord.backend.expression import bases
        from rhosocial.activerecord.backend.expression.core import Column

        all_params = []
        parts = ["ON CONFLICT"]

        # Add conflict target if specified
        if expr.conflict_target:
            target_parts = []
            for target in expr.conflict_target:
                if isinstance(target, str):
                    target_parts.append(self.format_identifier(target))
                elif hasattr(target, 'to_sql'):
                    target_sql, target_params = target.to_sql()
                    target_parts.append(target_sql)
                    all_params.extend(target_params)
                else:
                    target_parts.append(self.format_identifier(str(target)))
            if target_parts:
                parts.append(f"({', '.join(target_parts)})")

        # Add DO NOTHING or DO UPDATE
        if expr.do_nothing:
            parts.append("DO NOTHING")
        elif expr.update_assignments:
            update_parts = []
            for col, expr_val in expr.update_assignments.items():
                if isinstance(expr_val, Column) and getattr(expr_val, 'table', None) == 'EXCLUDED':
                    # EXCLUDED is a special pseudo-table in PostgreSQL ON CONFLICT.
                    # It must NOT be double-quoted, only the column name should be quoted.
                    val_sql = f'EXCLUDED.{self.format_identifier(expr_val.name)}'
                    update_parts.append(f"{self.format_identifier(col)} = {val_sql}")
                elif isinstance(expr_val, bases.BaseExpression):
                    val_sql, val_params = expr_val.to_sql()
                    update_parts.append(f"{self.format_identifier(col)} = {val_sql}")
                    all_params.extend(val_params)
                else:
                    update_parts.append(f"{self.format_identifier(col)} = {self.get_parameter_placeholder()}")
                    all_params.append(expr_val)

            parts.append(f"DO UPDATE SET {', '.join(update_parts)}")

            # Add WHERE clause if specified
            if expr.update_where:
                where_sql, where_params = expr.update_where.to_sql()
                parts.append(f"WHERE {where_sql}")
                all_params.extend(where_params)
        else:
            parts.append("DO NOTHING")

        return " ".join(parts), tuple(all_params)
