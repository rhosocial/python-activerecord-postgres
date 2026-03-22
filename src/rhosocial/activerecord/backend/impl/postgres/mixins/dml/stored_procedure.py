# src/rhosocial/activerecord/backend/impl/postgres/mixins/dml/stored_procedure.py
"""PostgreSQL stored procedure implementation mixin.

This module provides the PostgresStoredProcedureMixin class which implements
stored procedure support for PostgreSQL databases (introduced in PostgreSQL 11).
"""
from typing import Any, Dict, Optional, Tuple, List


class PostgresStoredProcedureMixin:
    """PostgreSQL stored procedure implementation.

    Stored procedures (introduced in PostgreSQL 11) support:
    - CALL statement for procedure invocation
    - Transaction control within procedures (COMMIT/ROLLBACK)
    """

    def supports_call_statement(self) -> bool:
        """CALL statement is supported since PostgreSQL 11."""
        return self.version >= (11, 0, 0)

    def supports_stored_procedure_transaction_control(self) -> bool:
        """Transaction control within procedures is supported since PostgreSQL 11."""
        return self.version >= (11, 0, 0)

    def supports_sql_body_functions(self) -> bool:
        """SQL-standard function body syntax is supported since PostgreSQL 14."""
        return self.version >= (14, 0, 0)

    def format_create_procedure_statement(
        self,
        name: str,
        parameters: Optional[List[Dict[str, str]]] = None,
        returns: Optional[str] = None,
        language: str = 'plpgsql',
        body: str = '',
        schema: Optional[str] = None,
        or_replace: bool = False,
        security: Optional[str] = None,
        cost: Optional[float] = None,
        rows: Optional[int] = None,
        set_params: Optional[Dict[str, str]] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE PROCEDURE statement.

        Args:
            name: Procedure name
            parameters: List of parameter dicts with 'name', 'type', 'mode' (IN/OUT/INOUT)
            returns: Return type (for functions, not procedures)
            language: Procedural language (default: plpgsql)
            body: Procedure body
            schema: Schema name
            or_replace: Add OR REPLACE clause
            security: 'DEFINER' or 'INVOKER'
            cost: Estimated execution cost
            rows: Estimated rows returned
            set_params: SET configuration parameters

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        if not self.supports_call_statement():
            raise ValueError("CREATE PROCEDURE requires PostgreSQL 11+")

        full_name = f"{schema}.{name}" if schema else name

        # OR REPLACE clause
        replace_clause = "OR REPLACE " if or_replace else ""

        sql = f"CREATE {replace_clause}PROCEDURE {full_name}"

        # Parameters
        if parameters:
            param_strs = []
            for param in parameters:
                param_str = ""
                mode = param.get('mode', 'IN')
                if mode and mode != 'IN':
                    param_str += f"{mode} "
                param_str += f"{param['name']} {param['type']}"
                if 'default' in param:
                    param_str += f" = {param['default']}"
                param_strs.append(param_str)
            sql += f"({', '.join(param_strs)})"
        else:
            sql += "()"

        # Language and options
        sql += f"\nLANGUAGE {language}"

        # Security
        if security:
            security = security.upper()
            if security not in ('DEFINER', 'INVOKER'):
                raise ValueError("Security must be 'DEFINER' or 'INVOKER'")
            sql += f"\nSECURITY {security}"

        # SET parameters
        if set_params:
            set_strs = [f"{k} = {v}" for k, v in set_params.items()]
            sql += f"\nSET {', '.join(set_strs)}"

        # Body
        sql += f"\nAS $$\n{body}\n$$"

        return sql, ()

    def format_drop_procedure_statement(
        self,
        name: str,
        parameters: Optional[List[Dict[str, str]]] = None,
        schema: Optional[str] = None,
        if_exists: bool = False,
        cascade: bool = False
    ) -> Tuple[str, tuple]:
        """Format DROP PROCEDURE statement.

        Args:
            name: Procedure name
            parameters: Parameter types for overload resolution
            schema: Schema name
            if_exists: Add IF EXISTS clause
            cascade: Add CASCADE clause

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        full_name = f"{schema}.{name}" if schema else name

        exists_clause = "IF EXISTS " if if_exists else ""

        sql = f"DROP PROCEDURE {exists_clause}{full_name}"

        # Parameter types for overload resolution
        if parameters:
            param_types = [p['type'] if isinstance(p, dict) else p for p in parameters]
            sql += f"({', '.join(param_types)})"

        if cascade:
            sql += " CASCADE"

        return sql, ()

    def format_call_statement(
        self,
        name: str,
        arguments: Optional[List[Any]] = None,
        schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CALL statement for stored procedure.

        Args:
            name: Procedure name
            arguments: List of argument values
            schema: Schema name

        Returns:
            Tuple of (SQL statement, parameters tuple)
        """
        if not self.supports_call_statement():
            raise ValueError("CALL statement requires PostgreSQL 11+")

        full_name = f"{schema}.{name}" if schema else name

        if arguments:
            placeholders = ', '.join(['%s'] * len(arguments))
            sql = f"CALL {full_name}({placeholders})"
            return sql, tuple(arguments)
        else:
            sql = f"CALL {full_name}()"
            return sql, ()
