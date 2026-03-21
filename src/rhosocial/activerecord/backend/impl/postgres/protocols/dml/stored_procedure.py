# src/rhosocial/activerecord/backend/impl/postgres/protocols/dml/stored_procedure.py
"""PostgreSQL stored procedure protocol definitions."""

from typing import Protocol, runtime_checkable, Optional, Tuple, List, Dict, Any


@runtime_checkable
class PostgresStoredProcedureSupport(Protocol):
    """PostgreSQL stored procedure protocol.

    Feature Source: Native support (no extension required)

    PostgreSQL stored procedures (introduced in PG 11):
    - CALL statement for procedure invocation
    - Transaction control within procedures (COMMIT/ROLLBACK)
    - SQL-standard function body syntax (PG 14+)

    Official Documentation:
    - Stored Procedures: https://www.postgresql.org/docs/current/xproc.html
    - CREATE PROCEDURE: https://www.postgresql.org/docs/current/sql-createprocedure.html

    Version Requirements:
    - CALL statement: PostgreSQL 11+
    - Transaction control: PostgreSQL 11+
    - SQL body functions: PostgreSQL 14+
    """

    def supports_call_statement(self) -> bool:
        """Whether CALL statement for stored procedures is supported.

        Native feature, PostgreSQL 11+.
        Enables calling stored procedures with transaction control.
        """
        ...

    def supports_stored_procedure_transaction_control(self) -> bool:
        """Whether stored procedures can manage transactions.

        Native feature, PostgreSQL 11+.
        Procedures can use COMMIT and ROLLBACK.
        """
        ...

    def supports_sql_body_functions(self) -> bool:
        """Whether SQL-standard function body syntax is supported.

        Native feature, PostgreSQL 14+.
        Enables BEGIN ATOMIC ... END syntax.
        """
        ...

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
        ...

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
        ...

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
        ...
