# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/trigger.py
from typing import Tuple

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.objects import Table, Trigger


class PostgresTriggerMixin:
    """PostgreSQL trigger DDL implementation.

    PostgreSQL uses 'EXECUTE FUNCTION func_name()' syntax instead of
    the SQL:1999 standard 'EXECUTE func_name'.
    """

    def supports_trigger(self) -> bool:
        """Triggers are supported in all versions."""
        return True

    def supports_create_trigger(self) -> bool:
        """CREATE TRIGGER is supported in all versions."""
        return True

    def supports_drop_trigger(self) -> bool:
        """DROP TRIGGER is supported in all versions."""
        return True

    def supports_trigger_referencing(self) -> bool:
        """REFERENCING clause is supported since PostgreSQL 10."""
        return self.version_at_least((10, 0, 0))

    def supports_trigger_when(self) -> bool:
        """WHEN condition is supported in all modern versions."""
        return True

    def supports_statement_trigger(self) -> bool:
        """FOR EACH STATEMENT is supported."""
        return True

    def supports_instead_of_trigger(self) -> bool:
        """INSTEAD OF triggers are supported."""
        return True

    def supports_trigger_if_not_exists(self) -> bool:
        """PostgreSQL has no ``CREATE TRIGGER IF NOT EXISTS`` clause.

        The ``CREATE TRIGGER`` synopsis is ``CREATE [ OR REPLACE ]
        [ CONSTRAINT ] TRIGGER name ...`` on every version up to and
        including PostgreSQL 18; no release ever carried ``IF NOT EXISTS``.
        ``OR REPLACE`` (PostgreSQL 14+) is a different clause and a different
        expression field, not a spelling of this one.
        """
        return False

    def supports_trigger_if_exists(self) -> bool:
        """``DROP TRIGGER IF EXISTS`` is supported on every supported version.

        The synopsis is ``DROP TRIGGER [ IF EXISTS ] name ON table_name
        [ CASCADE | RESTRICT ]`` in the PostgreSQL 9.6 documentation (this
        backend's baseline) and in the current documentation.
        """
        return True

    def format_create_trigger_statement(self, expr) -> Tuple[str, tuple]:
        """Format CREATE TRIGGER statement (PostgreSQL syntax).

        PostgreSQL uses ``EXECUTE FUNCTION func_name()`` instead of standard ``EXECUTE func_name``.

        Supported expression attributes:

        - ``expr.if_not_exists`` — refused: PostgreSQL's ``CREATE TRIGGER``
          synopsis has no ``IF NOT EXISTS`` clause on any version.
        - ``expr.trigger`` — the trigger being created, which renders its own name.
        - ``expr.timing`` — ``BEFORE``, ``AFTER``, or ``INSTEAD OF``.
        - ``expr.events`` — list of event types (``INSERT``, ``UPDATE``, ``DELETE``, ``TRUNCATE``).
        - ``expr.update_columns`` — column list for ``UPDATE OF``.
        - ``expr.table`` — the target table, which renders its own name.
        - ``expr.referencing`` — ``REFERENCING`` clause string (PG 10+).
        - ``expr.level`` — ``FOR EACH ROW`` or ``FOR EACH STATEMENT``.
        - ``expr.condition`` — ``WHEN`` predicate expression.
        - ``expr.function`` — the function to execute, which renders its own
          name.

        Args:
            expr: Expression instance with trigger attributes

        Returns:
            Tuple of (SQL string, params tuple)

        Raises:
            UnsupportedFeatureError: ``expr.if_not_exists`` is set. PostgreSQL
                has no ``CREATE TRIGGER IF NOT EXISTS`` clause, so the
                statement is refused rather than rendered without the
                qualifier the caller asked for.
        """
        if not isinstance(expr.trigger, Trigger):
            raise TypeError(
                f"CreateTriggerStatement.trigger must be a Trigger, "
                f"got {type(expr.trigger).__name__}"
            )
        if not isinstance(expr.table, Table):
            raise TypeError(
                f"CreateTriggerStatement.table must be a Table, "
                f"got {type(expr.table).__name__}"
            )

        parts = ["CREATE TRIGGER"]

        if expr.if_not_exists:
            raise UnsupportedFeatureError(
                self.name,
                "CREATE TRIGGER IF NOT EXISTS",
                f"{self.name} has no CREATE TRIGGER IF NOT EXISTS clause; "
                f"the CREATE TRIGGER synopsis takes OR REPLACE instead.",
            )

        parts.append(expr.trigger.to_sql()[0])

        parts.append(expr.timing.value)

        if expr.update_columns:
            cols = ", ".join(self.format_identifier(c) for c in expr.update_columns)
            events_str = f"UPDATE OF {cols}"
        else:
            events_str = " OR ".join(e.value for e in expr.events)
        parts.append(events_str)

        parts.append("ON")
        parts.append(expr.table.to_sql()[0])

        if expr.referencing and self.supports_trigger_referencing():
            parts.append(expr.referencing)

        if expr.level:
            parts.append(expr.level.value)

        all_params = []
        if expr.condition and self.supports_trigger_when():
            cond_sql, cond_params = expr.condition.to_sql()
            parts.append(f"WHEN ({cond_sql})")
            all_params.extend(cond_params)

        parts.append("EXECUTE FUNCTION")
        parts.append(f"{expr.function.to_sql()[0]}()")

        return " ".join(parts), tuple(all_params)

    def format_drop_trigger_statement(self, expr) -> Tuple[str, tuple]:
        """Format DROP TRIGGER statement (PostgreSQL syntax).

        - ``expr.if_exists`` — add ``IF EXISTS``.
        - ``expr.trigger`` — the trigger being dropped, which renders its own name.
        - ``expr.table`` — optional ``ON <table>`` clause.

        Args:
            expr: Expression instance with trigger attributes

        Returns:
            Tuple of (SQL string, empty params tuple)

        Raises:
            TypeError: ``expr.trigger`` is not a :class:`Trigger`, or
                ``expr.table`` is set and is not a :class:`Table`. The table slot
                is optional -- PostgreSQL's ``DROP TRIGGER`` can name the trigger
                alone -- so its check is on kind, not on presence.
            UnsupportedFeatureError: ``expr.if_exists`` is set and the probe says
                the dialect has no ``IF EXISTS`` clause.
        """
        if not isinstance(expr.trigger, Trigger):
            raise TypeError(
                f"DropTriggerStatement.trigger must be a Trigger, "
                f"got {type(expr.trigger).__name__}"
            )
        if expr.table is not None and not isinstance(expr.table, Table):
            raise TypeError(
                f"DropTriggerStatement.table must be a Table, "
                f"got {type(expr.table).__name__}"
            )
        parts = ["DROP TRIGGER"]

        if expr.if_exists:
            if not self.supports_trigger_if_exists():
                raise UnsupportedFeatureError(
                    self.name,
                    "DROP TRIGGER IF EXISTS",
                    f"{self.name} does not support DROP TRIGGER IF EXISTS.",
                )
            parts.append("IF EXISTS")

        parts.append(expr.trigger.to_sql()[0])

        if expr.table is not None:
            parts.append("ON")
            parts.append(expr.table.to_sql()[0])

        return " ".join(parts), ()
