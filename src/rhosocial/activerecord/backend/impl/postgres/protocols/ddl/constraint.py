# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/constraint.py
"""PostgreSQL-proprietary constraint capability protocol definitions.

This module contains the PostgresConstraintSupport protocol which defines
the interface for PostgreSQL's proprietary constraint features that are
not part of the SQL standard.

SQL standard constraint capabilities are defined in the core
ConstraintSupport protocol (rhosocial.activerecord.backend.dialect.protocols).

PostgreSQL-proprietary features:
- NOT VALID (add constraint without validating existing data)
- EXCLUDE constraints
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.protocols import ConstraintSupport

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.statements import (
        AlterConstraintAction,
        ColumnConstraint,
        TableConstraint,
        ValidateConstraintAction,
    )
    from rhosocial.activerecord.backend.impl.postgres.expression.ddl.exclude_constraint import (
        PostgresExcludeBranch,
    )


@runtime_checkable
class PostgresConstraintSupport(ConstraintSupport, Protocol):
    """PostgreSQL-proprietary constraint capabilities protocol.

    These features are PostgreSQL-specific and not part of the SQL standard.
    SQL standard constraint features (PRIMARY KEY, UNIQUE, CHECK, FK, etc.)
    are defined in the core ConstraintSupport protocol.

    Feature Source: Native support (no extension required)

    Official Documentation:
    - NOT VALID: https://www.postgresql.org/docs/current/sql-altertable.html
    - EXCLUDE: https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-EXCLUSION
    """

    def supports_constraint_enforced(self) -> bool:
        """Whether ``[NOT] ENFORCED`` is in the grammar at all (measured: PG18+).

        A grammar question with no kind in it: at 17 the clause is not in the
        grammar for any kind, at 18+ it is.  Which kinds may then carry it is
        the backend's narrowing -- CHECK and FOREIGN KEY only, measured with
        ``0A000`` for PRIMARY KEY, UNIQUE and EXCLUDE -- and it lives in the
        dialect's own ``_require_constraint_enforced``, called from its public
        ``format_table_constraint`` / ``format_column_constraint``.  See
        ``PostgresConstraintMixin`` for the SQLSTATEs.
        """
        ...

    def supports_alter_constraint_enforced(self) -> bool:
        """Whether the ``ALTER CONSTRAINT ... [NOT] ENFORCED`` verb exists (PG18+).

        Measured on PostgreSQL 19beta4: CHECK and FOREIGN KEY only, reported
        as ``42809`` rather than ``0A000`` because the constraint already
        exists.  The per-kind *version* boundary (FOREIGN KEY from 18, CHECK
        from 19) is the backend's narrowing and is not visible from here.
        """
        ...

    # Referential actions and match modes (SQL:1999).  Declared on this
    # protocol because they are part of PostgreSQL's constraint contract even
    # where core's shared default happens to agree; see the mixin for the
    # measurements.

    def supports_fk_on_delete(self) -> bool:
        """Whether ON DELETE referential actions are supported (measured: yes)."""
        ...

    def supports_fk_on_update(self) -> bool:
        """Whether ON UPDATE referential actions are supported (measured: yes).

        Pinned locally because it is not a universal SQL:1999 feature -- some
        backends have no ``ON UPDATE`` action at all.
        """
        ...

    def supports_fk_match(self) -> bool:
        """Whether MATCH {FULL|SIMPLE} is supported (measured: yes).

        Measured caveat: ``MATCH PARTIAL`` parses and is then refused with
        ``0A000: MATCH PARTIAL not yet implemented`` on 17, 18 and 19 alike,
        so ``True`` does not mean all three match types work.
        """
        ...

    # Constraint deferral (SQL:1999). PostgreSQL accepts DEFERRABLE only
    # for some kinds even though the grammar figure draws it as a tail
    # property shared by every branch.

    def supports_deferrable_constraint(self) -> bool:
        """Whether DEFERRABLE / INITIALLY DEFERRED/IMMEDIATE works at all.

        Per-kind truth lives in the ``supports_deferrable_*_constraint``
        switches below; ``True`` here is not an all-kinds grant.
        """
        ...

    def supports_deferrable_check_constraint(self) -> bool:
        """Whether CHECK accepts DEFERRABLE (measured: no, both levels)."""
        ...

    def supports_deferrable_not_null_constraint(self) -> bool:
        """Whether NOT NULL accepts DEFERRABLE (measured: no)."""
        ...

    def supports_deferrable_primary_key_constraint(self) -> bool:
        """Whether PRIMARY KEY accepts DEFERRABLE (measured: yes)."""
        ...

    def supports_deferrable_unique_constraint(self) -> bool:
        """Whether UNIQUE accepts DEFERRABLE (measured: yes)."""
        ...

    def supports_deferrable_foreign_key_constraint(self) -> bool:
        """Whether FOREIGN KEY accepts DEFERRABLE INITIALLY DEFERRED (yes)."""
        ...

    def supports_deferrable_exclude_constraint(self) -> bool:
        """Whether EXCLUDE accepts DEFERRABLE (measured: yes)."""
        ...

    def supports_enable_disable_constraint(self) -> bool:
        """Whether ``ENABLE``/``DISABLE CONSTRAINT`` is valid syntax (no)."""
        ...

    def supports_validate_constraint(self) -> bool:
        """Whether VALIDATE CONSTRAINT is supported.

        Measured on PostgreSQL 19beta4: yes, except on a ``NOT ENFORCED``
        constraint, which fails with ``55000``.
        """
        ...

    def supports_constraint_novalidate(self) -> bool:
        """Whether NOT VALID constraint option is supported.

        PostgreSQL-proprietary feature. Allows adding a constraint
        without validating existing data, then validating later
        with VALIDATE CONSTRAINT.

        Native feature, all PostgreSQL versions.  Measured on 19beta4:
        table-level ADD for CHECK and FOREIGN KEY only; PRIMARY KEY and
        UNIQUE are refused with ``0A000``, and at column level the clause
        does not exist for any kind (``42601 syntax error at or near
        "VALID"``).
        """
        ...

    def supports_exclude_constraint(self) -> bool:
        """Whether EXCLUDE constraints are supported.

        PostgreSQL-proprietary feature. Exclusion constraints ensure
        that no two rows overlap on specified columns using specified
        operators (e.g., && for range overlap).

        Native feature, all PostgreSQL versions.
        Requires appropriate operator class (e.g., btree_gist for gist).
        """
        ...

    # ALTER TABLE constraint lifecycle (SQL:92).  supports_add_constraint and
    # supports_drop_constraint are consulted by core before rendering, so they
    # are declared here to pin them rather than inherit the shared default.

    def supports_add_constraint(self) -> bool:
        """Whether ALTER TABLE ADD CONSTRAINT is supported (measured: yes, all kinds)."""
        ...

    def supports_drop_constraint(self) -> bool:
        """Whether ALTER TABLE DROP CONSTRAINT is supported (measured: yes).

        A missing constraint raises ``42704``; ``IF EXISTS`` is the only
        idempotent spelling.
        """
        ...

    def supports_rename_constraint(self) -> bool:
        """Whether ALTER TABLE RENAME CONSTRAINT is supported (measured: yes).

        Not yet in the core mixin; core S3 may rename it.  A missing
        constraint raises ``42704`` and there is no ``IF EXISTS`` form.
        """
        ...

    def supports_drop_constraint_if_exists(self) -> bool:
        """Whether ``DROP CONSTRAINT IF EXISTS`` is supported.

        Vendor extension (not in ISO/IEC 9075-2 §11.10). PostgreSQL has
        supported it since 9.6.  Measured: dropping a missing constraint with
        the qualifier is silently successful.
        """
        ...

    def format_table_constraint(self, expr: "TableConstraint") -> Tuple[str, tuple]:
        """Format a table constraint, narrowing the enforcement trait.

        ``ADD CONSTRAINT`` needs no override of its own any more: core renders
        the production, dispatches to the branch (which for PostgreSQL means
        :meth:`format_table_exclude_constraint` when the branch is an EXCLUDE)
        and appends ``NOT VALID``, so the only PostgreSQL-specific judgement is
        the one about the enforcement trait and it is made here.
        """
        ...

    def format_column_constraint(self, constraint: "ColumnConstraint") -> Tuple[str, tuple]:
        """Format a column constraint, refusing the enforcement trait outright.

        PostgreSQL has no column-level ``[NOT] ENFORCED`` for any kind or any
        version, so this seam refuses before delegating.
        """
        ...

    def format_alter_constraint_action(self, action: "AlterConstraintAction") -> Tuple[str, tuple]:
        """Format ALTER CONSTRAINT enforcement on the measured per-kind boundary."""
        ...

    def format_table_exclude_constraint(
        self, constraint: "PostgresExcludeBranch"
    ) -> Tuple[str, tuple]:
        """Format a PostgreSQL EXCLUDE body.

        Core has no EXCLUDE grammar to fall back on, so this formatter is the
        only implementation: the index access method, the per-element operators
        and the partial predicate have no generic spelling.
        """
        ...

    def format_validate_constraint_action(self, action: "ValidateConstraintAction") -> Tuple[str, tuple]:
        """Format VALIDATE CONSTRAINT."""
        ...
