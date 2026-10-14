# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/constraint.py
"""PostgreSQL constraint capability declarations.

Every switch here is declared **in this package**, even where core's
``ConstraintMixin`` already defaults it to the same value.  That is deliberate:
``ConstraintMixin`` is shared with the dummy dialect and is the kind of default
that can move, and a silent move of a core default would move PostgreSQL's
measured contract with it.  A declaration here is the pin.

SQL standard constraint capability defaults are defined in the core
ConstraintMixin (rhosocial.activerecord.backend.dialect.mixins); the switches
this backend relies on are re-declared locally and carry the measurement that
fixed them.

Two measured formatting rules that are **not** capability switches, because
``supports_*`` cannot express "supported but only in this spelling":

``NULLS NOT DISTINCT`` must PRECEDE the column list.  PostgreSQL 19beta4::

    ALTER TABLE t ADD CONSTRAINT u UNIQUE NULLS NOT DISTINCT (a)  -- accepted
    ALTER TABLE t ADD CONSTRAINT u UNIQUE (a) NULLS NOT DISTINCT
    -> 42601: syntax error at or near "NULLS"

So a renderer that appends the qualifier after the column list emits
invalid SQL for a feature the server does support.

``MATCH PARTIAL`` parses but is not implemented, so it is not the "accepted
and then silently dropped" case -- it fails loudly, with a feature-not-
supported code, and the same wording on every version measured (17 / 18 / 19)::

    ALTER TABLE t ADD CONSTRAINT f FOREIGN KEY (a) REFERENCES r(a) MATCH PARTIAL
    -> 0A000: MATCH PARTIAL not yet implemented

``MATCH FULL`` and ``MATCH SIMPLE`` are accepted.  See
:meth:`PostgresConstraintMixin.supports_fk_match`.
"""

import re
from typing import Any, List, Tuple, TYPE_CHECKING, cast

from rhosocial.activerecord.backend.expression.statements import (
    ColumnCheckConstraint,
    ColumnForeignKeyConstraint,
    ColumnPrimaryKeyConstraint,
    ColumnUniqueConstraint,
    TableCheckConstraint,
    TableExcludeConstraint,
    TableForeignKeyConstraint,
    TablePrimaryKeyConstraint,
    TableUniqueConstraint,
)

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect.mixins.ddl_table import ConstraintMixin
    from rhosocial.activerecord.backend.expression.statements import (
        AlterConstraint,
        ColumnConstraint,
        TableConstraint,
        ValidateConstraint,
    )

    class _ConstraintMixinBase(ConstraintMixin):
        version: Tuple[int, int, int]
        name: str

        def format_identifier(self, identifier: str, need_quote: bool = True) -> str:
            return identifier

else:

    class _ConstraintMixinBase:
        pass


class PostgresConstraintMixin(_ConstraintMixinBase):
    """PostgreSQL constraint capability declarations.

    Every value below is either a live server measurement or a documented
    grammar fact, and the measured ones carry the server version, the exact
    SQLSTATE and the server's own message, so a future maintainer can tell
    a fact apart from a guess.

    Measurements were taken against the three instances
    ``tests/config/postgres_scenarios.yaml`` names, all on ``192.168.1.3``,
    with psycopg 3.3.6, each in a transaction that was rolled back:

    * ``postgres_19beta4`` -- port 16690, ``server_version_num`` 190000,
      ``PostgreSQL 19beta4 (Debian 19~beta4-1.pgdg13+1) on
      aarch64-unknown-linux-gnu``
    * ``postgres_18`` -- port 16689, ``server_version_num`` 180006
    * ``postgres_17`` -- port 16688, ``server_version_num`` 170011

    Only ``TEMP`` tables inside rolled-back transactions were used.

    What matters is what the server *refuses*, because the
    ``column_constraint`` / ``table_constraint`` grammar figures draw
    ``[DEFERRABLE ...] [INITIALLY ...] [ENFORCED | NOT ENFORCED]`` as tail
    properties shared by every branch, which reads as "applies to all
    constraint kinds".  It does not: the accept surface is narrower than the
    grammar surface, and only a server can show you where.  Per "backend
    facts win over documentation", the switches below are set to the
    measured accept surface.

    Measured matrix (PostgreSQL 19beta4)::

        feature      column level                        table level
        -----------  -----------------------------------  -----------------------------
        NOT ENFORCED CHECK/FK yes; PK/UNIQUE no           CHECK/FK yes; PK/UNIQUE/EXCLUDE no
        NOT VALID    none -- syntax error at "VALID"     CHECK/FK yes; PK/UNIQUE no
        DEFERRABLE   PK/UNIQUE/FK yes; CHECK/            PK/UNIQUE/FK yes; CHECK no
                     NOT NULL no
        INITIALLY    PK/UNIQUE/FK yes; CHECK no          PK/UNIQUE/FK yes; CHECK no
        NO INHERIT   NOT NULL/CHECK yes                  NOT NULL/CHECK yes

    Enforcement version boundaries, measured on all three instances rather
    than read off a version table (the 19beta4 instance alone cannot separate
    18 from 19, which is why the older ones matter)::

        statement                                   17      18      19
        -----------------------------------------  ------  ------  ------
        ADD CONSTRAINT ... NOT ENFORCED (CHECK/FK) 42601  OK      OK
        ADD CONSTRAINT ... NOT ENFORCED (PK)      42601  0A000   0A000
        ALTER CONSTRAINT ... [NOT] ENFORCED (FK)   42601  OK      OK
        ALTER CONSTRAINT ... [NOT] ENFORCED (CK)   42601  42809   OK

    Two things fall out of that table and are easy to get wrong: the ``>= 18``
    gate on the ADD side covers CHECK and FOREIGN KEY together, while on the
    ALTER side the two kinds split across two releases; and ``42601`` on 17
    means the clause is not in the grammar at all, whereas ``42809`` on 18 for
    CHECK means it is parsed and then refused on an existing constraint.

    SQL standard constraint capability defaults are in ConstraintMixin.
    """

    # Referential actions and match modes (SQL:1999).  Declared here because
    # ``ON UPDATE`` is the clause that actually distinguishes PostgreSQL from
    # several other backends (Oracle has no ON UPDATE action at all), so it
    # must not be left riding on a shared core default.

    def supports_fk_on_delete(self) -> bool:
        """True: ``ON DELETE`` referential actions are supported.

        PostgreSQL 19beta4, all accepted at ``ADD CONSTRAINT`` time::

            ALTER TABLE t ADD CONSTRAINT f FOREIGN KEY (a) REFERENCES r(a) ON DELETE CASCADE
            ALTER TABLE t ADD CONSTRAINT f FOREIGN KEY (a) REFERENCES r(a) ON DELETE SET NULL

        Core already defaults this to ``True``; the declaration pins it to the
        measurement so a move of the shared default cannot change what
        PostgreSQL renders.
        """
        return True

    def supports_fk_on_update(self) -> bool:
        """True: ``ON UPDATE`` referential actions are supported.

        PostgreSQL 19beta4, all accepted at ``ADD CONSTRAINT`` time::

            ALTER TABLE t ADD CONSTRAINT f FOREIGN KEY (a) REFERENCES r(a) ON UPDATE CASCADE
            ALTER TABLE t ADD CONSTRAINT f FOREIGN KEY (a) REFERENCES r(a) ON UPDATE SET NULL
            ALTER TABLE t ADD CONSTRAINT f FOREIGN KEY (a) REFERENCES r(a) ON UPDATE RESTRICT

        Worth pinning locally because this is *not* a universal SQL:1999
        feature.  Oracle has no ``ON UPDATE`` action, and the cross-backend
        table in ``python-activerecord/.claude/plan/2026-10-11/
        alter-table-live-findings.md`` lists PostgreSQL among the backends
        that do have it.  Left on a shared core default, a change made for a
        backend without the clause would silently start rendering it here.

        The clause name is unaffected by core's two-level constraint nodes:
        ``supports_foreign_key_on_update`` is about the REFERENCES clause, not
        about which node spells the constraint.
        """
        return True

    def supports_fk_match(self) -> bool:
        """True: ``MATCH {FULL|SIMPLE}`` is supported -- ``MATCH PARTIAL`` is not.

        PostgreSQL 19beta4, all three spelled on one foreign key::

            ALTER TABLE t ADD CONSTRAINT f FOREIGN KEY (a) REFERENCES r(a) MATCH FULL
            -- accepted
            ALTER TABLE t ADD CONSTRAINT f FOREIGN KEY (a) REFERENCES r(a) MATCH SIMPLE
            -- accepted
            ALTER TABLE t ADD CONSTRAINT f FOREIGN KEY (a) REFERENCES r(a) MATCH PARTIAL
            -> 0A000: MATCH PARTIAL not yet implemented

        ``MATCH PARTIAL`` is in the grammar and is **parsed** -- the refusal
        comes from the semantic check, with the feature-not-supported
        SQLSTATE, not from a parser error.  The same 0A000 and the same wording
        come back on all three instances measured (170011 / 180006 / 190000)
        and at CREATE TABLE time as well as ``ADD CONSTRAINT`` time, so this is
        a long-standing gap rather than a regression.

        So the switch is ``True``: ``MATCH FULL`` and ``MATCH SIMPLE`` are
        genuinely supported, and turning the switch off would refuse clauses
        the server honours.  What ``supports_*`` cannot express is "PARTIAL is
        accepted by the parser and then refused" -- recorded here so a renderer
        does not treat a ``True`` as "all three match types work".
        """
        return True

    # Constraint deferral (SQL:1999). DEFERRABLE is NOT an all-kinds feature
    # on PostgreSQL even though the grammar figure draws it as a tail
    # property shared by every branch: CHECK and NOT NULL are refused at
    # both levels.  These per-kind switches record the measured surface.
    # (A core revision may rename them, or split them per constraint level;
    # the measurement below is level-agnostic for every kind except NOT
    # NULL, which exists only at column level.)

    def supports_deferrable_constraint(self) -> bool:
        """Whether ``DEFERRABLE`` / ``INITIALLY ...`` is supported at all.

        PostgreSQL 19beta4: accepted for PRIMARY KEY, UNIQUE and FOREIGN KEY
        at both column and table level::

            ALTER TABLE t ADD CONSTRAINT p PRIMARY KEY (a) DEFERRABLE
            CREATE TABLE ct (a int PRIMARY KEY DEFERRABLE)          -- accepted
            CREATE TABLE ct (a int, PRIMARY KEY (a) DEFERRABLE)     -- accepted

        ``True`` here must NOT be read as "every constraint kind accepts
        DEFERRABLE" -- the grammar figure says that and the server
        contradicts it.  The per-kind truth is in
        :meth:`supports_deferrable_check_constraint` and friends.
        """
        return True

    def supports_deferrable_check_constraint(self) -> bool:
        """Whether CHECK accepts ``DEFERRABLE`` / ``INITIALLY ...``.

        PostgreSQL 19beta4: **no, at both levels**::

            ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0) DEFERRABLE
            -> 0A000: CHECK constraints cannot be marked DEFERRABLE

            CREATE TABLE ct (a int, CHECK (a > 0) DEFERRABLE)
            -> 0A000: CHECK constraints cannot be marked DEFERRABLE

            CREATE TABLE ct (a int CHECK (a > 0) DEFERRABLE)
            -> 42601: misplaced DEFERRABLE clause

        (The column-level ``NOT NULL`` variant of that last error is
        recorded on :meth:`supports_deferrable_not_null_constraint`.)

        ``INITIALLY ...`` alone is refused too (column level ``42601:
        misplaced INITIALLY DEFERRED clause``; table level the 0A000
        above), because deferral timing only makes sense on a deferrable
        constraint.
        """
        return False

    def supports_deferrable_not_null_constraint(self) -> bool:
        """Whether NOT NULL accepts ``DEFERRABLE``.

        PostgreSQL 19beta4: **no**::

            CREATE TABLE ct (a int NOT NULL DEFERRABLE)
            -> 42601: misplaced DEFERRABLE clause
        """
        return False

    def supports_deferrable_primary_key_constraint(self) -> bool:
        """Whether PRIMARY KEY accepts ``DEFERRABLE`` / ``INITIALLY ...``.

        PostgreSQL 19beta4: yes at both levels::

            ALTER TABLE t ADD CONSTRAINT p PRIMARY KEY (a) DEFERRABLE    -- accepted
            ALTER TABLE t ADD CONSTRAINT p PRIMARY KEY (a) INITIALLY DEFERRED
            CREATE TABLE ct (a int PRIMARY KEY DEFERRABLE)               -- accepted
            CREATE TABLE ct (a int, PRIMARY KEY (a) DEFERRABLE)          -- accepted
        """
        return True

    def supports_deferrable_unique_constraint(self) -> bool:
        """Whether UNIQUE accepts ``DEFERRABLE`` / ``INITIALLY ...``.

        PostgreSQL 19beta4: yes at both levels::

            ALTER TABLE t ADD CONSTRAINT u UNIQUE (a) DEFERRABLE          -- accepted
            CREATE TABLE ct (a int UNIQUE DEFERRABLE)                    -- accepted
            CREATE TABLE ct (a int, UNIQUE (a) DEFERRABLE)               -- accepted
        """
        return True

    def supports_deferrable_foreign_key_constraint(self) -> bool:
        """Whether FOREIGN KEY accepts ``DEFERRABLE INITIALLY DEFERRED``.

        PostgreSQL 19beta4: yes at both levels, and the deferral is really
        recorded (not accepted-and-ignored).  After::

            ALTER TABLE t ADD CONSTRAINT fkd FOREIGN KEY (a)
                REFERENCES parent(a) DEFERRABLE INITIALLY DEFERRED

        ``pg_constraint`` shows ``condeferrable = true`` and
        ``condeferred = true`` (the sibling ``NOT ENFORCED`` FK on the same
        table shows both ``false``).
        """
        return True

    def supports_deferrable_exclude_constraint(self) -> bool:
        """Whether EXCLUDE accepts ``DEFERRABLE``.

        PostgreSQL 19beta4: yes -- the one kind where the grammar figure's
        "tail property for all branches" promise actually holds::

            ALTER TABLE t ADD CONSTRAINT e EXCLUDE USING gist (a WITH =) DEFERRABLE
            CREATE TEMP TABLE ct (a int, CONSTRAINT e EXCLUDE USING gist (a WITH =) DEFERRABLE)
        """
        return True

    # Constraint enforcement control (SQL:2016)

    def supports_constraint_enforced(self) -> bool:
        """Whether ``ENFORCED`` / ``NOT ENFORCED`` is in the grammar at all.

        PostgreSQL 19beta4, both ``ALTER TABLE ADD CONSTRAINT`` and
        ``ALTER TABLE ALTER CONSTRAINT`` and CREATE TABLE:

        * CHECK and FOREIGN KEY: accepted.
        * PRIMARY KEY / UNIQUE: ``0A000: PRIMARY KEY constraints cannot be
          marked NOT ENFORCED`` / ``0A000: UNIQUE constraints cannot be
          marked NOT ENFORCED`` (on ``ALTER CONSTRAINT`` the code differs
          because the constraint already exists: ``42809: cannot alter
          enforceability of constraint ... Only foreign key and check
          constraints can change enforceability.``).  At column level the
          clause is a parse error instead: ``42601: misplaced NOT ENFORCED
          clause``.
        * EXCLUDE: ``0A000: EXCLUDE constraints cannot be marked NOT
          ENFORCED``.

        The version boundary is PG18, and it is a *grammar* boundary:
        ``NOT ENFORCED`` enters the CREATE TABLE grammar in 18.  Measured on
        all three instances::

            -- 170011
            ALTER TABLE t ADD CONSTRAINT ck CHECK (a > 0) NOT ENFORCED
            -> 42601: syntax error at or near "ENFORCED"
            -- 180006 / 190000: accepted for CHECK and FOREIGN KEY

        At PG17 the clause does not exist for *any* kind -- the parser never
        sees it -- whereas at 18+ the PK/UNIQUE refusals above are semantic.
        The two look similar from the outside and are not the same defect.

        This answers the grammar question only, with no kind: which kinds may
        carry the clause is this backend's narrowing, and it lives in
        :meth:`_require_constraint_enforced`, called from
        :meth:`format_table_constraint` and :meth:`format_column_constraint`.
        """
        return self.version >= (18, 0, 0)

    def supports_alter_constraint_enforced(self) -> bool:
        """Whether ``ALTER TABLE ... ALTER CONSTRAINT ... [NOT] ENFORCED`` works.

        PostgreSQL 19beta4.  The accept surface is the same as for ADD
        CONSTRAINT -- only CHECK and FOREIGN KEY -- but the rejection is
        reported against the existing object rather than at parse time, so
        the SQLSTATE differs::

            ALTER TABLE t ALTER CONSTRAINT <check> NOT ENFORCED   -- accepted
            ALTER TABLE t ALTER CONSTRAINT <fk>    NOT ENFORCED   -- accepted
            ALTER TABLE t ALTER CONSTRAINT <pk>    NOT ENFORCED
            -> 42809: cannot alter enforceability of constraint "pk" ...
                     HINT: Only foreign key and check constraints can change enforceability.
            ALTER TABLE t ALTER CONSTRAINT <unique> NOT ENFORCED  -- same 42809

        Version boundaries, measured on all three instances rather than
        derived from a version table.  The two kinds split across two
        releases here, which is why this needed the 18 instance and not just
        19beta4::

            -- 170011, both kinds
            ALTER TABLE t ALTER CONSTRAINT ck NOT ENFORCED
            -> 42601: syntax error at or near "ENFORCED"
            ALTER TABLE t ALTER CONSTRAINT fk NOT ENFORCED
            -> 42601: syntax error at or near "ENFORCED"

            -- 180006: FOREIGN KEY works, CHECK does not
            ALTER TABLE t ALTER CONSTRAINT fk NOT ENFORCED    -- accepted
            ALTER TABLE t ALTER CONSTRAINT fk  ENFORCED       -- accepted (back)
            ALTER TABLE t ALTER CONSTRAINT ck NOT ENFORCED
            -> 42809: cannot alter enforceability of constraint "ck" of relation "t"

            -- 190000: both kinds, both directions
            ALTER TABLE t ALTER CONSTRAINT ck NOT ENFORCED    -- accepted
            ALTER TABLE t ALTER CONSTRAINT ck  ENFORCED       -- accepted

        So FOREIGN KEY is PG18 and CHECK is PG19, and both were previously
        recorded as doc-derived.  They are now measured.

        The asymmetry worth remembering: at 18, ``ADD CONSTRAINT ... NOT
        ENFORCED`` already works for a CHECK (:meth:`supports_constraint_enforced`
        has no such split) while ``ALTER CONSTRAINT ... NOT ENFORCED`` on the
        same CHECK is refused with 42809.  "PostgreSQL 18 can mark a CHECK
        NOT ENFORCED" is true on the ADD path and false on the ALTER path.

        This switch therefore answers the *verb* -- the ``ALTER CONSTRAINT ...
        [NOT] ENFORCED`` grammar exists from 18 on -- and says nothing about
        the kind.  Which kinds can change enforceability is this backend's
        narrowing, and it is measured per kind: see
        :meth:`_require_enforceable_constraint`, called from
        :meth:`format_alter_constraint_action`.
        """
        return self.version >= (18, 0, 0)

    def supports_enable_disable_constraint(self) -> bool:
        """Whether ``ALTER TABLE ... ENABLE|DISABLE CONSTRAINT`` works.

        PostgreSQL 19beta4: **no -- this is not PostgreSQL syntax at all**,
        despite PostgreSQL having the near neighbours ``ENABLE TRIGGER`` /
        ``ENABLE ALWAYS TRIGGER``.  Both forms are rejected by the parser
        before any object is looked at::

            ALTER TABLE t ENABLE  CONSTRAINT ck
            -> 42601: syntax error at or near "CONSTRAINT"
            ALTER TABLE t DISABLE CONSTRAINT ck
            -> 42601: syntax error at or near "CONSTRAINT"

        PostgreSQL expresses the same intent with ``DEFERRABLE`` /
        ``SET CONSTRAINTS`` / ``NOT ENFORCED`` instead.  (This switch is
        not part of the core mixin yet; its name may change when the core
        adopts it.)
        """
        return False

    def supports_validate_constraint(self) -> bool:
        """Whether ``VALIDATE CONSTRAINT`` is accepted.

        PostgreSQL 19beta4: yes, on a ``NOT VALID`` constraint, and on a
        plain enforced one::

            ALTER TABLE t ADD CONSTRAINT cknv CHECK (a > 1) NOT VALID
            ALTER TABLE t VALIDATE CONSTRAINT cknv   -- accepted

        Also accepted on all three instances measured (170011 / 180006 /
        190000), so this switch carries no version gate.

        The one exception: a ``NOT ENFORCED`` constraint cannot be
        validated at all, and the server says so rather than silently
        validating nothing -- same 55000 on both kinds::

            ALTER TABLE t VALIDATE CONSTRAINT ck     -- ck is NOT ENFORCED
            -> 55000: cannot validate NOT ENFORCED constraint
            ALTER TABLE t VALIDATE CONSTRAINT fk     -- fk is NOT ENFORCED
            -> 55000: cannot validate NOT ENFORCED constraint

        ``supports_*`` cannot express "except on a constraint that is itself
        NOT ENFORCED", so that interaction lives in the formatter rather than
        in a switch; it is recorded here because it is a real refusal that a
        caller can reach with two supported switches set to True.
        """
        return True

    #: Branch class -> the SQL spelling used in a refusal message. The
    #: spelling is the SQL keyword, not the Python class name, because the
    #: message is read by someone who wrote SQL.
    _KIND_SQL = {
        ColumnPrimaryKeyConstraint: "PRIMARY KEY",
        ColumnUniqueConstraint: "UNIQUE",
        ColumnCheckConstraint: "CHECK",
        ColumnForeignKeyConstraint: "FOREIGN KEY",
        TablePrimaryKeyConstraint: "PRIMARY KEY",
        TableUniqueConstraint: "UNIQUE",
        TableCheckConstraint: "CHECK",
        TableForeignKeyConstraint: "FOREIGN KEY",
    }

    def _require_constraint_enforced(self, branch: type) -> None:
        """Refuse ``[NOT] ENFORCED`` on a kind PostgreSQL will not accept.

        The accept surface of ``ADD CONSTRAINT``/``CREATE TABLE`` is CHECK and
        FOREIGN KEY only, measured on 18 and 19beta4; PRIMARY KEY and UNIQUE
        come back with ``0A000`` and EXCLUDE with its own ``0A000``.  The
        grammar gate is :meth:`supports_constraint_enforced`; the per-kind
        half is this helper, which is why the probe takes no kind.

        Raises:
            TypeError: ``branch`` is not a constraint branch class. The kind
                used to be a tag an enum carried; it is a class now, and a
                spelling has no place here.
            UnsupportedFeatureError: The version is below 18, or the kind is
                outside the measured accept surface.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        from rhosocial.activerecord.backend.expression.statements import (
            ColumnConstraintBranch,
            TableConstraintBranch,
        )

        if not (isinstance(branch, type)
                and issubclass(branch, (ColumnConstraintBranch, TableConstraintBranch))):
            raise TypeError(
                f"constraint kind must be a constraint branch class, got {branch!r}"
            )
        if (
            branch in (TableCheckConstraint, TableForeignKeyConstraint)
            and self.supports_constraint_enforced()
        ):
            return
        kind = self._KIND_SQL.get(branch, branch.__name__)
        feature = f"{kind} NOT ENFORCED"
        raise UnsupportedFeatureError(
            self.name,
            feature,
            f"{self.name} does not support {feature}.",
        )

    def _require_enforceable_constraint(self, branch: type) -> None:
        """Refuse ``ALTER CONSTRAINT ... [NOT] ENFORCED`` on a kind it will not take.

        The verb exists from PG18; the two kinds split across two releases,
        which is what this helper carries: FOREIGN KEY from 18, CHECK from 19.
        Measured on 180006 and 190000; at 18 a CHECK comes back as ``42809:
        cannot alter enforceability of constraint ... Only foreign key and
        check constraints can change enforceability.``

        Raises:
            TypeError: ``branch`` is not a constraint branch class.
            UnsupportedFeatureError: The kind is outside the measured surface,
                or the server is older than the kind's boundary.
        """
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        from rhosocial.activerecord.backend.expression.statements import (
            ColumnConstraintBranch,
            TableConstraintBranch,
        )

        if not (isinstance(branch, type)
                and issubclass(branch, (ColumnConstraintBranch, TableConstraintBranch))):
            raise TypeError(
                f"constraint kind must be a constraint branch class, got {branch!r}"
            )
        boundary = {
            TableForeignKeyConstraint: (18, 0, 0),
            TableCheckConstraint: (19, 0, 0),
        }.get(branch)
        if boundary is not None and self.version >= boundary:
            return
        kind = self._KIND_SQL.get(branch, branch.__name__)
        feature = f"ALTER CONSTRAINT {kind} NOT ENFORCED"
        raise UnsupportedFeatureError(
            self.name,
            feature,
            f"{self.name} does not support {feature}.",
        )

    def supports_constraint_novalidate(self) -> bool:
        """NOT VALID constraint option (PostgreSQL-proprietary).

        Allows adding a constraint without validating existing data.
        Measured at TABLE level (``ALTER TABLE ADD CONSTRAINT`` and, via
        the core's CREATE TABLE guard, nowhere else) on PostgreSQL 19beta4::

            ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0) NOT VALID     -- accepted
            ALTER TABLE t ADD CONSTRAINT f FOREIGN KEY (a) ... NOT VALID -- accepted
            ALTER TABLE t ADD CONSTRAINT p PRIMARY KEY (a) NOT VALID
            -> 0A000: PRIMARY KEY constraints cannot be marked NOT VALID
            ALTER TABLE t ADD CONSTRAINT u UNIQUE (a) NOT VALID
            -> 0A000: UNIQUE constraints cannot be marked NOT VALID

        At COLUMN level NOT VALID does not exist for *any* kind, not even
        CHECK -- the parser never sees the clause as part of a column
        constraint::

            CREATE TABLE ct (a int CHECK (a > 0) NOT VALID)
            -> 42601: syntax error at or near "VALID"

        So there is nothing to switch on for column level: the combination
        is inexpressible on PostgreSQL, and the core's CREATE TABLE guard
        ("NOT VALID is only valid when adding a constraint") keeps that
        unreachable rather than relying on the server.
        """
        return True

    def supports_exclude_constraint(self) -> bool:
        """EXCLUDE constraints (PostgreSQL-proprietary).

        Exclusion constraints prevent overlapping rows on specified
        columns using specified operators.
        Supported by all PostgreSQL versions.

        Note the interaction with NOT ENFORCED: EXCLUSion constraints are
        PK/UNIQUE-shaped, so they refuse it with
        ``0A000: EXCLUDE constraints cannot be marked NOT ENFORCED``,
        while ``DEFERRABLE`` is accepted (see
        :meth:`supports_deferrable_exclude_constraint`).
        """
        return True

    # ALTER TABLE constraint lifecycle (SQL:92).  Core consults
    # supports_add_constraint and supports_drop_constraint before it renders
    # (DDLColumnMixin raises UnsupportedFeatureError when they are False), so
    # these two are load-bearing rather than merely informational; RENAME is
    # declared next to them because it is the third lifecycle verb of the same
    # feature group and is measured on the same server.

    def supports_add_constraint(self) -> bool:
        """True: ``ALTER TABLE ... ADD CONSTRAINT`` covers all four kinds.

        Measured on PostgreSQL 19beta4, all four accepted at ADD time::

            ALTER TABLE t ADD CONSTRAINT pk PRIMARY KEY (a)
            ALTER TABLE t ADD CONSTRAINT u  UNIQUE (a)
            ALTER TABLE t ADD CONSTRAINT ck CHECK (a > 0)
            ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES r(a)

        EXCLUDE is accepted too (see :meth:`supports_exclude_constraint`), so
        the accept surface is all five constraint kinds.  Which *clauses* may
        trail the constraint is a separate question with a much narrower
        answer -- see the per-kind deferral and enforcement switches above.

        One table may hold only one primary key, as in every other backend;
        that is a property of primary keys, not of this statement, so it does
        not belong on this switch.

        Core already defaults this to ``True``; the declaration pins it to the
        measurement.
        """
        return True

    def supports_drop_constraint(self) -> bool:
        """True: ``ALTER TABLE ... DROP CONSTRAINT name`` is accepted.

        Measured on PostgreSQL 19beta4::

            ALTER TABLE t DROP CONSTRAINT ck        -- accepted
            ALTER TABLE t DROP CONSTRAINT dpk       -- a PRIMARY KEY, accepted

        A missing constraint is an **error**, not a silent no-op, and the
        refusal is distinguishable by code::

            ALTER TABLE t DROP CONSTRAINT does_not_exist
            -> 42704: constraint "does_not_exist" of relation "t" does not exist
            ALTER TABLE t DROP CONSTRAINT IF EXISTS also_missing   -- accepted

        So the idempotent path is ``IF EXISTS``, declared separately by
        :meth:`supports_drop_constraint_if_exists`, and a caller cannot get
        idempotence by catching 42704 the way some backends allow.

        Unlike MySQL, PostgreSQL's ``DROP CONSTRAINT`` namespace **does**
        contain the primary key -- ``dpk`` above was a real primary key
        constraint and it dropped.  (MySQL needs a separate
        ``DROP PRIMARY KEY`` for that, which is the trap this note exists to
        head off.)

        Core already defaults this to ``True``; the declaration pins it to the
        measurement.
        """
        return True

    def supports_rename_constraint(self) -> bool:
        """True: ``ALTER TABLE ... RENAME CONSTRAINT ... TO ...`` is accepted.

        Measured on PostgreSQL 19beta4, for a CHECK and a FOREIGN KEY::

            ALTER TABLE t RENAME CONSTRAINT ck  TO ck_r    -- accepted
            ALTER TABLE t RENAME CONSTRAINT fk  TO fk_r    -- accepted

        Both renames were confirmed to land: re-selecting ``conname`` from
        ``pg_constraint`` afterwards returns the new names.  Unlike the
        imperative-alteration statements, this one is by-name only -- there is
        no "rename the constraint on these columns" form, so a caller that
        only knows the column list cannot use it.

        A missing constraint is an error here too, and with the same SQLSTATE
        as the drop path::

            ALTER TABLE t RENAME CONSTRAINT nope TO nope2
            -> 42704: constraint "nope" for table "t" does not exist

        Note there is no ``IF EXISTS`` form of ``RENAME CONSTRAINT``, so this
        verb has no idempotent spelling on PostgreSQL.

        Name note: this switch is **not in the core mixin at all** -- it is
        declared here (and, with the opposite value, in the SQL Server
        backend) because the core ``AlterTableConstraintSupport`` protocol
        that owns it has not landed.  It survived core's S3 unchanged because
        S3 moved constraint *nodes*, not this verb; the measured value is what
        matters and is asserted either way.
        """
        return True

    def supports_drop_constraint_if_exists(self) -> bool:
        """``DROP CONSTRAINT IF EXISTS`` (vendor extension).

        PostgreSQL has supported the ``IF EXISTS`` qualifier on
        ``DROP CONSTRAINT`` since 9.6.  Measured on PostgreSQL 19beta4:
        dropping a constraint that does not exist is **silently successful**
        with the qualifier, and raises 42704 without it (see
        :meth:`supports_drop_constraint`)::

            ALTER TABLE t DROP CONSTRAINT IF EXISTS also_missing   -- accepted

        This is what makes an idempotent drop possible on PostgreSQL.
        """
        return True

    def format_column_constraint(self, constraint: "ColumnConstraint") -> Tuple[str, Tuple]:
        """Refuse the enforcement trait outright, then render as core does.

        PostgreSQL has no column-level ``[NOT] ENFORCED`` at all: the clause is
        a parse error for every kind on every version measured::

            CREATE TABLE ct (a int CHECK (a > 0) NOT ENFORCED)
            -> 42601: misplaced NOT ENFORCED clause
            CREATE TABLE ct (a int NOT NULL NOT ENFORCED)
            -> 42601: misplaced NOT ENFORCED clause

        The same holds on 17, where the clause is not in the grammar at all
        (``42601: syntax error at or near "ENFORCED"``).  So there is no kind
        and no version that makes the column-level trait renderable, and the
        answer does not depend on the branch: it is refused here, at the
        production level, before any delegation happens.

        Why the public ``format_column_constraint`` and not the private
        ``_format_constraint_enforcement``: that hook takes the production and
        is shared by the column-level trait renderer and the table-level one,
        so an override there would have to tell the two families apart by
        hand.  Overriding the column-level seam makes the family the override's
        own business and leaves the shared hook alone.
        """
        if constraint.enforced or constraint.not_enforced:
            from rhosocial.activerecord.backend.dialect.exceptions import (
                UnsupportedFeatureError,
            )

            kind = self._KIND_SQL.get(
                type(constraint.branch), type(constraint.branch).__name__
            )
            feature = f"{kind} NOT ENFORCED on a column constraint"
            raise UnsupportedFeatureError(
                self.name,
                feature,
                f"{self.name} does not support {feature}.",
            )
        return cast(Tuple[str, tuple], super().format_column_constraint(constraint))

    def format_table_constraint(self, expr: "TableConstraint") -> Tuple[str, tuple]:
        """Narrow the enforcement trait to CHECK and FOREIGN KEY, then render.

        The trait is expressible on a PRIMARY KEY and a UNIQUE at the
        expression layer, so this is where PostgreSQL's narrower accept
        surface is applied.  Both are refused by the server with
        ``0A000: PRIMARY KEY/UNIQUE constraints cannot be marked NOT
        ENFORCED``; EXCLUDE cannot carry the trait at all and is refused at
        construction by the core whitelist.
        """
        if expr.enforced or expr.not_enforced:
            self._require_constraint_enforced(type(expr.branch))
        return cast(Tuple[str, tuple], super().format_table_constraint(expr))

    def format_alter_constraint_action(self, action: "AlterConstraint") -> Tuple[str, tuple]:
        """Gate the ``ALTER CONSTRAINT`` verb on the measured per-kind boundary.

        Core already refuses a branch that is neither CHECK nor FOREIGN KEY and
        consults :meth:`supports_alter_constraint_enforced` for the verb.  What
        core cannot know is that the two kinds split across two releases --
        FOREIGN KEY from 18, CHECK from 19 -- which is what this override adds
        before delegating.
        """
        self._require_enforceable_constraint(action.constraint_branch)
        return cast(Tuple[str, tuple], super().format_alter_constraint_action(action))

    def format_validate_constraint_action(self, action: "ValidateConstraint") -> Tuple[str, tuple]:
        return cast(Tuple[str, tuple], super().format_validate_constraint_action(action))

    @staticmethod
    def _validate_exclude_expression_sql(expression_sql: str, context: str) -> None:
        if not isinstance(expression_sql, str) or not expression_sql.strip():
            raise ValueError(f"{context} must render a non-empty SQL expression")
        depth = 0
        quote: Any = None
        index = 0
        while index < len(expression_sql):
            character = expression_sql[index]
            if quote is not None:
                if character == quote:
                    if index + 1 < len(expression_sql) and expression_sql[index + 1] == quote:
                        index += 2
                        continue
                    quote = None
                elif character == "\\" and quote == "'":
                    raise ValueError(f"{context} contains an unsupported string escape")
                index += 1
                continue
            if character in {"'", '"'}:
                quote = character
            elif character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
                if depth < 0:
                    raise ValueError(f"{context} has unbalanced parentheses")
            elif character == ";":
                raise ValueError(f"{context} must not contain statement separators")
            elif character == "-" and expression_sql[index : index + 2] == "--":
                raise ValueError(f"{context} must not contain SQL comments")
            elif character == "/" and expression_sql[index : index + 2] == "/*":
                raise ValueError(f"{context} must not contain SQL comments")
            index += 1
        if quote is not None or depth != 0:
            raise ValueError(f"{context} has unbalanced quotes or parentheses")

    def format_table_exclude_constraint(
        self, constraint: TableExcludeConstraint,
    ) -> Tuple[str, tuple]:
        """Render PostgreSQL's EXCLUDE body.

        The body only: ``EXCLUDE USING <method> (<expr> WITH <op>, ...) [WHERE
        (<predicate>)]``.  The ``CONSTRAINT <name>`` prefix and the trailing
        traits belong to the production and are rendered by
        :meth:`format_table_constraint`, which is where the grammar puts them.

        Core's branch carries one operator over a column list; PostgreSQL's
        :class:`PostgresExcludeBranch` carries a pair per element and an
        optional partial predicate.  Both are read here, so a caller who
        builds the core branch gets ``EXCLUDE USING <method> (a WITH <op>)``
        and one who builds the PostgreSQL branch gets the richer spelling.

        The index access method and the operator are validated here rather
        than trusted from the caller, and every free-form fragment (an element
        expression, the predicate) goes through
        :meth:`_validate_exclude_expression_sql`, because ``EXCLUDE`` is the
        one constraint production where PostgreSQL takes expression text.
        """
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl.exclude_constraint import (
            PostgresExcludeBranch,
        )

        valid_using = frozenset({"gist", "btree", "spgist", "hash", "brin"})
        using = constraint.index_method or "gist"
        if not isinstance(using, str):
            raise ValueError("EXCLUDE index access method must be a string")
        using = using.strip().lower()
        if using not in valid_using:
            raise ValueError(
                f"Invalid index access method '{using}': must be one of {sorted(valid_using)}"
            )

        if isinstance(constraint, PostgresExcludeBranch):
            elements = list(constraint.elements)
            where = constraint.where
        else:
            elements = [(column, constraint.operator) for column in constraint.columns]
            where = None
        if not elements:
            raise ValueError("EXCLUDE constraint requires at least one element")

        rendered_elements: List[str] = []
        for element in elements:
            if not isinstance(element, (tuple, list)) or len(element) != 2:
                raise ValueError("each EXCLUDE element must be an expression/operator pair")
            expression, operator = element
            if not isinstance(operator, str):
                raise ValueError("EXCLUDE operator must be a string")
            operator = "".join(operator.strip().lower().split())
            if operator in {"is", "isnot"} or re.fullmatch(
                r"[a-z0-9_+\-*/<>=~!@#%^&|?$]+", operator
            ) is None:
                raise ValueError(f"Invalid exclude operator '{operator}'")
            if isinstance(expression, str):
                if not expression.strip():
                    raise ValueError("EXCLUDE element expression must not be empty")
                if "(" in expression or ")" in expression:
                    raise ValueError(
                        "parenthesized EXCLUDE elements must be supplied as SQL expressions"
                    )
                expression_sql = self.format_identifier(expression)
            else:
                to_sql = getattr(expression, "to_sql", None)
                if not callable(to_sql):
                    raise TypeError("EXCLUDE element expression must be an identifier or SQL expression")
                if expression.dialect is not self:
                    raise ValueError("EXCLUDE element expression must use the PostgreSQL dialect")
                expression_sql, expression_params = to_sql()
                if expression_params:
                    raise ValueError("EXCLUDE expression must not contain bind parameters")
                self._validate_exclude_expression_sql(expression_sql, "EXCLUDE element expression")
                expression_sql = f"({expression_sql})"
            rendered_elements.append(f"{expression_sql} WITH {operator}")

        parts: List[str] = ["EXCLUDE", "USING", using, f"({', '.join(rendered_elements)})"]

        if where is not None:
            where_to_sql = getattr(where, "to_sql", None)
            if not callable(where_to_sql):
                raise TypeError("EXCLUDE where must be a SQL expression")
            if where.dialect is not self:
                raise ValueError("EXCLUDE where expression must use the PostgreSQL dialect")
            where_sql, where_params = where_to_sql()
            if where_params:
                raise ValueError("EXCLUDE where expression must not contain bind parameters")
            self._validate_exclude_expression_sql(where_sql, "EXCLUDE where expression")
            parts.append(f"WHERE ({where_sql})")

        return " ".join(parts), ()
