# src/rhosocial/activerecord/backend/impl/postgres/partition/maintenance.py
"""PostgreSQL partition maintenance: plan in, tree out, DDL in units.

The executor is deliberately owner-agnostic. It knows a plan's parent, keys,
policies and name pattern; it knows nothing about which application owns the
table, which retention decision produced the cutoff, or which application
table a partition belongs to.

PostgreSQL's DDL is transactional, and the executor uses that: each create/drop
*unit* runs inside one transaction. A unit is one branch together with its
leaves, or all leaves of a flat (single-level) plan. A failed unit leaves no
partially-built branch behind; sibling branches are independent, as the plan
asks for heterogeneous branches.

Public surface is mirrored sync/async:

* :class:`PostgresPartitionMaintenance`
* :class:`AsyncPostgresPartitionMaintenance`

Both expose ``ensure``, ``list``, ``drop_before``, ``drop_branch``,
``validate`` and the batch forms ``ensure_many``, ``list_many`` and
``drop_before_many``.
"""

import re
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import (
    DropTableStatement,
    PartitionClause,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresCreatePartitionStatement,
    PostgresDetachPartitionStatement,
    PostgresPartitionTreeQueryStatement,
)
from .bounds import normalize_interval
from .plan import (
    ListPolicy,
    PartitionBranch,
    PartitionSpec,
    PostgresPartitionPlan,
    RangePolicy,
)
from .report import (
    PairedDifference,
    PartitionBatchReport,
    PartitionInfo,
    PartitionOperation,
    PartitionReport,
)

__all__ = [
    "AsyncPostgresPartitionMaintenance",
    "PostgresPartitionMaintenance",
]

_METHOD_WORDS = ("RANGE", "LIST", "HASH")

_RANGE_BOUND_RE = re.compile(
    r"^FOR VALUES FROM \((?P<start>.*)\) TO \((?P<end>.*)\)$", re.IGNORECASE
)
_LIST_BOUND_RE = re.compile(r"^FOR VALUES IN \((?P<values>.*)\)$", re.IGNORECASE)
_HASH_BOUND_RE = re.compile(
    r"^FOR VALUES WITH \(\s*MODULUS\s+(?P<modulus>\d+)\s*,\s*"
    r"REMAINDER\s+(?P<remainder>\d+)\s*\)$",
    re.IGNORECASE,
)
_TEMPORAL_RE = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
    r"(?:[ T](?P<hour>\d{2}):(?P<minute>\d{2})"
    r"(?::(?P<second>\d{2})(?:\.(?P<fraction>\d{1,6}))?)?"
    r"(?P<offset>Z|[+-]\d{2}(?::?\d{2})?)?)?$",
    re.IGNORECASE,
)
_MIDNIGHT = datetime.min.time()
_UNPARSEABLE_WORDS = ("MINVALUE", "MAXVALUE", "NULL")


def _unquote_bound_value(text: str) -> str:
    """Strip a single-quoted PostgreSQL literal and undo doubled quotes."""
    text = text.strip()
    if len(text) >= 2 and text[0] == "'" and text[-1] == "'":
        return text[1:-1].replace("''", "'")
    return text


def _split_bound_values(text: str) -> Tuple[str, ...]:
    """Split a LIST bound's comma-separated values, respecting quotes."""
    tokens: List[str] = []
    start = 0
    quoted = False
    index = 0
    while index < len(text):
        character = text[index]
        if character == "'":
            if quoted and index + 1 < len(text) and text[index + 1] == "'":
                index += 2
                continue
            quoted = not quoted
        elif character == "," and not quoted:
            tokens.append(text[start:index].strip())
            start = index + 1
        index += 1
    tail = text[start:].strip()
    if tail:
        tokens.append(tail)
    return tuple(tokens)


def _parse_list_value(token: str) -> Any:
    """Parse one LIST bound token into a Python scalar when possible."""
    token = token.strip()
    if len(token) >= 2 and token[0] == "'" and token[-1] == "'":
        return token[1:-1].replace("''", "'")
    if token.upper() == "NULL":
        return None
    try:
        return int(token)
    except ValueError:
        pass
    try:
        return float(token)
    except ValueError:
        return token


def _parse_temporal(text: str) -> Optional[Any]:
    """Parse PostgreSQL's date/timestamp bound text into a Python value."""
    text = _unquote_bound_value(text)
    if text.upper() in _UNPARSEABLE_WORDS:
        return None
    match = _TEMPORAL_RE.match(text)
    if match is None:
        return None
    parts = match.groupdict()
    try:
        year = int(parts["year"])
        month = int(parts["month"])
        day = int(parts["day"])
    except (TypeError, ValueError):
        return None
    if parts["hour"] is None:
        try:
            return date(year, month, day)
        except ValueError:
            return None
    microsecond = 0
    if parts["fraction"]:
        microsecond = int((parts["fraction"] + "000000")[:6])
    tzinfo = None
    offset = parts["offset"]
    if offset:
        if offset.upper() == "Z":
            tzinfo = timezone.utc
        else:
            sign = -1 if offset[0] == "-" else 1
            hours = int(offset[1:3])
            minutes = int(offset[4:6]) if len(offset) > 3 else 0
            tzinfo = timezone(sign * timedelta(hours=hours, minutes=minutes))
    try:
        return datetime(
            year, month, day,
            int(parts["hour"]), int(parts["minute"]),
            int(parts["second"] or 0), microsecond, tzinfo,
        )
    except ValueError:
        return None


def _parse_partition_bound(text: Optional[str]) -> Optional[Dict[str, Any]]:
    """Parse ``relpartbound`` text; return ``None`` when it is unparseable.

    Supported PostgreSQL shapes are RANGE ``FROM ... TO ...`` (date and
    timestamp values), LIST ``IN (...)`` and HASH ``MODULUS ... REMAINDER``.
    """
    if not isinstance(text, str):
        return None
    text = text.strip()
    if not text:
        return None
    match = _RANGE_BOUND_RE.match(text)
    if match is not None:
        parsed: Dict[str, Any] = {"kind": "RANGE"}
        start = _parse_temporal(match.group("start"))
        end = _parse_temporal(match.group("end"))
        if start is not None:
            parsed["range_start"] = start
        if end is not None:
            parsed["range_end"] = end
        return parsed
    match = _LIST_BOUND_RE.match(text)
    if match is not None:
        return {
            "kind": "LIST",
            "list_values": tuple(
                _parse_list_value(token)
                for token in _split_bound_values(match.group("values"))
            ),
        }
    match = _HASH_BOUND_RE.match(text)
    if match is not None:
        return {
            "kind": "HASH",
            "modulus": int(match.group("modulus")),
            "remainder": int(match.group("remainder")),
        }
    return None


def _same_bound(actual: Any, expected: Any) -> bool:
    """Compare parsed catalog bounds with plan bounds.

    The server may render a date where the plan holds a midnight datetime, or
    render a timestamp without an offset where the plan holds an aware one, so
    the comparison normalises those representational differences.
    """
    if isinstance(actual, datetime) and isinstance(expected, datetime):
        if actual.tzinfo is not None and expected.tzinfo is not None:
            return actual.astimezone(timezone.utc) == expected.astimezone(timezone.utc)
        if actual.tzinfo is not None or expected.tzinfo is not None:
            return actual.replace(tzinfo=None) == expected.replace(tzinfo=None)
        return actual == expected
    actual_is_datetime = isinstance(actual, datetime)
    expected_is_datetime = isinstance(expected, datetime)
    if actual_is_datetime != expected_is_datetime:
        moment = actual if actual_is_datetime else expected
        day = expected if actual_is_datetime else actual
        return moment.date() == day and moment.time() == _MIDNIGHT
    return actual == expected


def _format_bound_value(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value)


def _normalise_bound_text(text: Optional[str]) -> Optional[str]:
    """Collapse whitespace in a catalog bound; ``None`` when it is empty."""
    if not isinstance(text, str):
        return None
    normalised = " ".join(text.split())
    return normalised or None


def _key_value(value: Any) -> str:
    """Render one bucket component in a stable, comparable form.

    A midnight timestamp and a date are the same boundary in the plan's
    half-open grammar, so they share one key even when the catalog spells them
    differently. The raw text comparison still reports that spelling drift.
    """
    if isinstance(value, datetime):
        if value.time() == _MIDNIGHT:
            return value.date().isoformat()
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        return repr(value)
    return str(value)


def _sorted_values(values: Sequence[Any]) -> Tuple[Any, ...]:
    """Sort LIST values, falling back to type and repr for mixed types."""
    try:
        return tuple(sorted(values))
    except TypeError:
        return tuple(
            sorted(values, key=lambda value: (type(value).__name__, repr(value)))
        )


def _extras_bucket(info: PartitionInfo) -> Optional[str]:
    """Return the bucket key the row's parsed extras describe, if any."""
    method = (info.method or "").upper()
    extras = info.extras or {}
    if method == "RANGE":
        if "range_start" in extras and "range_end" in extras:
            return "RANGE|%s|%s" % (
                _key_value(extras["range_start"]),
                _key_value(extras["range_end"]),
            )
    elif method == "HASH":
        if "modulus" in extras and "remainder" in extras:
            return "HASH|%s|%s" % (
                _key_value(extras["modulus"]),
                _key_value(extras["remainder"]),
            )
    elif method == "LIST":
        if "list_values" in extras:
            values = _sorted_values(extras["list_values"])
            return "LIST|%s" % ",".join(_key_value(value) for value in values)
    return None


def _difference_key(info: PartitionInfo) -> str:
    """Return the normalized bucket a first-level row is compared on.

    Parsed ``extras`` come first because they give the same key for two
    catalogues that spell the same boundary differently. ``bound`` text is the
    fallback for a server whose bound parser did not recognise the shape, and
    the name suffix is the last resort for a row that carries no bound at all.
    """
    extras_key = _extras_bucket(info)
    if extras_key is not None:
        return extras_key
    bound = _normalise_bound_text(info.bound)
    if bound is not None:
        return bound
    parent = info.parent
    if parent is not None and info.name.startswith(parent + "_"):
        return info.name[len(parent) + 1:]
    return info.name


def _boundary_signature(info: PartitionInfo) -> Optional[str]:
    """Return the semantic boundary of a row, if it has one."""
    extras_key = _extras_bucket(info)
    if extras_key is not None:
        return extras_key
    return _normalise_bound_text(info.bound)


def _boundaries_disagree(left: PartitionInfo, right: PartitionInfo) -> bool:
    """Whether two rows sharing a bucket key describe different boundaries.

    Boundary text is compared verbatim (after whitespace normalization) because
    two catalogues that spell the same bucket differently are exactly the drift
    the paired view must surface. When either side carries no text, the parsed
    extras are the best remaining evidence.
    """
    left_text = _normalise_bound_text(left.bound)
    right_text = _normalise_bound_text(right.bound)
    if left_text is not None and right_text is not None:
        return left_text != right_text
    return _boundary_signature(left) != _boundary_signature(right)


def _policy_shape(policy: Any) -> Optional[Tuple[Any, ...]]:
    """Describe a policy's structure, ignoring the boundaries it generates."""
    if policy is None:
        return None
    if isinstance(policy, RangePolicy):
        explicit = policy.bounds is not None
        interval = None if explicit else normalize_interval(policy.interval)
        return ("RANGE", tuple(policy.columns), interval, explicit)
    if isinstance(policy, ListPolicy):
        return ("LIST", tuple(policy.columns))
    return ("HASH", tuple(policy.columns), policy.modulus)


def _branch_shape(branch: PartitionBranch) -> Tuple[Any, ...]:
    """Describe a first-level branch without its name or boundary values."""
    return (type(branch).__name__, _policy_shape(branch.child))


def _plan_shape(plan: PostgresPartitionPlan) -> Tuple[Any, ...]:
    """Describe the structure two paired plans must share to be comparable."""
    return (
        plan.method,
        tuple(plan.columns),
        plan.modulus,
        _policy_shape(plan.policy),
        tuple(_branch_shape(branch) for branch in plan.branches),
    )


class _PartitionMaintenanceBase:
    """Plan/catalog/DDL logic shared by the sync and async executors.

    Subclasses implement only the three I/O seams: ``_fetch_rows``, ``_execute``
    and ``_run_units``. Everything else -- expected specs, missing/orphan
    comparison, continuity checks, DDL rendering and unit grouping -- lives
    here once, so the two executors cannot drift apart.
    """

    def __init__(self, backend: Any) -> None:
        self.backend = backend
        self.dialect = backend.dialect

    # ------------------------------------------------------------------ I/O
    def _fetch_rows(self, sql: str, params: Tuple[Any, ...]):
        raise NotImplementedError

    def _execute(self, sql: str, params: Tuple[Any, ...]) -> None:
        raise NotImplementedError

    def _run_units(self, plan: PostgresPartitionPlan, units, callback) -> None:
        raise NotImplementedError

    # --------------------------------------------------------------- catalog
    def _tree_expression(self, plan: PostgresPartitionPlan):
        return PostgresPartitionTreeQueryStatement(
            self.dialect,
            root_table=plan.parent,
            schema=plan.schema,
            include_stats=True,
        )

    def _infos(self, plan: PostgresPartitionPlan) -> Tuple[PartitionInfo, ...]:
        sql, params = self._tree_expression(plan).to_sql()
        rows = self._fetch_rows(sql, params)
        return self._parse_rows(rows)

    @staticmethod
    def _parse_rows(rows: Sequence[Any]) -> Tuple[PartitionInfo, ...]:
        infos: List[PartitionInfo] = []
        for row in rows:
            raw_key = row.get("partition_key")
            method = None
            if raw_key:
                head = raw_key.split("(")[0].strip().upper()
                if head in _METHOD_WORDS:
                    method = head
            row_count = row.get("row_count")
            total_bytes = row.get("total_bytes")
            bound = row.get("bound")
            parsed = _parse_partition_bound(bound)
            extras: Dict[str, Any] = {}
            if parsed is not None:
                extras = {
                    key: value
                    for key, value in parsed.items()
                    if key != "kind"
                }
            infos.append(
                PartitionInfo(
                    name=row["name"],
                    parent=row.get("parent"),
                    level=int(row.get("level", 0) or 0),
                    schema=row.get("schema_name"),
                    method=method,
                    bound=bound,
                    row_count=None if row_count is None else int(row_count),
                    size_bytes=None if total_bytes is None else int(total_bytes),
                    is_partitioned=bool(row.get("is_partitioned")),
                    extras=extras,
                )
            )
        return tuple(infos)

    # ---------------------------------------------------------------- expected
    def _expected(
        self, plan: PostgresPartitionPlan, through: Optional[Any]
    ) -> Tuple[PartitionSpec, ...]:
        specs = tuple(plan.iter_specs(through=through))
        if not specs:
            raise ValueError("plan describes no partitions")
        return specs

    def _safe_expected(
        self, plan: PostgresPartitionPlan, through: Optional[Any] = None
    ) -> Tuple[PartitionSpec, ...]:
        """Return expected specs, or nothing when the plan has no horizon."""
        try:
            return self._expected(plan, through)
        except ValueError:
            if plan.is_fully_bounded(through=through):
                raise
            return ()

    @staticmethod
    def _actual_names(infos: Sequence[PartitionInfo]) -> Dict[str, PartitionInfo]:
        return {info.name: info for info in infos if info.level > 0}

    def _boundary_issue(
        self, spec: PartitionSpec, info: PartitionInfo
    ) -> Optional[str]:
        """Describe a parsed-bound mismatch or an unparseable bound."""
        parsed = _parse_partition_bound(info.bound)
        if parsed is None:
            return "unparsed bound for %s: %s" % (
                spec.name, info.bound if info.bound else "<missing>"
            )
        kind = parsed.get("kind")
        if kind != spec.method:
            return "boundary mismatch for %s: catalog %s bound, plan %s bound" % (
                spec.name, kind, spec.method,
            )
        if spec.method == "RANGE":
            start = parsed.get("range_start")
            end = parsed.get("range_end")
            if start is None or end is None:
                return "unparsed bound for %s: %s" % (spec.name, info.bound)
            if _same_bound(start, spec.range_start) and _same_bound(
                end, spec.range_end
            ):
                return None
            return (
                "boundary mismatch for %s: catalog %s..%s, plan %s..%s"
                % (
                    spec.name,
                    _format_bound_value(start),
                    _format_bound_value(end),
                    _format_bound_value(spec.range_start),
                    _format_bound_value(spec.range_end),
                )
            )
        if spec.method == "LIST":
            plan_values = tuple(spec.bounds.get("values", ()))
            actual_values = tuple(parsed.get("list_values", ()))
            if set(actual_values) == set(plan_values):
                return None
            return "boundary mismatch for %s: catalog values %s, plan values %s" % (
                spec.name, actual_values, plan_values,
            )
        plan_modulus = spec.bounds.get("modulus")
        plan_remainder = spec.bounds.get("remainder")
        if (
            parsed.get("modulus") == plan_modulus
            and parsed.get("remainder") == plan_remainder
        ):
            return None
        return (
            "boundary mismatch for %s: catalog modulus %s remainder %s, "
            "plan modulus %s remainder %s"
            % (
                spec.name,
                parsed.get("modulus"),
                parsed.get("remainder"),
                plan_modulus,
                plan_remainder,
            )
        )

    def _boundary_mismatch(
        self, spec: PartitionSpec, info: PartitionInfo
    ) -> bool:
        """Whether a parseable catalog bound disagrees with the plan's spec."""
        parsed = _parse_partition_bound(info.bound)
        if parsed is None:
            return False
        if parsed.get("kind") != spec.method:
            return True
        if spec.method == "RANGE":
            start = parsed.get("range_start")
            end = parsed.get("range_end")
            if start is None or end is None:
                return False
            return not (
                _same_bound(start, spec.range_start)
                and _same_bound(end, spec.range_end)
            )
        if spec.method == "LIST":
            plan_values = tuple(spec.bounds.get("values", ()))
            return set(parsed.get("list_values", ())) != set(plan_values)
        return (
            parsed.get("modulus") != spec.bounds.get("modulus")
            or parsed.get("remainder") != spec.bounds.get("remainder")
        )

    def _needs_partition(
        self, spec: PartitionSpec, actual: Dict[str, PartitionInfo]
    ) -> bool:
        info = actual.get(spec.name)
        if info is None:
            return True
        return self._boundary_mismatch(spec, info)

    def _missing(
        self,
        specs: Sequence[PartitionSpec],
        actual: Dict[str, PartitionInfo],
    ) -> Tuple[Tuple[str, str], ...]:
        return tuple(
            (spec.parent, spec.name)
            for spec in specs
            if self._needs_partition(spec, actual)
        )

    def _orphans(
        self,
        plan: PostgresPartitionPlan,
        specs: Sequence[PartitionSpec],
        actual: Dict[str, PartitionInfo],
        through: Optional[Any],
    ) -> Tuple[str, ...]:
        if not plan.is_fully_bounded(through=through):
            return ()
        expected_names = {spec.name for spec in specs}
        return tuple(
            sorted(name for name in actual if name not in expected_names)
        )

    # -------------------------------------------------------------- rendering
    def _create_statement(
        self,
        plan: PostgresPartitionPlan,
        spec: PartitionSpec,
        if_not_exists: bool,
    ) -> Tuple[str, Tuple[Any, ...]]:
        clause = None
        if spec.child_method is not None:
            clause = PartitionClause(
                self.dialect,
                spec.child_method,
                [Column(self.dialect, name) for name in spec.child_columns],
            )
        statement = PostgresCreatePartitionStatement(
            dialect=self.dialect,
            partition_name=spec.name,
            parent_table=spec.parent,
            partition_type=spec.method,
            partition_values=dict(spec.bounds),
            schema=plan.schema,
            parent_schema=plan.schema,
            if_not_exists=if_not_exists,
            partition_clause=clause,
        )
        return statement.to_sql()

    def _drop_statement(
        self,
        plan: PostgresPartitionPlan,
        name: str,
        cascade: bool,
    ) -> Tuple[str, Tuple[Any, ...]]:
        statement = DropTableStatement(
            dialect=self.dialect,
            table=Table(self.dialect, name, schema_name=plan.schema),
            if_exists=True,
            cascade=cascade,
        )
        return statement.to_sql()

    def _detach_statement(
        self, plan: PostgresPartitionPlan, spec: PartitionSpec
    ) -> Tuple[str, Tuple[Any, ...]]:
        statement = PostgresDetachPartitionStatement(
            dialect=self.dialect,
            partition_name=spec.name,
            parent_table=spec.parent,
            schema=plan.schema,
            parent_schema=plan.schema,
            concurrently=True,
        )
        return statement.to_sql()

    def _drop_statements(
        self,
        plan: PostgresPartitionPlan,
        spec: PartitionSpec,
        concurrent_detach: bool,
    ) -> Tuple[Tuple[str, str, Tuple[Any, ...]], ...]:
        """Return the ``(action, sql, params)`` sequence for one dropped leaf.

        ``DETACH PARTITION CONCURRENTLY`` is only rendered when the caller asks
        for it *and* the server supports it; every other case keeps the single
        transactional ``DROP TABLE`` path.
        """
        concurrent = (
            concurrent_detach and self.dialect.supports_concurrent_detach()
        )
        drop_sql, drop_params = self._drop_statement(plan, spec.name, cascade=False)
        if not concurrent:
            return (("drop", drop_sql, drop_params),)
        detach_sql, detach_params = self._detach_statement(plan, spec)
        return (
            ("detach", detach_sql, detach_params),
            ("drop", drop_sql, drop_params),
        )

    @staticmethod
    def _units(
        plan: PostgresPartitionPlan,
        specs: Sequence[PartitionSpec],
    ) -> Tuple[Tuple[PartitionSpec, ...], ...]:
        """Group specs into transactional units.

        A flat plan (one policy directly below the parent) is a single unit: its
        leaves are created or dropped together. A multi-level plan is one unit
        per first-level branch, in declaration order, with the branch ahead of
        its own leaves.
        """
        if plan.policy is not None:
            return (tuple(specs),) if specs else ()
        grouped: Dict[str, List[PartitionSpec]] = {}
        order: List[str] = []
        for spec in specs:
            key = spec.name if spec.level == 1 else spec.parent
            if key not in grouped:
                grouped[key] = []
                order.append(key)
            grouped[key].append(spec)
        return tuple(tuple(grouped[key]) for key in order)

    # -------------------------------------------------------------- validation
    def _continuity_issues(
        self, specs: Sequence[PartitionSpec]
    ) -> Tuple[str, ...]:
        issues: List[str] = []
        by_parent: Dict[str, List[PartitionSpec]] = {}
        for spec in specs:
            if spec.range_start is None or spec.range_end is None:
                continue
            by_parent.setdefault(spec.parent, []).append(spec)
        for parent, group in sorted(by_parent.items()):
            ordered = sorted(group, key=lambda item: item.range_start)
            for previous, current in zip(ordered, ordered[1:]):
                if current.range_start > previous.range_end:
                    issues.append(
                        "gap in %s between %s and %s"
                        % (parent, previous.name, current.name)
                    )
                elif current.range_start < previous.range_end:
                    issues.append(
                        "overlap in %s between %s and %s"
                        % (parent, previous.name, current.name)
                    )
        return tuple(issues)

    def _structure_issues(
        self,
        plan: PostgresPartitionPlan,
        infos: Sequence[PartitionInfo],
        specs: Sequence[PartitionSpec],
    ) -> Tuple[str, ...]:
        issues: List[str] = []
        actual = self._actual_names(infos)
        roots = [info for info in infos if info.level == 0]
        if not roots:
            issues.append("partitioned parent %s does not exist" % plan.parent)
            return tuple(issues)
        root = roots[0]
        if root.method != plan.method:
            issues.append(
                "parent %s is partitioned by %s, plan says %s"
                % (plan.parent, root.method, plan.method)
            )
        for spec in specs:
            info = actual.get(spec.name)
            if info is None:
                continue
            if info.parent != spec.parent:
                issues.append(
                    "%s belongs to %s, plan expects %s"
                    % (spec.name, info.parent, spec.parent)
                )
            if spec.child_method is not None:
                if not info.is_partitioned:
                    issues.append(
                        "branch %s is not further partitioned; %s leaves are missing"
                        % (spec.name, spec.child_method)
                    )
                elif info.method != spec.child_method:
                    issues.append(
                        "branch %s is partitioned by %s, plan says %s"
                        % (spec.name, info.method, spec.child_method)
                    )
            elif info.is_partitioned:
                issues.append(
                    "leaf %s is partitioned; plan expects it to be a leaf" % spec.name
                )
            bound_issue = self._boundary_issue(spec, info)
            if bound_issue is not None:
                issues.append(bound_issue)
        return tuple(issues)

    def _report(
        self,
        plan: PostgresPartitionPlan,
        infos: Sequence[PartitionInfo],
        specs: Sequence[PartitionSpec],
        through: Optional[Any],
        operations: Sequence[PartitionOperation] = (),
        dry_run: bool = False,
    ) -> PartitionReport:
        actual = self._actual_names(infos)
        issues = self._structure_issues(plan, infos, specs) + self._continuity_issues(specs)
        return PartitionReport(
            target=plan.parent,
            partitions=tuple(infos),
            missing=self._missing(specs, actual),
            orphans=self._orphans(plan, specs, actual, through),
            issues=issues,
            operations=tuple(operations),
            dry_run=dry_run,
        )

    def _render_creates(
        self,
        plan: PostgresPartitionPlan,
        specs: Sequence[PartitionSpec],
        if_not_exists: bool,
    ) -> Tuple[PartitionOperation, ...]:
        return tuple(
            PartitionOperation(
                action="create",
                name=spec.name,
                parent=spec.parent,
                sql=self._create_statement(plan, spec, if_not_exists)[0],
                level=spec.level,
            )
            for spec in specs
        )

    @staticmethod
    def _without(
        specs: Sequence[PartitionSpec], names: Sequence[str]
    ) -> Tuple[PartitionSpec, ...]:
        removed = set(names)
        return tuple(spec for spec in specs if spec.name not in removed)

    def _drop_candidates(
        self,
        plan: PostgresPartitionPlan,
        cutoff: Any,
        infos: Sequence[PartitionInfo],
        through: Optional[Any],
    ) -> Tuple[PartitionSpec, ...]:
        if not isinstance(cutoff, (date, datetime)):
            raise TypeError("cutoff must be a date or datetime")
        actual = self._actual_names(infos)
        specs = self._expected(plan, through=through if through is not None else cutoff)
        candidates: List[PartitionSpec] = []
        for spec in specs:
            if not spec.is_leaf or spec.range_end is None:
                continue
            try:
                early = spec.range_end <= cutoff
            except TypeError as exc:
                raise TypeError(
                    "cutoff must match the plan's bound type (date vs datetime)"
                ) from exc
            info = actual.get(spec.name)
            if early and info is not None and not info.is_partitioned:
                candidates.append(spec)
        return tuple(candidates)

    # ------------------------------------------------------------------ batch
    @staticmethod
    def _resolve_batch(plans, pairs):
        """Normalize both batch spellings and de-duplicate their targets."""
        pair_entries: List[
            Tuple[str, PostgresPartitionPlan, PostgresPartitionPlan]
        ] = []
        if pairs is not None:
            for entry in pairs:
                try:
                    name, left, right = entry
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "a pair must be (name, left_plan, right_plan)"
                    ) from exc
                if not isinstance(name, str) or not name.strip():
                    raise ValueError("pair name must be a non-empty string")
                pair_entries.append((name, left, right))
        batch: List[PostgresPartitionPlan] = []
        if plans is not None:
            if isinstance(plans, PostgresPartitionPlan):
                batch.append(plans)
            else:
                batch.extend(plans)
        for _, left, right in pair_entries:
            batch.extend((left, right))
        if not batch:
            raise ValueError("batch requires at least one plan")
        unique: List[PostgresPartitionPlan] = []
        seen: Dict[Tuple[Optional[str], str], bool] = {}
        for plan in batch:
            if not isinstance(plan, PostgresPartitionPlan):
                raise TypeError("batch plans must be PostgresPartitionPlan values")
            key = (plan.schema, plan.parent)
            if key in seen:
                continue
            seen[key] = True
            unique.append(plan)
        return tuple(pair_entries), tuple(unique)

    def _ensure_batch_work(
        self,
        plan: PostgresPartitionPlan,
        infos: Sequence[PartitionInfo],
        through: Optional[Any],
        if_not_exists: bool,
    ):
        """Return specs, rendered operations and create statements for a plan."""
        if not plan.is_fully_bounded(through=through):
            raise ValueError("plan has no finite horizon")
        if not infos:
            raise ValueError("partitioned parent %s does not exist" % plan.parent)
        specs = self._expected(plan, through)
        actual = self._actual_names(infos)
        operations: List[PartitionOperation] = []
        statements: List[Tuple[str, Tuple[Any, ...]]] = []
        for spec in specs:
            if not self._needs_partition(spec, actual):
                continue
            sql, params = self._create_statement(plan, spec, if_not_exists)
            statements.append((sql, params))
            operations.append(
                PartitionOperation("create", spec.name, spec.parent, sql, spec.level)
            )
        return specs, tuple(operations), tuple(statements)

    def _drop_batch_work(
        self,
        plan: PostgresPartitionPlan,
        infos: Sequence[PartitionInfo],
        cutoff: Any,
        through: Optional[Any],
        concurrent_detach: bool,
    ):
        """Return specs, candidates, rendered operations and drop statements."""
        specs = self._expected(plan, through=through if through is not None else cutoff)
        candidates = self._drop_candidates(plan, cutoff, infos, through)
        operations: List[PartitionOperation] = []
        statements: List[Tuple[str, Tuple[Any, ...]]] = []
        for spec in candidates:
            for action, sql, params in self._drop_statements(
                plan, spec, concurrent_detach
            ):
                operations.append(
                    PartitionOperation(action, spec.name, spec.parent, sql, spec.level)
                )
                statements.append((sql, params))
        return specs, candidates, tuple(operations), tuple(statements)

    @staticmethod
    def _bucket_map(
        pair_name: str,
        side: str,
        partitions: Sequence[PartitionInfo],
        issues: List[str],
    ) -> Dict[str, PartitionInfo]:
        """Map level-1 rows to their bucket key for one side of a pair."""
        buckets: Dict[str, PartitionInfo] = {}
        for info in partitions:
            if info.level != 1:
                continue
            key = _difference_key(info)
            if key in buckets:
                issues.append(
                    "pair %s has duplicate bucket %s in %s"
                    % (pair_name, key, side)
                )
                continue
            buckets[key] = info
        return buckets

    def _paired_report(
        self,
        pair_entries,
        reports: Dict[str, PartitionReport],
    ) -> Tuple[Tuple[PairedDifference, ...], Tuple[str, ...]]:
        """Compare the declarations a caller made and describe only real drift."""
        differences: List[PairedDifference] = []
        issues: List[str] = []
        for name, left, right in pair_entries:
            if _plan_shape(left) != _plan_shape(right):
                issues.append("pair %s plans are not comparable" % name)
                continue
            left_report = reports.get(left.parent)
            right_report = reports.get(right.parent)
            if left_report is None or right_report is None:
                issues.append("pair %s has no report for one side" % name)
                continue
            left_buckets = self._bucket_map(name, "left", left_report.partitions, issues)
            right_buckets = self._bucket_map(
                name, "right", right_report.partitions, issues
            )
            for key in sorted(set(left_buckets) | set(right_buckets)):
                left_info = left_buckets.get(key)
                right_info = right_buckets.get(key)
                if left_info is None:
                    assert right_info is not None
                    differences.append(
                        PairedDifference(name, key, "right", None, right_info.name)
                    )
                elif right_info is None:
                    differences.append(
                        PairedDifference(name, key, "left", left_info.name, None)
                    )
                elif _boundaries_disagree(left_info, right_info):
                    issues.append("boundary mismatch for %s/%s" % (name, key))
        return tuple(differences), tuple(issues)

    @staticmethod
    def _batch_report(
        pair_entries,
        reports: Dict[str, PartitionReport],
        differences: Tuple[PairedDifference, ...],
        issues: Tuple[str, ...],
    ) -> PartitionBatchReport:
        """Assemble the per-target reports and the paired view."""
        return PartitionBatchReport(
            reports=reports,
            pairs=tuple(name for name, _, _ in pair_entries),
            differences=differences,
            issues=issues,
        )


class PostgresPartitionMaintenance(_PartitionMaintenanceBase):
    """Synchronous PostgreSQL partition maintenance executor.

    Example:
        >>> maintenance = PostgresPartitionMaintenance(backend)
        >>> report = maintenance.ensure(plan, through=date(2027, 1, 1))
        >>> report.missing
        ()
    """

    def _fetch_rows(self, sql: str, params: Tuple[Any, ...]):
        return self.backend.fetch_all(sql, params)

    def _execute(self, sql: str, params: Tuple[Any, ...]) -> None:
        self.backend.execute(sql, params)

    def _run_units(self, plan, units, callback) -> None:
        for unit in units:
            with self.backend.transaction():
                for spec in unit:
                    callback(spec)

    def list(
        self,
        plan: PostgresPartitionPlan,
        through: Optional[Any] = None,
    ) -> PartitionReport:
        """Read the live tree and compare it with the plan.

        Args:
            plan: Tree description.
            through: Exclusive horizon used to expand generated RANGE policies.

        Returns:
            A report with the catalog tree, missing leaves, orphans and
            continuity issues. No DDL is executed.
        """
        infos = self._infos(plan)
        specs = self._safe_expected(plan, through)
        report = self._report(plan, infos, specs, through)
        if not plan.is_fully_bounded(through=through):
            report = replace(
                report, issues=report.issues + ("plan has no finite horizon",)
            )
        return report

    def list_many(
        self,
        plans: Optional[Any] = None,
        *,
        pairs: Optional[Any] = None,
    ) -> PartitionBatchReport:
        """Read several plans and report paired drift without changing anything.

        Args:
            plans: Independent plans to read.
            pairs: ``(pair_name, left_plan, right_plan)`` triples to compare.

        Returns:
            A batch report keyed by target, plus the paired drift.
        """
        pair_entries, batch_plans = self._resolve_batch(plans, pairs)
        reports: Dict[str, PartitionReport] = {}
        for plan in batch_plans:
            reports[plan.parent] = self.list(plan)
        differences, issues = self._paired_report(pair_entries, reports)
        return self._batch_report(pair_entries, reports, differences, issues)

    def ensure(
        self,
        plan: PostgresPartitionPlan,
        through: Optional[Any] = None,
        if_not_exists: bool = True,
    ) -> PartitionReport:
        """Create every partition the plan expects but the catalog lacks.

        Existing partitions are left untouched unless their catalog boundary
        disagrees with the plan; a mismatched partition is not treated as
        present. Each transactional unit (one branch with its leaves, or all
        leaves of a flat plan) is committed independently.

        Args:
            plan: Tree description.
            through: Exclusive horizon used to expand generated RANGE policies.
            if_not_exists: Add ``IF NOT EXISTS`` to every created statement.

        Returns:
            The post-operation report, with the executed DDL in ``operations``.
        """
        if not plan.is_fully_bounded(through=through):
            raise ValueError("plan has no finite horizon")
        infos = self._infos(plan)
        if not infos:
            raise ValueError("partitioned parent %s does not exist" % plan.parent)
        specs = self._expected(plan, through)
        actual = self._actual_names(infos)
        missing = tuple(
            spec for spec in specs if self._needs_partition(spec, actual)
        )
        operations: List[PartitionOperation] = []

        def create(spec: PartitionSpec) -> None:
            sql, params = self._create_statement(plan, spec, if_not_exists)
            self._execute(sql, params)
            operations.append(
                PartitionOperation("create", spec.name, spec.parent, sql, spec.level)
            )

        self._run_units(plan, self._units(plan, missing), create)
        report = self.list(plan, through)
        return replace(report, operations=tuple(operations), dry_run=False)

    def ensure_many(
        self,
        plans: Optional[Any] = None,
        *,
        pairs: Optional[Any] = None,
        through: Optional[Any] = None,
        if_not_exists: bool = True,
    ) -> PartitionBatchReport:
        """Create every missing partition for several plans in one transaction.

        The whole batch is one transactional unit: a failure in any plan aborts
        the batch instead of leaving earlier targets created. After a successful
        run every target is re-read, so each report reflects the post-state.

        Args:
            plans: Independent plans to reconcile.
            pairs: ``(pair_name, left_plan, right_plan)`` triples. Both sides
                are reconciled and later compared for bucket drift.
            through: Exclusive horizon applied to every plan.
            if_not_exists: Add ``IF NOT EXISTS`` to every created statement.

        Returns:
            A batch report keyed by target, plus the paired drift.

        Raises:
            ValueError: when no plan is supplied, a plan has no finite horizon,
                or a plan's parent table does not exist.
        """
        pair_entries, batch_plans = self._resolve_batch(plans, pairs)
        prepared = []
        for plan in batch_plans:
            infos = self._infos(plan)
            prepared.append(
                (plan,) + self._ensure_batch_work(
                    plan, infos, through, if_not_exists
                )
            )
        with self.backend.transaction():
            for _, _, _, statements in prepared:
                for sql, params in statements:
                    self._execute(sql, params)
        reports: Dict[str, PartitionReport] = {}
        for plan, specs, operations, _ in prepared:
            infos = self._infos(plan)
            reports[plan.parent] = self._report(
                plan, infos, specs, through, operations, dry_run=False
            )
        differences, issues = self._paired_report(pair_entries, reports)
        return self._batch_report(pair_entries, reports, differences, issues)

    def drop_before(
        self,
        plan: PostgresPartitionPlan,
        cutoff: Any,
        dry_run: bool = True,
        through: Optional[Any] = None,
        concurrent_detach: bool = False,
    ) -> PartitionReport:
        """Drop every leaf whose whole range ends at or before *cutoff*.

        Only true leaves are considered: a branch that is itself partitioned is
        never dropped here, so ``drop_before`` can never cascade into a whole
        subtree. The cutoff is compared per branch by each branch's own
        policy, which is what lets a monthly branch and a yearly branch answer
        the same cutoff differently.

        Args:
            plan: Tree description.
            cutoff: Exclusive retention boundary (date or datetime).
            dry_run: When true (the default), render the drops without
                executing them.
            through: Optional wider horizon for expected-spec expansion.
            concurrent_detach: On PostgreSQL 14+, detach each leaf with
                ``DETACH PARTITION CONCURRENTLY`` before dropping its table.
                Lower versions keep the plain ``DROP TABLE`` path.

        Returns:
            A report whose ``operations`` lists the (possibly not executed)
            drops.
        """
        infos = self._infos(plan)
        specs = self._expected(plan, through=through if through is not None else cutoff)
        candidates = self._drop_candidates(plan, cutoff, infos, through)
        operations = tuple(
            PartitionOperation(
                action=action,
                name=spec.name,
                parent=spec.parent,
                sql=sql,
                level=spec.level,
            )
            for spec in candidates
            for action, sql, _ in self._drop_statements(
                plan, spec, concurrent_detach
            )
        )
        if not dry_run:
            concurrent = (
                concurrent_detach and self.dialect.supports_concurrent_detach()
            )
            if concurrent:
                # DETACH ... CONCURRENTLY cannot run inside a transaction block,
                # so the concurrent path executes statement by statement.
                for unit in self._units(plan, candidates):
                    for spec in unit:
                        for _, sql, params in self._drop_statements(plan, spec, True):
                            self._execute(sql, params)
            else:
                def drop(spec: PartitionSpec) -> None:
                    for _, sql, params in self._drop_statements(plan, spec, False):
                        self._execute(sql, params)

                self._run_units(plan, self._units(plan, candidates), drop)
            infos = self._infos(plan)
            specs = self._without(specs, [spec.name for spec in candidates])
        return self._report(plan, infos, specs, through, operations, dry_run=dry_run)

    def drop_before_many(
        self,
        plans: Optional[Any] = None,
        *,
        pairs: Optional[Any] = None,
        cutoff: Any,
        dry_run: bool = True,
        through: Optional[Any] = None,
        concurrent_detach: bool = False,
    ) -> PartitionBatchReport:
        """Drop every leaf ending at or before *cutoff* across several plans.

        The drops run on one transaction. ``DETACH PARTITION CONCURRENTLY``
        cannot run inside a transaction, so the concurrent path is used only
        when the batch holds a single plan; a multi-plan batch records that
        limitation as an issue and keeps the transactional plain-drop path.

        Args:
            plans: Independent plans to retain.
            pairs: ``(pair_name, left_plan, right_plan)`` triples to retain and
                compare.
            cutoff: Exclusive retention boundary (date or datetime).
            dry_run: When true (the default), render drops without executing.
            through: Optional wider horizon for expected-spec expansion.
            concurrent_detach: Request detach-then-drop on PostgreSQL 14+.

        Returns:
            A batch report keyed by target, plus the paired drift. A dry run
            describes the pre-run state; a real run describes the post-state.
        """
        pair_entries, batch_plans = self._resolve_batch(plans, pairs)
        supports_concurrent = self.dialect.supports_concurrent_detach()
        concurrent = bool(concurrent_detach) and supports_concurrent
        batch_issues: List[str] = []
        if concurrent and len(batch_plans) > 1:
            concurrent = False
            batch_issues.append(
                "concurrent detach requires a single plan; batch has %d"
                % len(batch_plans)
            )
        prepared = []
        for plan in batch_plans:
            infos = self._infos(plan)
            prepared.append(
                (plan, infos)
                + self._drop_batch_work(plan, infos, cutoff, through, concurrent)
            )
        if not dry_run:
            if concurrent:
                # DETACH ... CONCURRENTLY cannot run inside a transaction block.
                for _, _, _, _, _, statements in prepared:
                    for sql, params in statements:
                        self._execute(sql, params)
            else:
                with self.backend.transaction():
                    for _, _, _, _, _, statements in prepared:
                        for sql, params in statements:
                            self._execute(sql, params)
        reports: Dict[str, PartitionReport] = {}
        for plan, infos, specs, candidates, operations, _ in prepared:
            if dry_run:
                reports[plan.parent] = self._report(
                    plan, infos, specs, through, operations, dry_run=True
                )
                continue
            fresh = self._infos(plan)
            remaining = self._without(specs, [spec.name for spec in candidates])
            reports[plan.parent] = self._report(
                plan, fresh, remaining, through, operations, dry_run=False
            )
        differences, issues = self._paired_report(pair_entries, reports)
        return self._batch_report(
            pair_entries, reports, differences, tuple(batch_issues) + issues
        )

    def drop_branch(
        self,
        plan: PostgresPartitionPlan,
        branch: PartitionBranch,
        dry_run: bool = False,
    ) -> PartitionReport:
        """Drop a whole first-level branch and its subpartitions (destructive by default).

        Pass ``dry_run=True`` to render the drop without executing it. This is
        the explicit operation reserved for removing a branch; ``drop_before``
        never removes a whole branch implicitly.

        Args:
            plan: Tree description.
            branch: The first-level branch instance declared by the plan.
            dry_run: When true, render the drop without executing it; defaults
                to False, which executes the drop.

        Returns:
            The report after (or before, under dry run) the drop.
        """
        infos = self._infos(plan)
        index = plan.branches.index(branch)
        name = plan.branch_name(index, branch)
        if name not in self._actual_names(infos):
            return self._report(plan, infos, self._safe_expected(plan), None)
        sql, params = self._drop_statement(plan, name, cascade=True)
        operation = PartitionOperation("drop_branch", name, plan.parent, sql, 1)
        if not dry_run:
            with self.backend.transaction():
                self._execute(sql, params)
            infos = self._infos(plan)
        specs = self._safe_expected(plan)
        specs = self._without(specs, [name] + [spec.name for spec in specs if spec.parent == name])
        return self._report(plan, infos, specs, None, (operation,), dry_run=dry_run)

    def validate(
        self,
        plan: PostgresPartitionPlan,
        through: Optional[Any] = None,
    ) -> PartitionReport:
        """Report continuity and structure problems without changing anything.

        Checks gaps and overlaps in the plan's generated RANGE boundaries,
        parent/strategy mismatches against the catalog, missing partitions and
        orphans. It never repairs a gap; ``ensure`` is the explicit repair.

        Args:
            plan: Tree description.
            through: Exclusive horizon used to expand generated RANGE policies.

        Returns:
            A report whose ``issues`` lists every problem found.
        """
        return self.list(plan, through)


class AsyncPostgresPartitionMaintenance(_PartitionMaintenanceBase):
    """Asynchronous PostgreSQL partition maintenance executor.

    The API mirrors :class:`PostgresPartitionMaintenance` exactly; every method
    is a coroutine and the DDL transaction units are PostgreSQL's own.
    """

    async def _fetch_rows(self, sql: str, params: Tuple[Any, ...]):
        return await self.backend.fetch_all(sql, params)

    async def _execute(self, sql: str, params: Tuple[Any, ...]) -> None:
        await self.backend.execute(sql, params)

    async def _infos(self, plan: PostgresPartitionPlan) -> Tuple[PartitionInfo, ...]:
        sql, params = self._tree_expression(plan).to_sql()
        rows = await self._fetch_rows(sql, params)
        return self._parse_rows(rows)

    async def _run_units(self, plan, units, callback) -> None:
        for unit in units:
            async with self.backend.transaction():
                for spec in unit:
                    await callback(spec)

    async def list(
        self,
        plan: PostgresPartitionPlan,
        through: Optional[Any] = None,
    ) -> PartitionReport:
        """Read the live tree and compare it with the plan asynchronously."""
        infos = await self._infos(plan)
        specs = self._safe_expected(plan, through)
        report = self._report(plan, infos, specs, through)
        if not plan.is_fully_bounded(through=through):
            report = replace(
                report, issues=report.issues + ("plan has no finite horizon",)
            )
        return report

    async def list_many(
        self,
        plans: Optional[Any] = None,
        *,
        pairs: Optional[Any] = None,
    ) -> PartitionBatchReport:
        """Read several plans asynchronously and report paired drift."""
        pair_entries, batch_plans = self._resolve_batch(plans, pairs)
        reports: Dict[str, PartitionReport] = {}
        for plan in batch_plans:
            reports[plan.parent] = await self.list(plan)
        differences, issues = self._paired_report(pair_entries, reports)
        return self._batch_report(pair_entries, reports, differences, issues)

    async def ensure(
        self,
        plan: PostgresPartitionPlan,
        through: Optional[Any] = None,
        if_not_exists: bool = True,
    ) -> PartitionReport:
        """Create every missing partition asynchronously."""
        if not plan.is_fully_bounded(through=through):
            raise ValueError("plan has no finite horizon")
        infos = await self._infos(plan)
        if not infos:
            raise ValueError("partitioned parent %s does not exist" % plan.parent)
        specs = self._expected(plan, through)
        actual = self._actual_names(infos)
        missing = tuple(
            spec for spec in specs if self._needs_partition(spec, actual)
        )
        operations: List[PartitionOperation] = []

        async def create(spec: PartitionSpec) -> None:
            sql, params = self._create_statement(plan, spec, if_not_exists)
            await self._execute(sql, params)
            operations.append(
                PartitionOperation("create", spec.name, spec.parent, sql, spec.level)
            )

        await self._run_units(plan, self._units(plan, missing), create)
        report = await self.list(plan, through)
        return replace(report, operations=tuple(operations), dry_run=False)

    async def ensure_many(
        self,
        plans: Optional[Any] = None,
        *,
        pairs: Optional[Any] = None,
        through: Optional[Any] = None,
        if_not_exists: bool = True,
    ) -> PartitionBatchReport:
        """Create every missing partition for several plans asynchronously.

        The whole batch runs in one transaction, matching
        :meth:`PostgresPartitionMaintenance.ensure_many`.
        """
        pair_entries, batch_plans = self._resolve_batch(plans, pairs)
        prepared = []
        for plan in batch_plans:
            infos = await self._infos(plan)
            prepared.append(
                (plan,) + self._ensure_batch_work(
                    plan, infos, through, if_not_exists
                )
            )
        async with self.backend.transaction():
            for _, _, _, statements in prepared:
                for sql, params in statements:
                    await self._execute(sql, params)
        reports: Dict[str, PartitionReport] = {}
        for plan, specs, operations, _ in prepared:
            infos = await self._infos(plan)
            reports[plan.parent] = self._report(
                plan, infos, specs, through, operations, dry_run=False
            )
        differences, issues = self._paired_report(pair_entries, reports)
        return self._batch_report(pair_entries, reports, differences, issues)

    async def drop_before(
        self,
        plan: PostgresPartitionPlan,
        cutoff: Any,
        dry_run: bool = True,
        through: Optional[Any] = None,
        concurrent_detach: bool = False,
    ) -> PartitionReport:
        """Drop leaves ending at or before *cutoff* asynchronously."""
        infos = await self._infos(plan)
        specs = self._expected(plan, through=through if through is not None else cutoff)
        candidates = self._drop_candidates(plan, cutoff, infos, through)
        operations = tuple(
            PartitionOperation(
                action=action,
                name=spec.name,
                parent=spec.parent,
                sql=sql,
                level=spec.level,
            )
            for spec in candidates
            for action, sql, _ in self._drop_statements(
                plan, spec, concurrent_detach
            )
        )
        if not dry_run:
            concurrent = (
                concurrent_detach and self.dialect.supports_concurrent_detach()
            )
            if concurrent:
                # DETACH ... CONCURRENTLY cannot run inside a transaction block.
                for unit in self._units(plan, candidates):
                    for spec in unit:
                        for _, sql, params in self._drop_statements(plan, spec, True):
                            await self._execute(sql, params)
            else:
                async def drop(spec: PartitionSpec) -> None:
                    for _, sql, params in self._drop_statements(plan, spec, False):
                        await self._execute(sql, params)

                await self._run_units(plan, self._units(plan, candidates), drop)
            infos = await self._infos(plan)
            specs = self._without(specs, [spec.name for spec in candidates])
        return self._report(plan, infos, specs, through, operations, dry_run=dry_run)

    async def drop_before_many(
        self,
        plans: Optional[Any] = None,
        *,
        pairs: Optional[Any] = None,
        cutoff: Any,
        dry_run: bool = True,
        through: Optional[Any] = None,
        concurrent_detach: bool = False,
    ) -> PartitionBatchReport:
        """Drop every leaf ending at or before *cutoff* asynchronously.

        This is the asynchronous counterpart of
        :meth:`PostgresPartitionMaintenance.drop_before_many`.
        """
        pair_entries, batch_plans = self._resolve_batch(plans, pairs)
        supports_concurrent = self.dialect.supports_concurrent_detach()
        concurrent = bool(concurrent_detach) and supports_concurrent
        batch_issues: List[str] = []
        if concurrent and len(batch_plans) > 1:
            concurrent = False
            batch_issues.append(
                "concurrent detach requires a single plan; batch has %d"
                % len(batch_plans)
            )
        prepared = []
        for plan in batch_plans:
            infos = await self._infos(plan)
            prepared.append(
                (plan, infos)
                + self._drop_batch_work(plan, infos, cutoff, through, concurrent)
            )
        if not dry_run:
            if concurrent:
                # DETACH ... CONCURRENTLY cannot run inside a transaction block.
                for _, _, _, _, _, statements in prepared:
                    for sql, params in statements:
                        await self._execute(sql, params)
            else:
                async with self.backend.transaction():
                    for _, _, _, _, _, statements in prepared:
                        for sql, params in statements:
                            await self._execute(sql, params)
        reports: Dict[str, PartitionReport] = {}
        for plan, infos, specs, candidates, operations, _ in prepared:
            if dry_run:
                reports[plan.parent] = self._report(
                    plan, infos, specs, through, operations, dry_run=True
                )
                continue
            fresh = await self._infos(plan)
            remaining = self._without(specs, [spec.name for spec in candidates])
            reports[plan.parent] = self._report(
                plan, fresh, remaining, through, operations, dry_run=False
            )
        differences, issues = self._paired_report(pair_entries, reports)
        return self._batch_report(
            pair_entries, reports, differences, tuple(batch_issues) + issues
        )

    async def drop_branch(
        self,
        plan: PostgresPartitionPlan,
        branch: PartitionBranch,
        dry_run: bool = False,
    ) -> PartitionReport:
        """Drop a whole first-level branch asynchronously (destructive by default).

        The dropped branch takes its subpartitions with it. Pass
        ``dry_run=True`` to render the drop without executing it. This is
        the explicit operation reserved for removing a branch; ``drop_before``
        never removes a whole branch implicitly.

        Args:
            plan: Tree description.
            branch: The first-level branch instance declared by the plan.
            dry_run: When true, render the drop without executing it; defaults
                to False, which executes the drop.

        Returns:
            The report after (or before, under dry run) the drop.
        """
        infos = await self._infos(plan)
        index = plan.branches.index(branch)
        name = plan.branch_name(index, branch)
        if name not in self._actual_names(infos):
            return self._report(plan, infos, self._safe_expected(plan), None)
        sql, params = self._drop_statement(plan, name, cascade=True)
        operation = PartitionOperation("drop_branch", name, plan.parent, sql, 1)
        if not dry_run:
            async with self.backend.transaction():
                await self._execute(sql, params)
            infos = await self._infos(plan)
        specs = self._safe_expected(plan)
        specs = self._without(specs, [name] + [spec.name for spec in specs if spec.parent == name])
        return self._report(plan, infos, specs, None, (operation,), dry_run=dry_run)

    async def validate(
        self,
        plan: PostgresPartitionPlan,
        through: Optional[Any] = None,
    ) -> PartitionReport:
        """Report continuity and structure problems asynchronously."""
        return await self.list(plan, through)
