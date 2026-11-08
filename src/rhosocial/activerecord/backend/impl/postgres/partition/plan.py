# src/rhosocial/activerecord/backend/impl/postgres/partition/plan.py
"""Owner-agnostic partition plans for PostgreSQL.

ActiveRecord deliberately does not model partitioning (see the core plan
``2026-11-02/partitioned-model-support.md``): a model declares the primary-key
shape a partitioned table needs, and the backend owns the tree. This module is
the backend's description of a tree:

    Parent(HASH, tenant_bucket, modulus=4)
     |-- p0 (RANGE, created_at, monthly)
     |-- p1 (RANGE, created_at, quarterly)
     |-- p2 (RANGE, created_at, yearly)
     `-- p3 (RANGE, created_at, explicit 15-day bounds)

PostgreSQL allows each branch of a HASH-partitioned parent to choose its own
child strategy, so the plan is a tree of policies, not one flat strategy. A
plan carries table names, columns and policies only; it carries no application
names, no retention default and no application vocabulary. Those belong to the
caller.

Policies describe the partitions *below* their parent. ``RangePolicy``,
``ListPolicy`` and ``HashPolicy`` each generate concrete boundaries and
labels; naming and DDL rendering are the maintenance executor's job.
"""

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterator, Optional, Tuple, Union

from .bounds import default_label, iter_range_bounds, normalize_interval

__all__ = [
    "HashBranch",
    "HashPolicy",
    "ListBranch",
    "ListPolicy",
    "PartitionPolicy",
    "PartitionSpec",
    "PostgresPartitionPlan",
    "RangeBranch",
    "RangePolicy",
]

_METHODS = ("RANGE", "LIST", "HASH")


def _as_columns(value: Any) -> Tuple[str, ...]:
    """Normalise a column declaration to a tuple of non-empty names."""
    if isinstance(value, str):
        columns = (value,)
    elif isinstance(value, (tuple, list)):
        columns = tuple(value)
    else:
        raise TypeError("columns must be a string or a tuple of strings")
    if not columns:
        raise ValueError("columns must not be empty")
    for column in columns:
        if not isinstance(column, str) or not column.strip():
            raise ValueError("every column must be a non-empty string")
    return columns


def _normalise_method(method: Any) -> str:
    if isinstance(method, str):
        normalized = method.upper()
        if normalized in _METHODS:
            return normalized
    raise ValueError("method must be one of %s, got %r" % (", ".join(_METHODS), method))


def _label_from_values(values: Tuple[Any, ...]) -> str:
    text = "_".join(str(value) for value in values)
    label = re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_")
    return label or "value"


def _render_label(pattern: str, start: Any, end: Any) -> str:
    return pattern.format(
        start=start,
        end=end,
        year=start.year,
        month=start.month,
        day=start.day,
        quarter=(start.month - 1) // 3 + 1,
    )


@dataclass(frozen=True)
class RangePolicy:
    """Generate RANGE partitions with half-open ``[start, end)`` bounds.

    Either *bounds* supplies every boundary explicitly, or *start* plus
    *interval* generates them. A generated policy needs a horizon: *count*, or
    a ``through`` argument on the maintenance call. The same policy object can
    serve one flat monthly table or one branch of a HASH tree (each branch
    advancing at its own granularity).

    Attributes:
        columns: Partition key columns below the policy's parent.
        start: First boundary when *bounds* is not given.
        interval: Step between generated boundaries; a named unit, a
            ``"<amount> <unit>"`` spelling, or an ``(amount, unit)`` pair.
        count: Maximum number of generated boundaries.
        bounds: Explicit ``(start, end)`` pairs, mutually exclusive with the
            generated form.
        label_pattern: Optional ``str.format`` pattern for the name suffix;
            receives ``start``, ``end``, ``year``, ``month``, ``day`` and
            ``quarter``.
    """

    columns: Union[str, Tuple[str, ...]] = "created_at"
    start: Optional[Any] = None
    interval: Any = "month"
    count: Optional[int] = None
    bounds: Optional[Tuple[Tuple[Any, Any], ...]] = None
    label_pattern: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", _as_columns(self.columns))
        if self.bounds is not None:
            pairs = tuple(tuple(pair) for pair in self.bounds)
            if self.start is not None:
                raise ValueError("start and bounds are mutually exclusive")
        else:
            pairs = None
            if self.start is None:
                raise ValueError("RangePolicy requires start or bounds")
            if not isinstance(self.start, (date, datetime)):
                raise TypeError("RangePolicy start must be date or datetime")
        normalize_interval(self.interval)
        if self.count is not None:
            if isinstance(self.count, bool) or not isinstance(self.count, int) or self.count <= 0:
                raise ValueError("count must be a positive integer")
        if pairs is not None:
            if not pairs:
                raise ValueError("bounds must not be empty")
            for pair in pairs:
                if len(pair) != 2:
                    raise ValueError("every bounds entry must be (start, end)")
                if not isinstance(pair[0], (date, datetime)) or not isinstance(
                    pair[1], (date, datetime)
                ):
                    raise TypeError("bound values must be date or datetime")
                if pair[0] >= pair[1]:
                    raise ValueError("bound start must be before end")
            object.__setattr__(self, "bounds", pairs)

    @property
    def method(self) -> str:
        """The partitioning strategy this policy generates."""
        return "RANGE"

    def iter_bounds(self, through: Optional[Any] = None) -> Iterator[Tuple[Any, Any]]:
        """Yield the policy's ``(start, end)`` boundaries.

        Explicit bounds are yielded as declared. Generated bounds stop at
        *count* or at *through*, whichever comes first.

        Raises:
            ValueError: when the policy has no horizon, or when *through* is
                not after the first boundary.
        """
        if self.bounds is not None:
            for pair in self.bounds:
                yield pair[0], pair[1]
            return
        yield from iter_range_bounds(
            self.start,
            self.interval,
            count=self.count,
            through=through,
        )

    def label_for(self, start: Any, end: Any) -> str:
        """Return the partition-name suffix for one generated boundary."""
        if self.label_pattern:
            return _render_label(self.label_pattern, start, end)
        if self.bounds is not None:
            return start.strftime("%Y_%m_%d")
        return default_label(start, self.interval)


@dataclass(frozen=True)
class ListPolicy:
    """Generate LIST partitions from explicit value groups.

    Attributes:
        columns: Partition key column (PostgreSQL LIST accepts exactly one).
        groups: One tuple of values per generated partition.
        labels: Optional name suffix per group, same length as *groups*.
    """

    columns: Union[str, Tuple[str, ...]] = "status"
    groups: Tuple[Tuple[Any, ...], ...] = ()
    labels: Optional[Tuple[str, ...]] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", _as_columns(self.columns))
        groups = tuple(tuple(group) for group in self.groups)
        if not groups:
            raise ValueError("ListPolicy requires at least one value group")
        for group in groups:
            if not group:
                raise ValueError("every value group must be non-empty")
        object.__setattr__(self, "groups", groups)
        if self.labels is not None:
            labels = tuple(self.labels)
            if len(labels) != len(groups):
                raise ValueError("labels must have one entry per value group")
            for label in labels:
                if not isinstance(label, str) or not label.strip():
                    raise ValueError("every label must be a non-empty string")
            object.__setattr__(self, "labels", labels)

    @property
    def method(self) -> str:
        """The partitioning strategy this policy generates."""
        return "LIST"

    def iter_groups(self) -> Iterator[Tuple[Tuple[Any, ...], str]]:
        """Yield ``(values, label)`` for every generated partition."""
        for index, values in enumerate(self.groups):
            if self.labels is not None:
                label = self.labels[index]
            else:
                label = _label_from_values(values)
            yield values, label


@dataclass(frozen=True)
class HashPolicy:
    """Generate HASH partitions for a fixed modulus.

    Attributes:
        columns: Partition key columns.
        modulus: Number of hash buckets.
        remainders: Buckets to generate; all buckets when omitted.
    """

    columns: Union[str, Tuple[str, ...]] = "bucket"
    modulus: int = 1
    remainders: Optional[Tuple[int, ...]] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", _as_columns(self.columns))
        if isinstance(self.modulus, bool) or not isinstance(self.modulus, int):
            raise TypeError("modulus must be an integer")
        if self.modulus <= 0:
            raise ValueError("modulus must be a positive integer")
        if self.remainders is not None:
            remainders = tuple(self.remainders)
            if not remainders:
                raise ValueError("remainders must not be empty")
            for remainder in remainders:
                if isinstance(remainder, bool) or not isinstance(remainder, int):
                    raise TypeError("remainder must be an integer")
                if not 0 <= remainder < self.modulus:
                    raise ValueError("remainder must satisfy 0 <= remainder < modulus")
            if len(set(remainders)) != len(remainders):
                raise ValueError("remainders must be unique")
            object.__setattr__(self, "remainders", remainders)

    @property
    def method(self) -> str:
        """The partitioning strategy this policy generates."""
        return "HASH"

    def iter_remainders(self) -> Iterator[int]:
        """Yield the selected remainders in ascending order."""
        if self.remainders is not None:
            yield from self.remainders
            return
        for remainder in range(self.modulus):
            yield remainder


#: A policy is any of the three strategy generators.
PartitionPolicy = Union[RangePolicy, ListPolicy, HashPolicy]


@dataclass(frozen=True)
class HashBranch:
    """A first-level partition of a HASH-partitioned parent.

    Attributes:
        remainder: Hash bucket this branch owns.
        child: Optional policy for partitions below this branch.
        name: Optional branch name suffix, substituted into ``name_pattern``;
            the default is ``p<n>``, giving ``<parent>_p<n>``.
    """

    remainder: int
    child: Optional[PartitionPolicy] = None
    name: Optional[str] = None

    def __post_init__(self) -> None:
        if isinstance(self.remainder, bool) or not isinstance(self.remainder, int):
            raise TypeError("remainder must be an integer")
        if self.remainder < 0:
            raise ValueError("remainder must not be negative")
        if self.name is not None and not self.name.strip():
            raise ValueError("name must be a non-empty string")


@dataclass(frozen=True)
class RangeBranch:
    """A first-level RANGE partition, independent of a policy horizon.

    Attributes:
        start: Inclusive lower bound.
        end: Exclusive upper bound.
        child: Optional policy for partitions below this branch.
        name: Optional branch name suffix, substituted into ``name_pattern``.
        label: Optional name suffix instead of the derived date label.
    """

    start: Any
    end: Any
    child: Optional[PartitionPolicy] = None
    name: Optional[str] = None
    label: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.start, (date, datetime)) or not isinstance(
            self.end, (date, datetime)
        ):
            raise TypeError("RangeBranch bounds must be date or datetime")
        if self.start >= self.end:
            raise ValueError("RangeBranch start must be before end")
        if self.name is not None and not self.name.strip():
            raise ValueError("name must be a non-empty string")
        if self.label is not None and not self.label.strip():
            raise ValueError("label must be a non-empty string")


@dataclass(frozen=True)
class ListBranch:
    """A first-level LIST partition, independent of a policy horizon.

    Attributes:
        values: Values owned by this branch.
        child: Optional policy for partitions below this branch.
        name: Optional branch name suffix, substituted into ``name_pattern``.
        label: Optional name suffix instead of the derived value label.
    """

    values: Tuple[Any, ...]
    child: Optional[PartitionPolicy] = None
    name: Optional[str] = None
    label: Optional[str] = None

    def __post_init__(self) -> None:
        values = tuple(self.values)
        if not values:
            raise ValueError("ListBranch values must not be empty")
        object.__setattr__(self, "values", values)
        if self.name is not None and not self.name.strip():
            raise ValueError("name must be a non-empty string")
        if self.label is not None and not self.label.strip():
            raise ValueError("label must be a non-empty string")


#: A first-level partition of any parent strategy.
PartitionBranch = Union[HashBranch, RangeBranch, ListBranch]


@dataclass(frozen=True)
class PartitionSpec:
    """One concrete partition the plan asks PostgreSQL to own.

    A spec is derived data: the executor turns it into ``CREATE TABLE ...
    PARTITION OF`` and compares it with the catalog. ``child_method`` /
    ``child_columns`` describe the clause attached to a branch that is itself
    partitioned; ``range_end`` is the exclusive upper bound of a RANGE leaf,
    which is what ``drop_before`` compares against a cutoff.
    """

    name: str
    parent: str
    method: str
    columns: Tuple[str, ...]
    bounds: Dict[str, Any]
    level: int
    is_leaf: bool
    child_method: Optional[str] = None
    child_columns: Tuple[str, ...] = ()
    range_start: Optional[Any] = None
    range_end: Optional[Any] = None
    label: Optional[str] = None


@dataclass(frozen=True)
class PostgresPartitionPlan:
    """A PostgreSQL partition tree, described by policies rather than names.

    Exactly one of *policy* and *branches* is supplied:

    * *policy* generates every partition directly below the parent. This is the
      flat single-table shape -- a RANGE parent with one monthly policy -- and
      is also how a HASH or LIST parent gets all of its first-level partitions.
    * *branches* lists first-level partitions explicitly, each optionally
      carrying its own child policy. Branches are how a PostgreSQL HASH parent
      gives different RANGE granularities to different hash buckets.

    Attributes:
        parent: Partitioned parent table name.
        method: Strategy the parent uses: ``RANGE``, ``LIST`` or ``HASH``.
        columns: Parent partition key columns.
        modulus: HASH bucket count when *branches* holds ``HashBranch`` values.
        branches: Explicit first-level partitions.
        policy: Auto-generated first-level partitions.
        schema: Optional schema shared by the parent and every partition.
        name_pattern: ``str.format`` pattern for generated names; must contain
            ``{suffix}`` and normally also ``{parent}``.
    """

    parent: str
    method: str
    columns: Union[str, Tuple[str, ...]] = ()
    modulus: Optional[int] = None
    branches: Tuple[PartitionBranch, ...] = ()
    policy: Optional[PartitionPolicy] = None
    schema: Optional[str] = None
    name_pattern: str = "{parent}_{suffix}"

    def __post_init__(self) -> None:
        if not isinstance(self.parent, str) or not self.parent.strip():
            raise ValueError("parent must be a non-empty string")
        object.__setattr__(self, "method", _normalise_method(self.method))
        object.__setattr__(self, "columns", _as_columns(self.columns))
        if not self.name_pattern:
            raise ValueError("name_pattern must not be empty")
        if "{suffix}" not in self.name_pattern:
            raise ValueError("name_pattern must contain {suffix}")
        if self.policy is not None and self.branches:
            raise ValueError("policy and branches are mutually exclusive")
        if self.schema is not None and not self.schema.strip():
            raise ValueError("schema must be a non-empty string")

        branches = tuple(self.branches)
        for index, branch in enumerate(branches):
            self._check_branch(index, branch)
        object.__setattr__(self, "branches", branches)

        if self.policy is not None:
            self._check_flat_policy(self.policy)
            self._check_policy(self.policy)

        if self.method == "HASH":
            if self.policy is not None:
                if not isinstance(self.policy, HashPolicy):
                    raise ValueError("a HASH parent's policy must be a HashPolicy")
            elif branches:
                if self.modulus is None:
                    raise ValueError("a HASH parent with branches requires modulus")
                if isinstance(self.modulus, bool) or not isinstance(self.modulus, int):
                    raise TypeError("modulus must be an integer")
                if self.modulus <= 0:
                    raise ValueError("modulus must be a positive integer")
                for branch in branches:
                    if branch.remainder >= self.modulus:
                        raise ValueError("HashBranch remainder must be less than modulus")
        elif self.modulus is not None:
            raise ValueError("modulus is only meaningful for a HASH parent")

    def _check_flat_policy(self, policy: PartitionPolicy) -> None:
        expected = {
            "RANGE": RangePolicy,
            "LIST": ListPolicy,
            "HASH": HashPolicy,
        }[self.method]
        if not isinstance(policy, expected):
            raise ValueError(
                "a %s parent's policy must be a %s" % (self.method, expected.__name__)
            )

    @staticmethod
    def _check_policy(policy: PartitionPolicy) -> None:
        """Validate a policy regardless of which parent level owns it."""
        if not isinstance(policy, (RangePolicy, ListPolicy, HashPolicy)):
            raise TypeError("child must be a RangePolicy, ListPolicy or HashPolicy")
        if isinstance(policy, HashPolicy) and policy.modulus < 1:
            raise ValueError("HashPolicy modulus must be positive")

    def _check_branch(self, index: int, branch: PartitionBranch) -> None:
        if self.method == "HASH":
            if not isinstance(branch, HashBranch):
                raise ValueError("a HASH parent's branches must be HashBranch values")
        elif self.method == "RANGE":
            if not isinstance(branch, RangeBranch):
                raise ValueError("a RANGE parent's branches must be RangeBranch values")
        else:
            if not isinstance(branch, ListBranch):
                raise ValueError("a LIST parent's branches must be ListBranch values")
        if branch.child is not None:
            self._check_policy(branch.child)

    def _name(self, suffix: str, **placeholders: Any) -> str:
        return self.name_pattern.format(
            parent=self.parent, suffix=suffix, **placeholders
        )

    def branch_suffix(self, index: int, branch: PartitionBranch) -> str:
        """Return the name suffix reserved for a first-level branch."""
        if branch.name is not None:
            return branch.name
        if isinstance(branch, HashBranch):
            return "p%d" % branch.remainder
        if branch.label is not None:
            return branch.label
        if isinstance(branch, RangeBranch):
            return branch.start.strftime("%Y_%m_%d")
        return _label_from_values(branch.values)

    def branch_name(self, index: int, branch: PartitionBranch) -> str:
        """Return the final table name of a first-level branch.

        ``name`` is a suffix, not a literal table name, so a caller that renames
        a branch still gets the plan's ``name_pattern`` and its child leaves
        compose under the same prefix.
        """
        suffix = self.branch_suffix(index, branch)
        return self._name(suffix, index=index, remainder=getattr(branch, "remainder", None))

    def _policy_specs(
        self,
        policy: PartitionPolicy,
        parent: str,
        level: int,
        prefix: Optional[str],
        through: Optional[Any],
    ) -> Iterator[PartitionSpec]:
        branch_placeholders = {"index": None, "remainder": None}
        if isinstance(policy, RangePolicy):
            for start, end in policy.iter_bounds(through=through):
                label = policy.label_for(start, end)
                suffix = label if prefix is None else "%s_%s" % (prefix, label)
                yield PartitionSpec(
                    name=self._name(suffix, **branch_placeholders),
                    parent=parent,
                    method="RANGE",
                    columns=policy.columns,
                    bounds={"from": start, "to": end},
                    level=level,
                    is_leaf=True,
                    range_start=start,
                    range_end=end,
                    label=label,
                )
        elif isinstance(policy, ListPolicy):
            for values, label in policy.iter_groups():
                suffix = label if prefix is None else "%s_%s" % (prefix, label)
                yield PartitionSpec(
                    name=self._name(suffix, **branch_placeholders),
                    parent=parent,
                    method="LIST",
                    columns=policy.columns,
                    bounds={"values": values},
                    level=level,
                    is_leaf=True,
                    label=label,
                )
        else:
            for remainder in policy.iter_remainders():
                label = "p%d" % remainder
                suffix = label if prefix is None else "%s_%s" % (prefix, label)
                yield PartitionSpec(
                    name=self._name(suffix, **branch_placeholders),
                    parent=parent,
                    method="HASH",
                    columns=policy.columns,
                    bounds={"modulus": policy.modulus, "remainder": remainder},
                    level=level,
                    is_leaf=True,
                    label=label,
                )

    def _branch_spec(
        self,
        index: int,
        branch: PartitionBranch,
    ) -> PartitionSpec:
        name = self.branch_name(index, branch)
        suffix = self.branch_suffix(index, branch)
        child_method = None
        child_columns: Tuple[str, ...] = ()
        if branch.child is not None:
            child_method = branch.child.method
            child_columns = branch.child.columns
        bounds: Dict[str, Any]
        if isinstance(branch, HashBranch):
            bounds = {"modulus": self.modulus, "remainder": branch.remainder}
        elif isinstance(branch, RangeBranch):
            bounds = {"from": branch.start, "to": branch.end}
        else:
            bounds = {"values": branch.values}
        return PartitionSpec(
            name=name,
            parent=self.parent,
            method=self.method,
            columns=self.columns,
            bounds=bounds,
            level=1,
            is_leaf=branch.child is None,
            child_method=child_method,
            child_columns=child_columns,
            range_start=branch.start if isinstance(branch, RangeBranch) else None,
            range_end=branch.end if isinstance(branch, RangeBranch) else None,
            label=suffix,
        )

    def iter_specs(self, through: Optional[Any] = None) -> Iterator[PartitionSpec]:
        """Yield every partition the plan describes.

        First-level branches are yielded before their children, in declaration
        order, so an executor can create them in yielded order. Names are
        checked for uniqueness here rather than at execution time.

        Raises:
            ValueError: if two generated partitions would share a name, or if
                a generated RANGE policy has no horizon.
        """
        yielded: Dict[str, PartitionSpec] = {}

        def emit(spec: PartitionSpec) -> PartitionSpec:
            if spec.name in yielded:
                raise ValueError("plan generates duplicate partition name %r" % spec.name)
            yielded[spec.name] = spec
            return spec

        if self.policy is not None:
            for spec in self._policy_specs(self.policy, self.parent, 1, None, through):
                yield emit(spec)
            return

        if not self.branches:
            return

        for index, branch in enumerate(self.branches):
            spec = self._branch_spec(index, branch)
            yield emit(spec)
            if branch.child is not None:
                prefix = spec.label
                for child in self._policy_specs(
                    branch.child, spec.name, 2, prefix, through
                ):
                    yield emit(child)

    def is_fully_bounded(self, through: Optional[Any] = None) -> bool:
        """Whether every policy has a finite horizon.

        A plan whose RANGE policy has neither explicit bounds nor *count*, and
        whose call supplies no *through*, cannot say which partitions are
        expected in the future. ``list`` still reports what exists, but it does
        not label extra partitions orphans in that case.
        """
        if through is not None:
            return True
        policies = []
        if self.policy is not None:
            policies.append(self.policy)
        for branch in self.branches:
            if branch.child is not None:
                policies.append(branch.child)
        for policy in policies:
            if isinstance(policy, RangePolicy):
                if policy.bounds is None and policy.count is None:
                    return False
        return True
