"""Partition auto-management named expression functions.

This module provides named expression helpers for automated partition
management in PostgreSQL. Each function accepts ``dialect`` as its first
argument and returns a ``BaseExpression`` representing the partition DDL
operation.

All functions follow the named expression contract:
    - First parameter: ``dialect`` (the PostgreSQL dialect instance).
    - Returns: A ``BaseExpression`` whose ``to_sql()`` produces the DDL SQL.
    - Parameters: Additional keyword arguments control partition naming,
      bounds, and schema qualifiers.

Reference:
    - PostgreSQL Partitioning: https://www.postgresql.org/docs/current/ddl-partitioning.html
"""

from datetime import date, datetime, timedelta
from typing import Dict, Optional, Tuple

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresCreatePartitionExpression,
    PostgresPartitionMetadataExpression,
)


def create_next_monthly_partition(
    dialect,
    parent_table: str,
    schema: Optional[str] = None,
    reference_date: Optional[str] = None,
    lead_months: int = 1,
) -> PostgresCreatePartitionExpression:
    """Create the next monthly RANGE partition for a partitioned table.

    Generates a ``PostgresCreatePartitionExpression`` for a monthly RANGE
    partition whose start boundary is computed from the reference date
    (or today) advanced by ``lead_months``.

    Partition naming convention: ``<parent_table>_YYYY_MM``.

    Args:
        dialect: The PostgreSQL dialect instance.
        parent_table: Name of the partitioned parent table.
        schema: Optional schema name for the partition.
        reference_date: Reference date in ``YYYY-MM-DD`` format.
            Defaults to today.
        lead_months: Number of months ahead to create the partition.
            ``1`` (default) creates next month's partition.

    Returns:
        PostgresCreatePartitionExpression for the monthly partition.

    Raises:
        ValueError: If reference_date is not a valid date string.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect((14, 0, 0))
        >>> expr = create_next_monthly_partition(
        ...     dialect, "orders", reference_date="2026-06-01")
        >>> sql, _ = expr.to_sql()
        >>> print(sql)
        CREATE TABLE orders_2026_07 PARTITION OF orders FOR VALUES FROM ('2026-07-01') TO ('2026-08-01')
    """
    if reference_date is not None:
        try:
            ref = date.fromisoformat(reference_date)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"Invalid reference_date: {reference_date!r}. "
                "Expected format: YYYY-MM-DD."
            ) from exc
    else:
        ref = date.today()

    # Advance by lead_months
    target_month = ref.month + lead_months - 1
    target_year = ref.year + target_month // 12
    target_month = (target_month % 12) + 1

    start_date = date(target_year, target_month, 1)
    # Compute first day of next month
    next_month = target_month + 1
    next_year = target_year
    if next_month > 12:
        next_month = 1
        next_year += 1
    end_date = date(next_year, next_month, 1)

    partition_name = f"{parent_table}_{target_year}_{target_month:02d}"

    return PostgresCreatePartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table=parent_table,
        partition_type="RANGE",
        partition_values={"from": start_date.isoformat(), "to": end_date.isoformat()},
        schema=schema,
    )


def create_range_partition_for_month(
    dialect,
    parent_table: str,
    year: int,
    month: int,
    schema: Optional[str] = None,
) -> PostgresCreatePartitionExpression:
    """Create a monthly RANGE partition for a specific year and month.

    Generates a ``PostgresCreatePartitionExpression`` for a single monthly
    RANGE partition covering the given year-month.

    Partition naming convention: ``<parent_table>_YYYY_MM``.

    Args:
        dialect: The PostgreSQL dialect instance.
        parent_table: Name of the partitioned parent table.
        year: The year (e.g., 2026).
        month: The month (1-12).
        schema: Optional schema name for the partition.

    Returns:
        PostgresCreatePartitionExpression for the specified month.

    Raises:
        ValueError: If month is not in 1..12.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect((14, 0, 0))
        >>> expr = create_range_partition_for_month(dialect, "orders", 2026, 6)
        >>> sql, _ = expr.to_sql()
        >>> print(sql)
        CREATE TABLE orders_2026_06 PARTITION OF orders FOR VALUES FROM ('2026-06-01') TO ('2026-07-01')
    """
    if not 1 <= month <= 12:
        raise ValueError(f"Invalid month: {month}. Must be 1-12.")

    start_date = date(year, month, 1)
    next_month = month + 1
    next_year = year
    if next_month > 12:
        next_month = 1
        next_year += 1
    end_date = date(next_year, next_month, 1)

    partition_name = f"{parent_table}_{year}_{month:02d}"

    return PostgresCreatePartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table=parent_table,
        partition_type="RANGE",
        partition_values={"from": start_date.isoformat(), "to": end_date.isoformat()},
        schema=schema,
    )


def create_range_partition_for_quarter(
    dialect,
    parent_table: str,
    year: int,
    quarter: int,
    schema: Optional[str] = None,
) -> PostgresCreatePartitionExpression:
    """Create a quarterly RANGE partition for a specific year and quarter.

    Generates a ``PostgresCreatePartitionExpression`` for a quarterly RANGE
    partition covering the given year-quarter.

    Partition naming convention: ``<parent_table>_YYYY_Q<quarter>``.

    Args:
        dialect: The PostgreSQL dialect instance.
        parent_table: Name of the partitioned parent table.
        year: The year (e.g., 2026).
        quarter: The quarter (1-4).
        schema: Optional schema name for the partition.

    Returns:
        PostgresCreatePartitionExpression for the specified quarter.

    Raises:
        ValueError: If quarter is not in 1..4.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect((14, 0, 0))
        >>> expr = create_range_partition_for_quarter(dialect, "orders", 2026, 1)
        >>> sql, _ = expr.to_sql()
        >>> print(sql)
        CREATE TABLE orders_2026_q1 PARTITION OF orders FOR VALUES FROM ('2026-01-01') TO ('2026-04-01')
    """
    if not 1 <= quarter <= 4:
        raise ValueError(f"Invalid quarter: {quarter}. Must be 1-4.")

    start_month = (quarter - 1) * 3 + 1
    start_date = date(year, start_month, 1)
    end_month = start_month + 3
    end_year = year
    if end_month > 12:
        end_month = 1
        end_year += 1
    end_date = date(end_year, end_month, 1)

    partition_name = f"{parent_table}_{year}_q{quarter}"

    return PostgresCreatePartitionExpression(
        dialect=dialect,
        partition_name=partition_name,
        parent_table=parent_table,
        partition_type="RANGE",
        partition_values={"from": start_date.isoformat(), "to": end_date.isoformat()},
        schema=schema,
    )


def create_range_partitions_for_interval(
    dialect,
    parent_table: str,
    from_date: str,
    to_date: str,
    interval: str = "month",
    schema: Optional[str] = None,
) -> Tuple[BaseExpression, ...]:
    """Create multiple RANGE partitions covering a date interval.

    Generates a tuple of ``PostgresCreatePartitionExpression`` objects,
    one for each unit (month or quarter) in the interval from
    ``from_date`` to ``to_date``.

    This function returns **multiple** expressions instead of a single
    one, so it must be called programmatically rather than through the
    standard named expression resolver (which expects a single
    ``BaseExpression``).

    Args:
        dialect: The PostgreSQL dialect instance.
        parent_table: Name of the partitioned parent table.
        from_date: Start date in ``YYYY-MM-DD`` format (inclusive).
        to_date: End date in ``YYYY-MM-DD`` format (exclusive).
        interval: Partition interval: ``"month"`` or ``"quarter"``.
        schema: Optional schema name.

    Returns:
        Tuple of PostgresCreatePartitionExpression instances, one per
        interval unit.

    Raises:
        ValueError: If interval is not ``"month"`` or ``"quarter"``, or
            if date range is invalid.

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect((14, 0, 0))
        >>> exprs = create_range_partitions_for_interval(
        ...     dialect, "orders", "2026-01-01", "2026-03-01")
        >>> len(exprs)
        2
        >>> exprs[0].partition_name
        'orders_2026_01'
    """
    if interval not in ("month", "quarter"):
        raise ValueError(f"Invalid interval: {interval!r}. Must be 'month' or 'quarter'.")

    try:
        start = date.fromisoformat(from_date)
        end = date.fromisoformat(to_date)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Invalid date format. Expected YYYY-MM-DD, got from_date={from_date!r}, to_date={to_date!r}."
        ) from exc

    if start >= end:
        raise ValueError("from_date must be before to_date.")

    results: Tuple[BaseExpression, ...] = ()

    current = start
    while current < end:
        if interval == "month":
            expr = create_range_partition_for_month(
                dialect, parent_table, current.year, current.month, schema=schema,
            )
            # Advance to next month
            next_month = current.month + 1
            next_year = current.year
            if next_month > 12:
                next_month = 1
                next_year += 1
            current = date(next_year, next_month, 1)
        else:  # quarter
            # Find quarter for current date
            q = (current.month - 1) // 3 + 1
            expr = create_range_partition_for_quarter(
                dialect, parent_table, current.year, q, schema=schema,
            )
            # Advance to next quarter start
            q_start_month = (q * 3) + 1
            q_start_year = current.year
            if q_start_month > 12:
                q_start_month = 1
                q_start_year += 1
            current = date(q_start_year, q_start_month, 1)

        results = results + (expr,)

    return results
