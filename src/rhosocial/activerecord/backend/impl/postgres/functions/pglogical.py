# src/rhosocial/activerecord/backend/impl/postgres/functions/pglogical.py
"""
PostgreSQL pglogical Extension Functions.

This module provides SQL expression generators for PostgreSQL pglogical
extension functions. All functions return Expression objects (FunctionCall)
that integrate with the expression-dialect architecture.

The pglogical extension provides logical streaming replication for PostgreSQL,
using a publish/subscribe model.

PostgreSQL Documentation: https://github.com/2ndQuadrant/pglogical

The pglogical extension must be installed:
    CREATE EXTENSION IF NOT EXISTS pglogical;

Supported functions:
- pglogical_create_node: Create a pglogical node
- pglogical_create_publication: Create a publication for replication
- pglogical_create_subscription: Create a subscription to a publication
- pglogical_show_subscription_status: Show subscription status
- pglogical_alter_subscription_synchronize: Synchronize a subscription

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
- They do not concatenate SQL strings directly
"""

from typing import Optional, Union, List, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports strings and existing BaseExpression objects.

    For string inputs, generates a literal expression. For
    BaseExpression inputs, returns them unchanged.

    Args:
        dialect: The SQL dialect instance
        expr: Value to convert

    Returns:
        BaseExpression representing the value
    """
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif isinstance(expr, str):
        return core.Literal(dialect, expr)
    else:
        return core.Literal(dialect, expr)


# ============== Node Management ==============

def pglogical_create_node(
    dialect: "SQLDialectBase",
    node_name: Union[str, "bases.BaseExpression"],
    dsn: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Create a pglogical node for logical replication.

    Creates a new pglogical node that can act as a provider and/or
    subscriber in a logical replication setup.

    Args:
        dialect: The SQL dialect instance
        node_name: Name for the new node (must be unique within the cluster)
        dsn: Connection DSN string for the node (e.g., 'host=127.0.0.1 dbname=mydb')

    Returns:
        FunctionCall for pglogical.create_node(node_name, dsn)

    Example:
        >>> from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
        >>> dialect = PostgresDialect()
        >>> pglogical_create_node(dialect, 'provider_node', 'host=127.0.0.1 dbname=mydb')
    """
    return core.FunctionCall(
        dialect, "pglogical.create_node",
        _convert_to_expression(dialect, node_name),
        _convert_to_expression(dialect, dsn),
    )


# ============== Publication Management ==============

def pglogical_create_publication(
    dialect: "SQLDialectBase",
    pub_name: Union[str, "bases.BaseExpression"],
    tables: Union[str, List[str], "bases.BaseExpression"],
    replication_sets: Optional[Union[List[str], "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Create a pglogical publication for replication.

    Creates a new publication that makes specified tables available
    for replication to subscriber nodes.

    Args:
        dialect: The SQL dialect instance
        pub_name: Name for the new publication
        tables: Table(s) to include in the publication. Can be a single
                table name, a list of table names, or an expression
        replication_sets: Optional list of replication set names to
                          associate the publication with. Defaults to
                          {'default', 'default_insert_only', 'ddl_sql'}
                          if not specified

    Returns:
        FunctionCall for pglogical.create_publication(pub_name, tables[, replication_sets])

    Example:
        >>> pglogical_create_publication(dialect, 'my_pub', '{users, orders}')
        >>> pglogical_create_publication(dialect, 'my_pub', '{users, orders}', '{default}')
    """
    args = [
        _convert_to_expression(dialect, pub_name),
        _convert_to_expression(dialect, tables),
    ]
    if replication_sets is not None:
        args.append(_convert_to_expression(dialect, replication_sets))
    return core.FunctionCall(dialect, "pglogical.create_publication", *args)


# ============== Subscription Management ==============

def pglogical_create_subscription(
    dialect: "SQLDialectBase",
    sub_name: Union[str, "bases.BaseExpression"],
    provider_dsn: Union[str, "bases.BaseExpression"],
    publication_names: Union[str, List[str], "bases.BaseExpression"],
    replication_sets: Optional[Union[List[str], "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Create a pglogical subscription to a publication.

    Creates a new subscription that replicates data from the specified
    provider's publication to the local node.

    Args:
        dialect: The SQL dialect instance
        sub_name: Name for the new subscription
        provider_dsn: Connection DSN string for the provider node
        publication_names: Publication name(s) to subscribe to. Can be a
                           single name, a list of names, or an expression
        replication_sets: Optional list of replication set names to
                          filter the subscription

    Returns:
        FunctionCall for pglogical.create_subscription(sub_name, provider_dsn, publication_names[, replication_sets])

    Example:
        >>> pglogical_create_subscription(
        ...     dialect, 'my_sub', 'host=provider.db dbname=mydb', '{my_pub}')
        >>> pglogical_create_subscription(
        ...     dialect, 'my_sub', 'host=provider.db dbname=mydb', '{my_pub}', '{default}')
    """
    args = [
        _convert_to_expression(dialect, sub_name),
        _convert_to_expression(dialect, provider_dsn),
        _convert_to_expression(dialect, publication_names),
    ]
    if replication_sets is not None:
        args.append(_convert_to_expression(dialect, replication_sets))
    return core.FunctionCall(dialect, "pglogical.create_subscription", *args)


def pglogical_show_subscription_status(
    dialect: "SQLDialectBase",
    sub_name: Optional[Union[str, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Show the status of pglogical subscriptions.

    Returns the current status of one or all subscriptions on the
    local node. When sub_name is provided, only the specified
    subscription's status is returned.

    Args:
        dialect: The SQL dialect instance
        sub_name: Optional subscription name to filter by. If None,
                  returns status for all subscriptions

    Returns:
        FunctionCall for pglogical.show_subscription_status([sub_name])

    Example:
        >>> pglogical_show_subscription_status(dialect)
        >>> pglogical_show_subscription_status(dialect, 'my_sub')
    """
    if sub_name is not None:
        return core.FunctionCall(
            dialect, "pglogical.show_subscription_status",
            _convert_to_expression(dialect, sub_name),
        )
    return core.FunctionCall(
        dialect, "pglogical.show_subscription_status",
    )


def pglogical_alter_subscription_synchronize(
    dialect: "SQLDialectBase",
    sub_name: Union[str, "bases.BaseExpression"],
    truncate: Optional[Union[bool, "bases.BaseExpression"]] = None,
) -> core.FunctionCall:
    """Synchronize a pglogical subscription.

    Starts the synchronization process for the specified subscription.
    This copies existing data from the provider to the subscriber.

    Args:
        dialect: The SQL dialect instance
        sub_name: Name of the subscription to synchronize
        truncate: Optional flag to truncate local tables before
                  synchronization. If True, existing data in the
                  subscriber tables will be removed before copying

    Returns:
        FunctionCall for pglogical.alter_subscription_synchronize(sub_name[, truncate])

    Example:
        >>> pglogical_alter_subscription_synchronize(dialect, 'my_sub')
        >>> pglogical_alter_subscription_synchronize(dialect, 'my_sub', truncate=True)
    """
    args = [
        _convert_to_expression(dialect, sub_name),
    ]
    if truncate is not None:
        args.append(_convert_to_expression(dialect, truncate))
    return core.FunctionCall(
        dialect, "pglogical.alter_subscription_synchronize", *args,
    )


__all__ = [
    # Node management
    "pglogical_create_node",
    # Publication management
    "pglogical_create_publication",
    # Subscription management
    "pglogical_create_subscription",
    "pglogical_show_subscription_status",
    "pglogical_alter_subscription_synchronize",
]
