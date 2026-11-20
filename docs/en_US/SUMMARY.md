# Table of Contents

* [rhosocial-activerecord PostgreSQL Backend Documentation](README.md)

## Introduction

* [Overview](introduction/README.md)
* [Relationship with the core library](introduction/relationship.md)
* [Supported versions](introduction/supported_versions.md)

## Installation & Configuration

* [Installation & Configuration overview](installation_and_configuration/README.md)
* [Installation guide](installation_and_configuration/installation.md)
* [Connection configuration](installation_and_configuration/configuration.md)
* [SSL/TLS configuration](installation_and_configuration/ssl.md)
* [Connection management](installation_and_configuration/pool.md)
* [Client encoding](installation_and_configuration/encoding.md)
* [Backend groups](installation_and_configuration/backend_group.md)

## Plugins

* [Plugins overview](plugins/README.md)
* [hstore](plugins/hstore.md)
* [pg_trgm](plugins/pg_trgm.md)
* [pgvector](plugins/pgvector.md)
* [PostGIS](plugins/postgis.md)

## PostgreSQL Specific Features

* [PostgreSQL Specific Features overview](postgres_specific_features/README.md)
* [PostgreSQL-specific field types](postgres_specific_features/field_types.md)
* [PostgreSQL dialect expressions](postgres_specific_features/dialect.md)
* [Advanced indexing](postgres_specific_features/indexing.md)
* [RETURNING clause](postgres_specific_features/returning.md)
* [EXPLAIN support](postgres_specific_features/explain.md)
* [Introspection](postgres_specific_features/introspection.md)
* [Partitioning](postgres_specific_features/partition.md)
* [Property graph queries](postgres_specific_features/property_graph_query.md)
* [Protocol support](postgres_specific_features/protocol_support.md)

## DDL Operations

* [DDL overview](ddl/README.md)
* [Materialized views](ddl/materialized_view.md)
* [DDL example](../examples/chapter_04_ddl/ddl.py)

## Transaction Support

* [Transaction Support overview](transaction_support/README.md)
* [Transaction isolation levels](transaction_support/isolation_level.md)
* [Savepoint support](transaction_support/savepoint.md)
* [DEFERRABLE mode](transaction_support/deferrable.md)
* [Deadlock handling](transaction_support/deadlock.md)

## Type Adapters

* [Type Adapters overview](type_adapters/README.md)
* [PostgreSQL to Python type mapping](type_adapters/mapping.md)
* [Custom type adapters](type_adapters/custom.md)
* [Timezone handling](type_adapters/timezone.md)
* [Array type handling](type_adapters/arrays.md)
* [Array comparison](type_adapters/array_comparison.md)

## Testing

* [Testing overview](testing/README.md)
* [Test configuration](testing/configuration.md)
* [Using testsuite for testing](https://github.com/rhosocial/python-activerecord-testsuite/blob/main/docs/en_US/README.md)
* [Local PostgreSQL testing](testing/local.md)

## Troubleshooting

* [Troubleshooting overview](troubleshooting/README.md)
* [Common connection errors](troubleshooting/connection.md)
* [Performance issues](troubleshooting/performance.md)

## Scenarios

* [Scenarios overview](scenarios/README.md)
* [Parallel worker processing](scenarios/parallel_workers.md)

## Worker Isolation Experiment

* [Worker connection isolation](../examples/chapter_05_worker_isolation/README.md)
