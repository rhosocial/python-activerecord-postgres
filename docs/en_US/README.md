# rhosocial-activerecord PostgreSQL Backend Documentation

> 🤖 **AI Learning Assistant**: Key concepts in this documentation are marked with 💡 AI Prompt. When you encounter concepts you don't understand, you can ask the AI assistant directly.

> **Example:** "How does the PostgreSQL backend handle JSONB data types? What are the advantages over regular JSON?"

> 📖 **For detailed usage, please refer to**: [AI-Assisted Development Guide](introduction/ai_assistance.md)

## Table of Contents

1. **[Introduction](introduction/README.md)**
    *   **[PostgreSQL Backend Overview](introduction/README.md)**: Why choose PostgreSQL backend
    *   **[Relationship with Core Library](introduction/relationship.md)**: Integration of rhosocial-activerecord and PostgreSQL backend
    *   **[Supported Versions](introduction/supported_versions.md)**: PostgreSQL 8.0~17, Python 3.8+ support

2. **[Installation & Configuration](installation_and_configuration/README.md)**
    *   **[Installation Guide](installation_and_configuration/installation.md)**: pip installation and environment requirements
    *   **[Connection Configuration](installation_and_configuration/configuration.md)**: host, port, database, username, password and other configuration options
    *   **[SSL/TLS Configuration](installation_and_configuration/ssl.md)**: secure connection settings
    *   **[Connection Management](installation_and_configuration/pool.md)**: connect-on-use pattern
    *   **[Client Encoding](installation_and_configuration/encoding.md)**: character encoding settings

3. **[PostgreSQL Specific Features](postgres_specific_features/README.md)**
    *   **[PostgreSQL-Specific Field Types](postgres_specific_features/field_types.md)**: ARRAY, JSONB, UUID, Range types
    *   **[PostgreSQL Dialect Expressions](postgres_specific_features/dialect.md)**: PostgreSQL-specific SQL dialect
    *   **[Advanced Indexing](postgres_specific_features/indexing.md)**: GIN, GiST, BRIN index types
    *   **[RETURNING Clause](postgres_specific_features/returning.md)**: INSERT/UPDATE/DELETE RETURNING support

4. **[Transaction Support](transaction_support/README.md)**
    *   **[Transaction Isolation Levels](transaction_support/isolation_level.md)**: READ COMMITTED, REPEATABLE READ, SERIALIZABLE
    *   **[Savepoint Support](transaction_support/savepoint.md)**: nested transactions
    *   **[DEFERRABLE Mode](transaction_support/deferrable.md)**: deferred constraint checking
    *   **[Deadlock Handling](transaction_support/deadlock.md)**: failure retry mechanism

5. **[Type Adapters](type_adapters/README.md)**
    *   **[PostgreSQL to Python Type Mapping](type_adapters/mapping.md)**: type conversion rules
    *   **[Custom Type Adapters](type_adapters/custom.md)**: extending type support
    *   **[Timezone Handling](type_adapters/timezone.md)**: TIMESTAMP WITH TIME ZONE
    *   **[Array Type Handling](type_adapters/arrays.md)**: PostgreSQL array support

6. **[Testing](testing/README.md)**
    *   **[Test Configuration](testing/configuration.md)**: test environment setup
    *   **[Using testsuite for Testing](testing/testsuite.md)**: test suite usage
    *   **[Local PostgreSQL Testing](testing/local.md)**: local database testing

7. **[Troubleshooting](troubleshooting/README.md)**
    *   **[Common Connection Errors](troubleshooting/connection.md)**: connection issue diagnosis
    *   **[Performance Issues](troubleshooting/performance.md)**: performance bottleneck analysis
    *   **[SQL Standard Compliance](troubleshooting/sql_standard.md)**: strict SQL compliance handling

> 📖 **Core Library Documentation**: To learn about the complete functionality of the ActiveRecord framework, please refer to [rhosocial-activerecord documentation](https://github.com/rhosocial/python-activerecord/tree/main/docs/en_US).
