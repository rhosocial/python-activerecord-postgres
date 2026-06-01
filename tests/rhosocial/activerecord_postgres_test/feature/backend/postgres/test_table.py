from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestTableSupport:
    """Test PostgresTableMixin feature detection."""

    def test_supports_if_not_exists_table_pg95(self):
        assert PostgresDialect((9, 5, 0)).supports_if_not_exists_table() is True

    def test_supports_if_not_exists_table_pg94(self):
        assert PostgresDialect((9, 4, 0)).supports_if_not_exists_table() is False

    def test_supports_if_exists_table(self):
        assert PostgresDialect().supports_if_exists_table() is True

    def test_supports_temporary_table(self):
        assert PostgresDialect().supports_temporary_table() is True

    def test_supports_table_inheritance(self):
        assert PostgresDialect().supports_table_inheritance() is True

    def test_supports_table_partitioning_pg10(self):
        assert PostgresDialect((10, 0, 0)).supports_table_partitioning() is True

    def test_supports_table_partitioning_pg94(self):
        assert PostgresDialect((9, 4, 0)).supports_table_partitioning() is False

    def test_supports_table_tablespace(self):
        assert PostgresDialect().supports_table_tablespace() is True
