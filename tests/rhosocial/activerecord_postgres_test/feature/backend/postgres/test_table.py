from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.mixins.table import PostgresTableMixin


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

    def test_supports_table_tablespace(self):
        assert PostgresDialect().supports_table_tablespace() is True


class TestPostgresTableMixinDirect:
    """Test PostgresTableMixin directly (not through PostgresDialect)."""

    class _Host:
        version = (15, 0, 0)

    class _LowHost:
        version = (9, 4, 0)

    class _TableMixin(_Host, PostgresTableMixin):
        pass

    class _TableMixinLow(_LowHost, PostgresTableMixin):
        pass

    def test_supports_if_not_exists_table_direct(self):
        assert self._TableMixin().supports_if_not_exists_table() is True

    def test_supports_if_not_exists_table_low(self):
        assert not self._TableMixinLow().supports_if_not_exists_table()

    def test_supports_if_exists_table_direct(self):
        assert self._TableMixin().supports_if_exists_table() is True

    def test_supports_temporary_table_direct(self):
        assert self._TableMixin().supports_temporary_table() is True

    def test_supports_table_inheritance_direct(self):
        assert self._TableMixin().supports_table_inheritance() is True

    def test_supports_table_tablespace_direct(self):
        assert self._TableMixin().supports_table_tablespace() is True
