from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect

def test_format_limit_offset():
    """Tests the format_limit_offset method of the PostgresDialect."""
    dialect = PostgresDialect(config=PostgresConnectionConfig())

    # Test with limit only
    assert dialect.format_limit_offset(limit=10) == "LIMIT 10"

    # Test with offset only
    assert dialect.format_limit_offset(offset=5) == "OFFSET 5"

    # Test with both limit and offset
    assert dialect.format_limit_offset(limit=20, offset=10) == "LIMIT 20 OFFSET 10"

    # Test with None values
    assert dialect.format_limit_offset(limit=None, offset=None) == ""

    # Test with zero values
    assert dialect.format_limit_offset(limit=0, offset=0) == "LIMIT 0 OFFSET 0"

def test_format_identifier():
    """Tests the format_identifier method."""
    dialect = PostgresDialect(config=PostgresConnectionConfig())

    # Test simple identifier
    assert dialect.format_identifier("my_table") == '"my_table"'

    # The dialect should handle spaces by quoting
    assert dialect.format_identifier("my table") == '"my table"'

    # Test identifier with a double quote inside, which should be escaped
    assert dialect.format_identifier('my"table') == '"my""table"'

    # Test an identifier that is already quoted (it should be escaped and re-quoted)
    assert dialect.format_identifier('"my_table"') == '"""my_table"""'

def test_format_string_literal():
    """Tests the format_string_literal method."""
    dialect = PostgresDialect(config=PostgresConnectionConfig())

    # Test a simple string
    assert dialect.format_string_literal("hello") == "'hello'"

    # Test a string with a single quote
    assert dialect.format_string_literal("it's a test") == "'it''s a test'"
