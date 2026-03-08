# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_textsearch_types.py
"""
Unit tests for PostgreSQL text search types.

Tests for:
- PostgresTsVector data class
- PostgresTsQuery data class
- PostgresTsVectorAdapter conversion
- PostgresTsQueryAdapter conversion
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.text_search import (
    TsVectorLexeme,
    PostgresTsVector,
    TsQueryLexeme,
    TsQueryOperator,
    PostgresTsQuery,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.text_search import (
    PostgresTsVectorAdapter,
    PostgresTsQueryAdapter,
)


class TestTsVectorLexeme:
    """Tests for TsVectorLexeme data class."""

    def test_create_lexeme_no_positions(self):
        """Test creating a lexeme without positions."""
        lex = TsVectorLexeme('hello')
        assert lex.lexeme == 'hello'
        assert lex.positions == []

    def test_create_lexeme_with_positions(self):
        """Test creating a lexeme with positions."""
        lex = TsVectorLexeme('hello', [(1, 'A'), (2, 'B')])
        assert lex.lexeme == 'hello'
        assert len(lex.positions) == 2
        assert lex.positions[0] == (1, 'A')
        assert lex.positions[1] == (2, 'B')

    def test_lexeme_to_postgres_string_no_positions(self):
        """Test converting lexeme without positions to string."""
        lex = TsVectorLexeme('hello')
        assert lex.to_postgres_string() == "'hello'"

    def test_lexeme_to_postgres_string_with_positions(self):
        """Test converting lexeme with positions to string."""
        lex = TsVectorLexeme('hello', [(1, 'A'), (2, None)])
        assert lex.to_postgres_string() == "'hello':1A,2"

    def test_lexeme_from_postgres_string_simple(self):
        """Test parsing simple lexeme string."""
        lex = TsVectorLexeme.from_postgres_string("'hello'")
        assert lex.lexeme == 'hello'
        assert lex.positions == []

    def test_lexeme_from_postgres_string_with_positions(self):
        """Test parsing lexeme with positions."""
        lex = TsVectorLexeme.from_postgres_string("'hello':1A,2B")
        assert lex.lexeme == 'hello'
        assert lex.positions == [(1, 'A'), (2, 'B')]

    def test_lexeme_from_postgres_string_no_weight(self):
        """Test parsing lexeme with positions but no weight."""
        lex = TsVectorLexeme.from_postgres_string("'hello':1,2,3")
        assert lex.lexeme == 'hello'
        assert lex.positions == [(1, None), (2, None), (3, None)]

    def test_lexeme_equality(self):
        """Test lexeme equality comparison."""
        lex1 = TsVectorLexeme('hello', [(1, 'A')])
        lex2 = TsVectorLexeme('hello', [(1, 'A')])
        lex3 = TsVectorLexeme('hello', [(1, 'B')])
        assert lex1 == lex2
        assert lex1 != lex3

    def test_lexeme_hash(self):
        """Test that lexemes are hashable."""
        lex1 = TsVectorLexeme('hello', [(1, 'A')])
        lex2 = TsVectorLexeme('hello', [(1, 'A')])
        assert hash(lex1) == hash(lex2)
        assert len({lex1, lex2}) == 1

    def test_lexeme_invalid_format(self):
        """Test that invalid format raises ValueError."""
        with pytest.raises(ValueError):
            TsVectorLexeme.from_postgres_string("invalid")


class TestPostgresTsVector:
    """Tests for PostgresTsVector data class."""

    def test_create_empty_tsvector(self):
        """Test creating an empty tsvector."""
        ts = PostgresTsVector()
        assert len(ts) == 0
        assert ts.to_postgres_string() == ''

    def test_add_lexeme_simple(self):
        """Test adding a simple lexeme."""
        ts = PostgresTsVector()
        ts.add_lexeme('hello')
        assert 'hello' in ts
        assert len(ts) == 1

    def test_add_lexeme_with_positions(self):
        """Test adding a lexeme with positions."""
        ts = PostgresTsVector()
        ts.add_lexeme('hello', [(1, 'A'), (2, 'B')])
        positions = ts.get_positions('hello')
        assert len(positions) == 2
        assert positions[0] == (1, 'A')

    def test_add_duplicate_lexeme_merges_positions(self):
        """Test that adding duplicate lexeme merges positions."""
        ts = PostgresTsVector()
        ts.add_lexeme('hello', [(1, 'A')])
        ts.add_lexeme('hello', [(3, 'B')])
        positions = ts.get_positions('hello')
        assert len(positions) == 2
        assert (1, 'A') in positions
        assert (3, 'B') in positions

    def test_get_lexemes(self):
        """Test getting all lexemes."""
        ts = PostgresTsVector()
        ts.add_lexeme('hello')
        ts.add_lexeme('world')
        lexemes = ts.get_lexemes()
        assert 'hello' in lexemes
        assert 'world' in lexemes

    def test_tsvector_to_postgres_string(self):
        """Test converting tsvector to string."""
        ts = PostgresTsVector()
        ts.add_lexeme('hello', [(1, 'A')])
        ts.add_lexeme('world', [(2, 'B')])
        result = ts.to_postgres_string()
        assert "'hello':1A" in result
        assert "'world':2B" in result

    def test_tsvector_sorted_output(self):
        """Test that lexemes are sorted alphabetically in output."""
        ts = PostgresTsVector()
        ts.add_lexeme('zebra')
        ts.add_lexeme('apple')
        result = ts.to_postgres_string()
        assert result.startswith("'apple'")
        assert result.endswith("'zebra'")

    def test_tsvector_from_postgres_string(self):
        """Test parsing tsvector from string."""
        ts = PostgresTsVector.from_postgres_string("'hello':1A 'world':2B")
        assert 'hello' in ts
        assert 'world' in ts
        assert ts.get_positions('hello') == [(1, 'A')]

    def test_tsvector_from_string_alias(self):
        """Test from_string alias method."""
        ts1 = PostgresTsVector.from_postgres_string("'hello':1A")
        ts2 = PostgresTsVector.from_string("'hello':1A")
        assert ts1 == ts2

    def test_tsvector_from_empty_string(self):
        """Test parsing empty string."""
        ts = PostgresTsVector.from_postgres_string('')
        assert len(ts) == 0

    def test_tsvector_contains(self):
        """Test contains check."""
        ts = PostgresTsVector()
        ts.add_lexeme('hello')
        assert 'hello' in ts
        assert 'world' not in ts

    def test_tsvector_equality(self):
        """Test tsvector equality comparison."""
        ts1 = PostgresTsVector.from_postgres_string("'hello':1A 'world':2B")
        ts2 = PostgresTsVector.from_postgres_string("'hello':1A 'world':2B")
        ts3 = PostgresTsVector.from_postgres_string("'hello':1A")
        assert ts1 == ts2
        assert ts1 != ts3

    def test_tsvector_repr(self):
        """Test tsvector repr."""
        ts = PostgresTsVector()
        assert repr(ts) == "PostgresTsVector()"

        ts = PostgresTsVector.from_postgres_string("'hello':1A")
        r = repr(ts)
        assert "PostgresTsVector" in r


class TestPostgresTsQuerySimple:
    """Tests for simple PostgresTsQuery operations."""

    def test_create_empty_query(self):
        """Test creating an empty query."""
        q = PostgresTsQuery()
        assert q.root is None
        assert q.to_postgres_string() == ''

    def test_parse_simple_lexeme(self):
        """Test parsing a simple lexeme."""
        q = PostgresTsQuery.parse("hello")
        assert q.to_postgres_string() == "'hello'"

    def test_parse_quoted_lexeme(self):
        """Test parsing a quoted lexeme."""
        q = PostgresTsQuery.parse("'hello world'")
        assert "'hello world'" in q.to_postgres_string()

    def test_term_factory_method(self):
        """Test term factory method."""
        q = PostgresTsQuery.term('hello')
        assert q.to_postgres_string() == "'hello'"

    def test_empty_query_parse(self):
        """Test parsing empty string."""
        q = PostgresTsQuery.parse('')
        assert q.root is None

        q = PostgresTsQuery.parse('   ')
        assert q.root is None

    def test_query_equality(self):
        """Test query equality comparison."""
        q1 = PostgresTsQuery.parse("hello")
        q2 = PostgresTsQuery.parse("hello")
        q3 = PostgresTsQuery.parse("world")
        assert q1 == q2
        assert q1 != q3


class TestPostgresTsQueryBoolean:
    """Tests for PostgresTsQuery boolean operators."""

    def test_parse_and_query(self):
        """Test parsing AND query."""
        q = PostgresTsQuery.parse("hello & world")
        result = q.to_postgres_string()
        assert '&' in result
        assert "'hello'" in result
        assert "'world'" in result

    def test_parse_or_query(self):
        """Test parsing OR query."""
        q = PostgresTsQuery.parse("hello | world")
        result = q.to_postgres_string()
        assert '|' in result

    def test_parse_not_query(self):
        """Test parsing NOT query."""
        q = PostgresTsQuery.parse("!hello")
        result = q.to_postgres_string()
        assert result.startswith("!'hello'")

    def test_parse_complex_boolean(self):
        """Test parsing complex boolean expression."""
        q = PostgresTsQuery.parse("(hello | world) & test")
        result = q.to_postgres_string()
        assert '&' in result
        assert '|' in result

    def test_and_method(self):
        """Test and_ method."""
        q1 = PostgresTsQuery.term('hello')
        q2 = PostgresTsQuery.term('world')
        combined = q1.and_(q2)
        assert '&' in combined.to_postgres_string()

    def test_or_method(self):
        """Test or_ method."""
        q1 = PostgresTsQuery.term('hello')
        q2 = PostgresTsQuery.term('world')
        combined = q1.or_(q2)
        assert '|' in combined.to_postgres_string()

    def test_not_method(self):
        """Test not_ method."""
        q = PostgresTsQuery.term('hello')
        negated = q.not_()
        assert negated.to_postgres_string().startswith("!'hello'")

    def test_not_empty_query(self):
        """Test NOT on empty query."""
        q = PostgresTsQuery()
        result = q.not_()
        assert result.root is None


class TestPostgresTsQueryPhrase:
    """Tests for PostgresTsQuery phrase search."""

    def test_parse_phrase_search(self):
        """Test parsing phrase search with <->."""
        q = PostgresTsQuery.parse("hello <-> world")
        result = q.to_postgres_string()
        assert '<->' in result

    def test_parse_distance_search(self):
        """Test parsing distance search with <N>."""
        q = PostgresTsQuery.parse("hello <3> world")
        result = q.to_postgres_string()
        assert '<3>' in result

    def test_followed_by_method(self):
        """Test followed_by method."""
        q1 = PostgresTsQuery.term('hello')
        q2 = PostgresTsQuery.term('world')
        phrase = q1.followed_by(q2)
        assert '<->' in phrase.to_postgres_string()

    def test_followed_by_with_distance(self):
        """Test followed_by with custom distance."""
        q1 = PostgresTsQuery.term('hello')
        q2 = PostgresTsQuery.term('world')
        phrase = q1.followed_by(q2, distance=5)
        assert '<5>' in phrase.to_postgres_string()

    def test_followed_by_empty_query_raises(self):
        """Test that followed_by with empty query raises ValueError."""
        q1 = PostgresTsQuery.term('hello')
        q2 = PostgresTsQuery()
        with pytest.raises(ValueError):
            q1.followed_by(q2)

    def test_chained_phrase_search(self):
        """Test chained phrase search."""
        q = PostgresTsQuery.parse("hello <-> world <-> test")
        result = q.to_postgres_string()
        assert result.count('<->') == 2


class TestPostgresTsQueryWeight:
    """Tests for PostgresTsQuery weight filtering."""

    def test_parse_weighted_lexeme(self):
        """Test parsing lexeme with weight."""
        q = PostgresTsQuery.parse("'hello':A")
        result = q.to_postgres_string()
        assert ':A' in result

    def test_parse_multiple_weights(self):
        """Test parsing lexeme with multiple weights."""
        q = PostgresTsQuery.parse("'hello':AB")
        result = q.to_postgres_string()
        assert ':AB' in result

    def test_term_with_weight(self):
        """Test creating term with weight."""
        q = PostgresTsQuery.term('hello', weight='A')
        result = q.to_postgres_string()
        assert ':A' in result


class TestPostgresTsQueryPrefix:
    """Tests for PostgresTsQuery prefix matching."""

    def test_parse_prefix_lexeme(self):
        """Test parsing prefix lexeme."""
        q = PostgresTsQuery.parse("'hello':*")
        result = q.to_postgres_string()
        assert ':*' in result

    def test_parse_prefix_with_weight(self):
        """Test parsing prefix lexeme with weight."""
        q = PostgresTsQuery.parse("'hello':*A")
        result = q.to_postgres_string()
        assert ':*' in result
        assert ':A' in result or 'A' in result

    def test_term_with_prefix(self):
        """Test creating term with prefix."""
        q = PostgresTsQuery.term('hello', prefix=True)
        result = q.to_postgres_string()
        assert ':*' in result

    def test_term_with_prefix_and_weight(self):
        """Test creating term with prefix and weight."""
        q = PostgresTsQuery.term('hello', weight='A', prefix=True)
        result = q.to_postgres_string()
        assert ':*' in result


class TestTsQueryLexeme:
    """Tests for TsQueryLexeme data class."""

    def test_create_lexeme_simple(self):
        """Test creating simple query lexeme."""
        lex = TsQueryLexeme(lexeme='hello')
        assert lex.lexeme == 'hello'
        assert lex.weight is None
        assert lex.prefix is False

    def test_create_lexeme_with_weight(self):
        """Test creating lexeme with weight."""
        lex = TsQueryLexeme(lexeme='hello', weight='A')
        assert lex.weight == 'A'

    def test_create_lexeme_with_prefix(self):
        """Test creating lexeme with prefix."""
        lex = TsQueryLexeme(lexeme='hello', prefix=True)
        assert lex.prefix is True

    def test_lexeme_to_postgres_string_simple(self):
        """Test converting simple lexeme to string."""
        lex = TsQueryLexeme(lexeme='hello')
        assert lex.to_postgres_string() == "'hello'"

    def test_lexeme_to_postgres_string_prefix(self):
        """Test converting prefix lexeme to string."""
        lex = TsQueryLexeme(lexeme='hello', prefix=True)
        assert lex.to_postgres_string() == "'hello':*"

    def test_lexeme_to_postgres_string_weight(self):
        """Test converting weighted lexeme to string."""
        lex = TsQueryLexeme(lexeme='hello', weight='A')
        assert lex.to_postgres_string() == "'hello':A"


class TestTsQueryOperator:
    """Tests for TsQueryOperator data class."""

    def test_create_and_operator(self):
        """Test creating AND operator."""
        left = TsQueryLexeme(lexeme='hello')
        right = TsQueryLexeme(lexeme='world')
        op = TsQueryOperator(operator='&', left=left, right=right)
        assert op.operator == '&'
        assert op.to_postgres_string() == "'hello' & 'world'"

    def test_create_or_operator(self):
        """Test creating OR operator."""
        left = TsQueryLexeme(lexeme='hello')
        right = TsQueryLexeme(lexeme='world')
        op = TsQueryOperator(operator='|', left=left, right=right)
        assert op.to_postgres_string() == "'hello' | 'world'"

    def test_create_not_operator(self):
        """Test creating NOT operator."""
        operand = TsQueryLexeme(lexeme='hello')
        op = TsQueryOperator(operator='!', left=operand)
        assert op.right is None
        assert op.to_postgres_string() == "!'hello'"

    def test_create_followed_by_operator(self):
        """Test creating FOLLOWED BY operator."""
        left = TsQueryLexeme(lexeme='hello')
        right = TsQueryLexeme(lexeme='world')
        op = TsQueryOperator(operator='<->', left=left, right=right)
        assert op.to_postgres_string() == "'hello' <-> 'world'"

    def test_create_distance_operator(self):
        """Test creating distance operator."""
        left = TsQueryLexeme(lexeme='hello')
        right = TsQueryLexeme(lexeme='world')
        op = TsQueryOperator(operator='<5>', left=left, right=right, distance=5)
        assert op.to_postgres_string() == "'hello' <5> 'world'"


class TestPostgresTsVectorAdapter:
    """Tests for PostgresTsVectorAdapter."""

    def test_adapter_supported_types(self):
        """Test supported types property."""
        adapter = PostgresTsVectorAdapter()
        supported = adapter.supported_types
        assert PostgresTsVector in supported

    def test_to_database_tsvector(self):
        """Test converting PostgresTsVector to database."""
        adapter = PostgresTsVectorAdapter()
        ts = PostgresTsVector.from_postgres_string("'hello':1A")
        result = adapter.to_database(ts, str)
        assert "'hello':1A" in result

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresTsVectorAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresTsVectorAdapter()
        result = adapter.to_database("'hello':1A", str)
        assert result == "'hello':1A"

    def test_to_database_dict(self):
        """Test converting dict to database."""
        adapter = PostgresTsVectorAdapter()
        data = {'hello': [(1, 'A')], 'world': [(2, 'B')]}
        result = adapter.to_database(data, str)
        assert "'hello':1A" in result
        assert "'world':2B" in result

    def test_to_database_list_strings(self):
        """Test converting list of strings to database."""
        adapter = PostgresTsVectorAdapter()
        data = ['hello', 'world']
        result = adapter.to_database(data, str)
        assert "'hello'" in result
        assert "'world'" in result

    def test_to_database_list_tuples(self):
        """Test converting list of tuples to database."""
        adapter = PostgresTsVectorAdapter()
        data = [('hello', [(1, 'A')]), 'world']
        result = adapter.to_database(data, str)
        assert "'hello':1A" in result

    def test_to_database_invalid_type(self):
        """Test that invalid types raise TypeError."""
        adapter = PostgresTsVectorAdapter()
        with pytest.raises(TypeError):
            adapter.to_database(123, str)

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresTsVectorAdapter()
        result = adapter.from_database(None, PostgresTsVector)
        assert result is None

    def test_from_database_string(self):
        """Test converting string from database."""
        adapter = PostgresTsVectorAdapter()
        result = adapter.from_database("'hello':1A", PostgresTsVector)
        assert 'hello' in result
        assert result.get_positions('hello') == [(1, 'A')]

    def test_from_database_tsvector_object(self):
        """Test converting PostgresTsVector object."""
        adapter = PostgresTsVectorAdapter()
        ts = PostgresTsVector.from_postgres_string("'hello':1A")
        result = adapter.from_database(ts, PostgresTsVector)
        assert result == ts

    def test_from_database_invalid_type(self):
        """Test that invalid types raise TypeError."""
        adapter = PostgresTsVectorAdapter()
        with pytest.raises(TypeError):
            adapter.from_database(123, PostgresTsVector)

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresTsVectorAdapter()
        values = [
            PostgresTsVector.from_postgres_string("'hello':1A"),
            PostgresTsVector.from_postgres_string("'world':2B"),
            None,
        ]
        results = adapter.to_database_batch(values, str)
        assert "'hello':1A" in results[0]
        assert "'world':2B" in results[1]
        assert results[2] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresTsVectorAdapter()
        values = ["'hello':1A", "'world':2B", None]
        results = adapter.from_database_batch(values, PostgresTsVector)
        assert 'hello' in results[0]
        assert 'world' in results[1]
        assert results[2] is None


class TestPostgresTsQueryAdapter:
    """Tests for PostgresTsQueryAdapter."""

    def test_adapter_supported_types(self):
        """Test supported types property."""
        adapter = PostgresTsQueryAdapter()
        supported = adapter.supported_types
        assert PostgresTsQuery in supported

    def test_to_database_tsquery(self):
        """Test converting PostgresTsQuery to database."""
        adapter = PostgresTsQueryAdapter()
        q = PostgresTsQuery.parse("hello & world")
        result = adapter.to_database(q, str)
        assert 'hello' in result
        assert 'world' in result

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresTsQueryAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresTsQueryAdapter()
        result = adapter.to_database("hello & world", str)
        assert result == "hello & world"

    def test_to_database_invalid_type(self):
        """Test that invalid types raise TypeError."""
        adapter = PostgresTsQueryAdapter()
        with pytest.raises(TypeError):
            adapter.to_database(123, str)

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresTsQueryAdapter()
        result = adapter.from_database(None, PostgresTsQuery)
        assert result is None

    def test_from_database_string(self):
        """Test converting string from database."""
        adapter = PostgresTsQueryAdapter()
        result = adapter.from_database("hello & world", PostgresTsQuery)
        assert result.root is not None

    def test_from_database_tsquery_object(self):
        """Test converting PostgresTsQuery object."""
        adapter = PostgresTsQueryAdapter()
        q = PostgresTsQuery.parse("hello & world")
        result = adapter.from_database(q, PostgresTsQuery)
        assert result == q

    def test_from_database_invalid_type(self):
        """Test that invalid types raise TypeError."""
        adapter = PostgresTsQueryAdapter()
        with pytest.raises(TypeError):
            adapter.from_database(123, PostgresTsQuery)

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresTsQueryAdapter()
        values = [
            PostgresTsQuery.parse("hello"),
            PostgresTsQuery.parse("world"),
            None,
        ]
        results = adapter.to_database_batch(values, str)
        assert 'hello' in results[0]
        assert 'world' in results[1]
        assert results[2] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresTsQueryAdapter()
        values = ["hello", "world", None]
        results = adapter.from_database_batch(values, PostgresTsQuery)
        assert results[0].root is not None
        assert results[1].root is not None
        assert results[2] is None


class TestTsVectorRoundTrip:
    """Tests for tsvector round-trip conversion."""

    def test_simple_round_trip(self):
        """Test simple lexeme round-trip."""
        original = "'hello':1A 'world':2B"
        ts = PostgresTsVector.from_postgres_string(original)
        result = ts.to_postgres_string()
        ts2 = PostgresTsVector.from_postgres_string(result)
        assert ts == ts2

    def test_complex_round_trip(self):
        """Test complex tsvector round-trip."""
        original = "'apple':1,3A 'banana':2B 'cherry':5"
        ts = PostgresTsVector.from_postgres_string(original)
        result = ts.to_postgres_string()
        ts2 = PostgresTsVector.from_postgres_string(result)
        assert ts == ts2


class TestTsQueryRoundTrip:
    """Tests for tsquery round-trip conversion."""

    def test_simple_round_trip(self):
        """Test simple query round-trip."""
        original = "hello & world"
        q = PostgresTsQuery.parse(original)
        result = q.to_postgres_string()
        q2 = PostgresTsQuery.parse(result)
        assert q == q2

    def test_complex_round_trip(self):
        """Test complex query round-trip."""
        original = "(hello | world) & test"
        q = PostgresTsQuery.parse(original)
        result = q.to_postgres_string()
        q2 = PostgresTsQuery.parse(result)
        assert q == q2

    def test_phrase_round_trip(self):
        """Test phrase search round-trip."""
        original = "hello <-> world"
        q = PostgresTsQuery.parse(original)
        result = q.to_postgres_string()
        q2 = PostgresTsQuery.parse(result)
        assert q == q2
