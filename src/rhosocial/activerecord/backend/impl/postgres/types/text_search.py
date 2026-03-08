# src/rhosocial/activerecord/backend/impl/postgres/types/text_search.py
"""
PostgreSQL text search types representation.

This module provides Python classes for PostgreSQL text search types:
- TsVectorLexeme: A single lexeme entry with positions and weights
- PostgresTsVector: Text search vector (sorted list of distinct lexemes)
- TsQueryNode, TsQueryLexeme, TsQueryOperator: Query parse tree nodes
- PostgresTsQuery: Text search query

PostgreSQL full-text search allows searching through documents using
lexemes (normalized words) with optional weights and positions.

Weight values:
- A: Highest weight (default 1.0)
- B: Second weight (default 0.4)
- C: Third weight (default 0.2)
- D: Lowest weight (default 0.1)

Examples:
    # Create a tsvector
    tsvector = PostgresTsVector.from_string("'hello':1A 'world':2B")

    # Create a tsquery
    tsquery = PostgresTsQuery.parse("hello & world")

    # Search operations
    # @@ operator: tsvector @@ tsquery returns true if match

For type adapters (conversion between Python and database),
see adapters.text_search module.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Union
import re


@dataclass
class TsVectorLexeme:
    """A single lexeme entry in a tsvector.

    Represents a normalized word with optional positions and weights.

    Attributes:
        lexeme: The normalized word/term
        positions: List of (position, weight) tuples
        position: Integer position in the document (1-based)
        weight: Optional weight character ('A', 'B', 'C', 'D')

    Examples:
        TsVectorLexeme('hello', [(1, 'A')])
        TsVectorLexeme('world', [(2, None), (5, 'B')])
    """
    lexeme: str
    positions: List[Tuple[int, Optional[str]]] = field(default_factory=list)

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL tsvector lexeme format.

        Returns:
            str: Lexeme in format 'word':1A,2B or just 'word' if no positions
        """
        if not self.positions:
            return f"'{self.lexeme}'"

        pos_strs = []
        for pos, weight in self.positions:
            if weight:
                pos_strs.append(f"{pos}{weight}")
            else:
                pos_strs.append(str(pos))

        return f"'{self.lexeme}':{','.join(pos_strs)}"

    @classmethod
    def from_postgres_string(cls, value: str) -> 'TsVectorLexeme':
        """Parse PostgreSQL tsvector lexeme string.

        Args:
            value: String like 'word':1A,2B or 'word'

        Returns:
            TsVectorLexeme: Parsed lexeme object

        Raises:
            ValueError: If the format is invalid
        """
        value = value.strip()

        # Match pattern: 'word':positions or just 'word'
        match = re.match(r"'([^']+)':(\d+[A-D]?(?:,\d+[A-D]?)*)$|'([^']+)'$", value)

        if match:
            if match.group(1):
                lexeme = match.group(1)
                positions_str = match.group(2)
                positions = cls._parse_positions(positions_str)
            else:
                lexeme = match.group(3)
                positions = []
            return cls(lexeme=lexeme, positions=positions)

        raise ValueError(f"Invalid tsvector lexeme format: {value}")

    @staticmethod
    def _parse_positions(positions_str: str) -> List[Tuple[int, Optional[str]]]:
        """Parse position string like '1A,2B,3' into list of tuples."""
        positions = []
        for part in positions_str.split(','):
            part = part.strip()
            if not part:
                continue
            # Extract position number and optional weight
            pos_match = re.match(r'(\d+)([A-D])?$', part)
            if pos_match:
                pos = int(pos_match.group(1))
                weight = pos_match.group(2) if pos_match.group(2) else None
                positions.append((pos, weight))
            else:
                raise ValueError(f"Invalid position format: {part}")
        return positions

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TsVectorLexeme):
            return NotImplemented
        return self.lexeme == other.lexeme and self.positions == other.positions

    def __hash__(self) -> int:
        return hash((self.lexeme, tuple(self.positions)))


@dataclass
class PostgresTsVector:
    """PostgreSQL TSVECTOR type representation.

    A tsvector is a sorted list of distinct lexemes (normalized words).
    Each lexeme can have positions and weights indicating where in the
    document the word appears.

    Attributes:
        lexemes: Dictionary mapping lexeme strings to TsVectorLexeme objects

    Examples:
        # Empty tsvector
        tsvector = PostgresTsVector()

        # From string
        tsvector = PostgresTsVector.from_string("'hello':1A 'world':2B")

        # Add lexeme
        tsvector = PostgresTsVector()
        tsvector.add_lexeme('hello', [(1, 'A')])
    """
    lexemes: Dict[str, TsVectorLexeme] = field(default_factory=dict)

    def add_lexeme(
        self,
        lexeme: str,
        positions: Optional[List[Tuple[int, Optional[str]]]] = None
    ) -> None:
        """Add a lexeme with optional positions.

        Args:
            lexeme: The normalized word
            positions: List of (position, weight) tuples
        """
        if positions is None:
            positions = []

        if lexeme in self.lexemes:
            # Merge positions
            existing = self.lexemes[lexeme]
            existing.positions.extend(positions)
            # Remove duplicates and sort
            seen = set()
            unique_positions = []
            for pos in existing.positions:
                if pos not in seen:
                    seen.add(pos)
                    unique_positions.append(pos)
            existing.positions = sorted(unique_positions, key=lambda x: x[0])
        else:
            self.lexemes[lexeme] = TsVectorLexeme(lexeme, positions.copy())

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL tsvector literal.

        Returns:
            str: PostgreSQL tsvector format
        """
        if not self.lexemes:
            return ''

        # Sort lexemes alphabetically (PostgreSQL stores them sorted)
        sorted_lexemes = sorted(self.lexemes.values(), key=lambda x: x.lexeme)
        return ' '.join(lex.to_postgres_string() for lex in sorted_lexemes)

    @classmethod
    def from_postgres_string(cls, value: str) -> 'PostgresTsVector':
        """Parse PostgreSQL tsvector string.

        Args:
            value: PostgreSQL tsvector string like "'hello':1A 'world':2B"

        Returns:
            PostgresTsVector: Parsed tsvector object
        """
        if not value or not value.strip():
            return cls()

        tsvector = cls()
        value = value.strip()

        # Parse lexemes
        # Pattern: 'word':positions or 'word'
        i = 0
        while i < len(value):
            if value[i] == "'":
                # Find closing quote
                j = value.find("'", i + 1)
                if j == -1:
                    raise ValueError(f"Unclosed quote at position {i}")

                lexeme = value[i + 1:j]
                i = j + 1

                # Check for positions
                positions = []
                if i < len(value) and value[i] == ':':
                    i += 1
                    # Parse positions
                    pos_str = ''
                    while i < len(value) and value[i] not in " \t\n":
                        pos_str += value[i]
                        i += 1

                    positions = TsVectorLexeme._parse_positions(pos_str)
                else:
                    # Skip whitespace
                    while i < len(value) and value[i] in " \t\n":
                        i += 1

                tsvector.add_lexeme(lexeme, positions)
            else:
                i += 1

        return tsvector

    @classmethod
    def from_string(cls, value: str) -> 'PostgresTsVector':
        """Alias for from_postgres_string for compatibility."""
        return cls.from_postgres_string(value)

    def get_lexemes(self) -> List[str]:
        """Get all lexeme strings."""
        return list(self.lexemes.keys())

    def get_positions(self, lexeme: str) -> List[Tuple[int, Optional[str]]]:
        """Get positions for a specific lexeme."""
        if lexeme in self.lexemes:
            return self.lexemes[lexeme].positions.copy()
        return []

    def __contains__(self, lexeme: str) -> bool:
        """Check if a lexeme is in the tsvector."""
        return lexeme in self.lexemes

    def __len__(self) -> int:
        """Get the number of unique lexemes."""
        return len(self.lexemes)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PostgresTsVector):
            return NotImplemented
        return self.lexemes == other.lexemes

    def __repr__(self) -> str:
        if not self.lexemes:
            return "PostgresTsVector()"
        return f"PostgresTsVector.from_string({self.to_postgres_string()!r})"


class TsQueryNode:
    """Base class for tsquery nodes.

    Represents a node in the tsquery parse tree.
    """

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL tsquery format."""
        raise NotImplementedError("Subclasses must implement to_postgres_string")


@dataclass
class TsQueryLexeme(TsQueryNode):
    """A lexeme node in a tsquery.

    Attributes:
        lexeme: The search term
        weight: Optional weight filter ('A', 'B', 'C', 'D' or combination)
        prefix: True if this is a prefix match (lexeme:*)
    """
    lexeme: str
    weight: Optional[str] = None
    prefix: bool = False

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL tsquery format."""
        result = f"'{self.lexeme}'"
        if self.prefix:
            result += ':*'
        if self.weight:
            result += f':{self.weight}'
        return result

    @classmethod
    def from_postgres_string(cls, value: str) -> 'TsQueryLexeme':
        """Parse PostgreSQL tsquery lexeme."""
        value = value.strip()

        # Remove quotes and parse modifiers
        match = re.match(r"'([^']*)'(?::\*([A-D]+)|:([A-D]+)|:\*)?", value)
        if match:
            lexeme = match.group(1)
            weight = match.group(2) or match.group(3) if match.group(2) or match.group(3) else None
            prefix = bool(match.group(0) and ':*' in match.group(0))
            return cls(lexeme=lexeme, weight=weight, prefix=prefix)

        raise ValueError(f"Invalid tsquery lexeme: {value}")


@dataclass
class TsQueryOperator(TsQueryNode):
    """An operator node in a tsquery.

    Supported operators:
    - & (AND): Both operands must match
    - | (OR): Either operand must match
    - ! (NOT): Negation (unary operator)
    - <-> (FOLLOWED BY): Phrase search (N distance)

    Attributes:
        operator: The operator ('&', '|', '!', '<->', or '<N>' for distance)
        left: Left operand (or only operand for NOT)
        right: Right operand (None for NOT)
        distance: Distance for FOLLOWED BY operator (default 1)
    """
    operator: str
    left: TsQueryNode
    right: Optional[TsQueryNode] = None
    distance: int = 1

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL tsquery format."""
        if self.operator == '!':
            return f"!{self._node_to_string(self.left)}"
        elif self.operator == '<->':
            return f"{self._node_to_string(self.left)} <-> {self._node_to_string(self.right)}"
        elif self.operator.startswith('<') and self.operator.endswith('>'):
            return f"{self._node_to_string(self.left)} {self.operator} {self._node_to_string(self.right)}"
        else:
            return f"{self._node_to_string(self.left)} {self.operator} {self._node_to_string(self.right)}"

    def _node_to_string(self, node: Optional[TsQueryNode]) -> str:
        """Convert a node to string, adding parentheses for operators."""
        if node is None:
            return ''
        if isinstance(node, TsQueryOperator):
            return f"({node.to_postgres_string()})"
        if hasattr(node, 'to_postgres_string'):
            return node.to_postgres_string()
        return str(node)


class PostgresTsQuery:
    """PostgreSQL TSQUERY type representation.

    A tsquery represents a text search query with boolean operators.
    It can be matched against tsvector values using the @@ operator.

    Supported operators:
    - & (AND): Both terms must match
    - | (OR): Either term must match
    - ! (NOT): Term must not match
    - <-> (FOLLOWED BY): Phrase search, terms must be adjacent
    - <N> (DISTANCE): Terms must be within N positions

    Supported features:
    - Parentheses for grouping
    - Weight filtering: 'term':A or 'term':AB
    - Prefix matching: 'term':*

    Examples:
        # Simple term
        query = PostgresTsQuery.parse("hello")

        # Boolean operators
        query = PostgresTsQuery.parse("hello & world")
        query = PostgresTsQuery.parse("hello | world")
        query = PostgresTsQuery.parse("!hello")

        # Phrase search
        query = PostgresTsQuery.parse("hello <-> world")

        # Distance search
        query = PostgresTsQuery.parse("hello <3> world")

        # Weighted terms
        query = PostgresTsQuery.parse("'hello':A")

        # Prefix matching
        query = PostgresTsQuery.parse("'hello':*")
    """

    def __init__(self, root: Optional[TsQueryNode] = None):
        """Initialize a tsquery with an optional root node."""
        self.root = root

    @classmethod
    def parse(cls, query: str) -> 'PostgresTsQuery':
        """Parse a PostgreSQL tsquery string.

        Args:
            query: PostgreSQL tsquery string

        Returns:
            PostgresTsQuery: Parsed query object

        Raises:
            ValueError: If the query format is invalid
        """
        if not query or not query.strip():
            return cls()

        parser = _TsQueryParser(query)
        return cls(parser.parse())

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL tsquery format.

        Returns:
            str: PostgreSQL tsquery string
        """
        if self.root is None:
            return ''
        return self.root.to_postgres_string()

    @classmethod
    def from_postgres_string(cls, value: str) -> 'PostgresTsQuery':
        """Alias for parse for compatibility."""
        return cls.parse(value)

    @classmethod
    def from_string(cls, value: str) -> 'PostgresTsQuery':
        """Alias for parse for compatibility."""
        return cls.parse(value)

    @staticmethod
    def term(lexeme: str, weight: Optional[str] = None, prefix: bool = False) -> 'PostgresTsQuery':
        """Create a simple term query.

        Args:
            lexeme: The search term
            weight: Optional weight filter ('A', 'B', 'C', 'D' or combination)
            prefix: True for prefix matching

        Returns:
            PostgresTsQuery: Query object
        """
        return PostgresTsQuery(TsQueryLexeme(lexeme=lexeme, weight=weight, prefix=prefix))

    def and_(self, other: 'PostgresTsQuery') -> 'PostgresTsQuery':
        """Combine with another query using AND.

        Args:
            other: Another query

        Returns:
            PostgresTsQuery: Combined query
        """
        if self.root is None:
            return other
        if other.root is None:
            return self
        return PostgresTsQuery(TsQueryOperator(operator='&', left=self.root, right=other.root))

    def or_(self, other: 'PostgresTsQuery') -> 'PostgresTsQuery':
        """Combine with another query using OR.

        Args:
            other: Another query

        Returns:
            PostgresTsQuery: Combined query
        """
        if self.root is None:
            return other
        if other.root is None:
            return self
        return PostgresTsQuery(TsQueryOperator(operator='|', left=self.root, right=other.root))

    def not_(self) -> 'PostgresTsQuery':
        """Negate this query.

        Returns:
            PostgresTsQuery: Negated query
        """
        if self.root is None:
            return self
        return PostgresTsQuery(TsQueryOperator(operator='!', left=self.root))

    def followed_by(self, other: 'PostgresTsQuery', distance: int = 1) -> 'PostgresTsQuery':
        """Combine with another query using FOLLOWED BY operator.

        Args:
            other: Another query
            distance: Distance between terms (default 1 for adjacent)

        Returns:
            PostgresTsQuery: Combined query
        """
        if self.root is None or other.root is None:
            raise ValueError("Both queries must have terms for FOLLOWED BY")

        if distance == 1:
            return PostgresTsQuery(TsQueryOperator(operator='<->', left=self.root, right=other.root))
        else:
            return PostgresTsQuery(
                TsQueryOperator(operator=f'<{distance}>', left=self.root, right=other.root, distance=distance)
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PostgresTsQuery):
            return NotImplemented
        return self.to_postgres_string() == other.to_postgres_string()

    def __repr__(self) -> str:
        query_str = self.to_postgres_string()
        if not query_str:
            return "PostgresTsQuery()"
        return f"PostgresTsQuery.parse({query_str!r})"


class _TsQueryParser:
    """Parser for PostgreSQL tsquery strings.

    Implements a recursive descent parser for tsquery syntax.
    """

    def __init__(self, query: str):
        self.query = query
        self.pos = 0
        self.length = len(query)

    def parse(self) -> Optional[TsQueryNode]:
        """Parse the entire query."""
        self._skip_whitespace()
        if self.pos >= self.length:
            return None

        result = self._parse_or()
        return result

    def _parse_or(self) -> TsQueryNode:
        """Parse OR expressions (lowest precedence)."""
        left = self._parse_and()

        while self.pos < self.length:
            self._skip_whitespace()
            if self.pos >= self.length:
                break

            if self._peek() == '|':
                self.pos += 1
                self._skip_whitespace()
                right = self._parse_and()
                left = TsQueryOperator(operator='|', left=left, right=right)
            else:
                break

        return left

    def _parse_and(self) -> TsQueryNode:
        """Parse AND expressions."""
        left = self._parse_followed_by()

        while self.pos < self.length:
            self._skip_whitespace()
            if self.pos >= self.length:
                break

            if self._peek() == '&':
                self.pos += 1
                self._skip_whitespace()
                right = self._parse_followed_by()
                left = TsQueryOperator(operator='&', left=left, right=right)
            else:
                break

        return left

    def _parse_followed_by(self) -> TsQueryNode:
        """Parse FOLLOWED BY expressions (<->, <N>)."""
        left = self._parse_not()

        while self.pos < self.length:
            self._skip_whitespace()
            if self.pos >= self.length:
                break

            # Check for <N> or <->
            if self._peek() == '<':
                start_pos = self.pos
                self.pos += 1

                # Check for <-> (followed by)
                if self.pos < self.length and self.query[self.pos] == '-':
                    self.pos += 1
                    if self.pos < self.length and self.query[self.pos] == '>':
                        self.pos += 1
                        self._skip_whitespace()
                        right = self._parse_not()
                        left = TsQueryOperator(operator='<->', left=left, right=right)
                        continue

                # Check for <N> (distance)
                self.pos = start_pos + 1
                num_str = ''
                while self.pos < self.length and self.query[self.pos].isdigit():
                    num_str += self.query[self.pos]
                    self.pos += 1

                if num_str and self.pos < self.length and self.query[self.pos] == '>':
                    distance = int(num_str)
                    self.pos += 1
                    self._skip_whitespace()
                    right = self._parse_not()
                    left = TsQueryOperator(operator=f'<{distance}>', left=left, right=right, distance=distance)
                    continue

                # Not a valid operator, restore position
                self.pos = start_pos
                break
            else:
                break

        return left

    def _parse_not(self) -> TsQueryNode:
        """Parse NOT expressions."""
        self._skip_whitespace()

        if self.pos < self.length and self.query[self.pos] == '!':
            self.pos += 1
            self._skip_whitespace()
            operand = self._parse_primary()
            return TsQueryOperator(operator='!', left=operand)

        return self._parse_primary()

    def _parse_primary(self) -> TsQueryNode:
        """Parse primary expressions (terms or parenthesized expressions)."""
        self._skip_whitespace()

        if self.pos >= self.length:
            raise ValueError("Unexpected end of query")

        # Parenthesized expression
        if self.query[self.pos] == '(':
            self.pos += 1
            node = self._parse_or()
            self._skip_whitespace()
            if self.pos >= self.length or self.query[self.pos] != ')':
                raise ValueError("Missing closing parenthesis")
            self.pos += 1
            return node

        # Quoted lexeme
        if self.query[self.pos] == "'":
            return self._parse_lexeme()

        # Unquoted lexeme (single word)
        return self._parse_unquoted_lexeme()

    def _parse_lexeme(self) -> TsQueryLexeme:
        """Parse a quoted lexeme with optional weight and prefix."""
        if self.query[self.pos] != "'":
            raise ValueError("Expected opening quote")

        self.pos += 1
        lexeme = ''

        while self.pos < self.length and self.query[self.pos] != "'":
            lexeme += self.query[self.pos]
            self.pos += 1

        if self.pos >= self.length:
            raise ValueError("Unclosed quote")

        self.pos += 1  # Skip closing quote

        # Parse modifiers
        weight = None
        prefix = False

        if self.pos < self.length and self.query[self.pos] == ':':
            self.pos += 1

            if self.pos < self.length and self.query[self.pos] == '*':
                prefix = True
                self.pos += 1

            if self.pos < self.length and self.query[self.pos] in 'ABCD':
                weight = ''
                while self.pos < self.length and self.query[self.pos] in 'ABCD':
                    weight += self.query[self.pos]
                    self.pos += 1

        return TsQueryLexeme(lexeme=lexeme, weight=weight, prefix=prefix)

    def _parse_unquoted_lexeme(self) -> TsQueryLexeme:
        """Parse an unquoted lexeme (single word)."""
        lexeme = ''

        while self.pos < self.length:
            c = self.query[self.pos]
            if c in ' \t\n&|!<>()':
                break
            lexeme += c
            self.pos += 1

        if not lexeme:
            raise ValueError(f"Expected lexeme at position {self.pos}")

        return TsQueryLexeme(lexeme=lexeme)

    def _peek(self) -> Optional[str]:
        """Peek at the current character."""
        if self.pos < self.length:
            return self.query[self.pos]
        return None

    def _skip_whitespace(self) -> None:
        """Skip whitespace characters."""
        while self.pos < self.length and self.query[self.pos] in ' \t\n':
            self.pos += 1


# Text search utility functions for SQL generation

def to_tsvector(document: str, config: Optional[str] = None) -> str:
    """Generate SQL expression for PostgreSQL to_tsvector function.

    Args:
        document: The document text to convert to tsvector
        config: Optional text search configuration (e.g., 'english', 'simple')

    Returns:
        str: SQL expression for to_tsvector function

    Examples:
        >>> to_tsvector("hello world")
        "to_tsvector('hello world')"
        >>> to_tsvector("hello world", "english")
        "to_tsvector('english', 'hello world')"
    """
    if config:
        return f"to_tsvector('{config}', {document})"
    return f"to_tsvector({document})"


def to_tsquery(query: str, config: Optional[str] = None) -> str:
    """Generate SQL expression for PostgreSQL to_tsquery function.

    Args:
        query: The query string to convert to tsquery
        config: Optional text search configuration

    Returns:
        str: SQL expression for to_tsquery function

    Examples:
        >>> to_tsquery("hello & world")
        "to_tsquery('hello & world')"
        >>> to_tsquery("hello & world", "english")
        "to_tsquery('english', 'hello & world')"
    """
    if config:
        return f"to_tsquery('{config}', '{query}')"
    return f"to_tsquery('{query}')"


def plainto_tsquery(query: str, config: Optional[str] = None) -> str:
    """Generate SQL expression for PostgreSQL plainto_tsquery function.

    Converts plain text to tsquery, handling special characters.

    Args:
        query: The plain text query
        config: Optional text search configuration

    Returns:
        str: SQL expression for plainto_tsquery function

    Examples:
        >>> plainto_tsquery("hello world")
        "plainto_tsquery('hello world')"
        >>> plainto_tsquery("hello world", "english")
        "plainto_tsquery('english', 'hello world')"
    """
    if config:
        return f"plainto_tsquery('{config}', '{query}')"
    return f"plainto_tsquery('{query}')"


def phraseto_tsquery(query: str, config: Optional[str] = None) -> str:
    """Generate SQL expression for PostgreSQL phraseto_tsquery function.

    Converts text to tsquery for phrase search.

    Args:
        query: The phrase query string
        config: Optional text search configuration

    Returns:
        str: SQL expression for phraseto_tsquery function

    Examples:
        >>> phraseto_tsquery("hello world")
        "phraseto_tsquery('hello world')"
        >>> phraseto_tsquery("hello world", "english")
        "phraseto_tsquery('english', 'hello world')"
    """
    if config:
        return f"phraseto_tsquery('{config}', '{query}')"
    return f"phraseto_tsquery('{query}')"


def websearch_to_tsquery(query: str, config: Optional[str] = None) -> str:
    """Generate SQL expression for PostgreSQL websearch_to_tsquery function.

    Converts web search style query to tsquery (PostgreSQL 11+).

    Args:
        query: The web search query string
        config: Optional text search configuration

    Returns:
        str: SQL expression for websearch_to_tsquery function

    Examples:
        >>> websearch_to_tsquery("hello -world")
        "websearch_to_tsquery('hello -world')"
        >>> websearch_to_tsquery("hello -world", "english")
        "websearch_to_tsquery('english', 'hello -world')"
    """
    if config:
        return f"websearch_to_tsquery('{config}', '{query}')"
    return f"websearch_to_tsquery('{query}')"


def ts_matches(vector: str, query: str) -> str:
    """Generate SQL expression for tsvector @@ tsquery match operator.

    Args:
        vector: The tsvector expression or column
        query: The tsquery expression

    Returns:
        str: SQL expression using @@ operator

    Examples:
        >>> ts_matches("title_tsvector", "to_tsquery('hello')")
        "title_tsvector @@ to_tsquery('hello')"
    """
    return f"{vector} @@ {query}"


def ts_matches_expr(vector: str, query: str) -> str:
    """Generate SQL expression for tsvector @@@ tsquery match operator.

    The @@@ operator is a deprecated alias for @@.

    Args:
        vector: The tsvector expression or column
        query: The tsquery expression

    Returns:
        str: SQL expression using @@@ operator

    Examples:
        >>> ts_matches_expr("title_tsvector", "to_tsquery('hello')")
        "title_tsvector @@@ to_tsquery('hello')"
    """
    return f"{vector} @@@ {query}"


def ts_rank(
    vector: str,
    query: str,
    weights: Optional[List[float]] = None,
    normalization: int = 0
) -> str:
    """Generate SQL expression for PostgreSQL ts_rank function.

    Calculates relevance rank for tsvector matching tsquery.

    Args:
        vector: The tsvector expression or column
        query: The tsquery expression
        weights: Optional list of 4 weights [D, C, B, A] (default [0.1, 0.2, 0.4, 1.0])
        normalization: Normalization method (0-32, default 0)

    Returns:
        str: SQL expression for ts_rank function

    Examples:
        >>> ts_rank("title_tsvector", "to_tsquery('hello')")
        "ts_rank(title_tsvector, to_tsquery('hello'))"
        >>> ts_rank("title_tsvector", "to_tsquery('hello')", [0.1, 0.2, 0.4, 1.0])
        "ts_rank(array[0.1, 0.2, 0.4, 1.0], title_tsvector, to_tsquery('hello'))"
        >>> ts_rank("title_tsvector", "to_tsquery('hello')", normalization=1)
        "ts_rank(title_tsvector, to_tsquery('hello'), 1)"
    """
    if weights:
        weights_str = f"array[{', '.join(str(w) for w in weights)}]"
        if normalization != 0:
            return f"ts_rank({weights_str}, {vector}, {query}, {normalization})"
        return f"ts_rank({weights_str}, {vector}, {query})"
    if normalization != 0:
        return f"ts_rank({vector}, {query}, {normalization})"
    return f"ts_rank({vector}, {query})"


def ts_rank_cd(
    vector: str,
    query: str,
    weights: Optional[List[float]] = None,
    normalization: int = 0
) -> str:
    """Generate SQL expression for PostgreSQL ts_rank_cd function.

    Calculates relevance rank using cover density.

    Args:
        vector: The tsvector expression or column
        query: The tsquery expression
        weights: Optional list of 4 weights [D, C, B, A]
        normalization: Normalization method (0-32, default 0)

    Returns:
        str: SQL expression for ts_rank_cd function

    Examples:
        >>> ts_rank_cd("title_tsvector", "to_tsquery('hello')")
        "ts_rank_cd(title_tsvector, to_tsquery('hello'))"
        >>> ts_rank_cd("title_tsvector", "to_tsquery('hello')", [0.1, 0.2, 0.4, 1.0])
        "ts_rank_cd(array[0.1, 0.2, 0.4, 1.0], title_tsvector, to_tsquery('hello'))"
    """
    if weights:
        weights_str = f"array[{', '.join(str(w) for w in weights)}]"
        if normalization != 0:
            return f"ts_rank_cd({weights_str}, {vector}, {query}, {normalization})"
        return f"ts_rank_cd({weights_str}, {vector}, {query})"
    if normalization != 0:
        return f"ts_rank_cd({vector}, {query}, {normalization})"
    return f"ts_rank_cd({vector}, {query})"


def ts_headline(
    document: str,
    query: str,
    config: Optional[str] = None,
    options: Optional[str] = None
) -> str:
    """Generate SQL expression for PostgreSQL ts_headline function.

    Displays query matches in document with highlighting.

    Args:
        document: The document text or expression
        query: The tsquery expression
        config: Optional text search configuration
        options: Optional headline options string

    Returns:
        str: SQL expression for ts_headline function

    Examples:
        >>> ts_headline("content", "to_tsquery('hello')")
        "ts_headline(content, to_tsquery('hello'))"
        >>> ts_headline("content", "to_tsquery('hello')", config="english")
        "ts_headline('english', content, to_tsquery('hello'))"
        >>> ts_headline("content", "to_tsquery('hello')", options="StartSel=<b>, StopSel=</b>")
        "ts_headline(content, to_tsquery('hello'), 'StartSel=<b>, StopSel=</b>')"
    """
    if config:
        if options:
            return f"ts_headline('{config}', {document}, {query}, '{options}')"
        return f"ts_headline('{config}', {document}, {query})"
    if options:
        return f"ts_headline({document}, {query}, '{options}')"
    return f"ts_headline({document}, {query})"


def tsvector_concat(vec1: str, vec2: str) -> str:
    """Generate SQL expression for tsvector concatenation.

    Args:
        vec1: First tsvector expression
        vec2: Second tsvector expression

    Returns:
        str: SQL expression using || operator

    Examples:
        >>> tsvector_concat("title_tsvector", "body_tsvector")
        "title_tsvector || body_tsvector"
    """
    return f"{vec1} || {vec2}"


def tsvector_strip(vec: str) -> str:
    """Generate SQL expression for strip function.

    Removes positions and weights from tsvector.

    Args:
        vec: The tsvector expression

    Returns:
        str: SQL expression for strip function

    Examples:
        >>> tsvector_strip("title_tsvector")
        "strip(title_tsvector)"
    """
    return f"strip({vec})"


def tsvector_setweight(vec: str, weight: str) -> str:
    """Generate SQL expression for setweight function.

    Sets weight for all lexemes in tsvector.

    Args:
        vec: The tsvector expression
        weight: Weight character ('A', 'B', 'C', or 'D')

    Returns:
        str: SQL expression for setweight function

    Examples:
        >>> tsvector_setweight("title_tsvector", "A")
        "setweight(title_tsvector, 'A')"
    """
    return f"setweight({vec}, '{weight}')"


def tsvector_length(vec: str) -> str:
    """Generate SQL expression for tsvector length.

    Returns the number of lexemes in tsvector.

    Args:
        vec: The tsvector expression

    Returns:
        str: SQL expression for length function

    Examples:
        >>> tsvector_length("title_tsvector")
        "length(title_tsvector)"
    """
    return f"length({vec})"


__all__ = [
    'TsVectorLexeme',
    'PostgresTsVector',
    'TsQueryNode',
    'TsQueryLexeme',
    'TsQueryOperator',
    'PostgresTsQuery',
    'to_tsvector',
    'to_tsquery',
    'plainto_tsquery',
    'phraseto_tsquery',
    'websearch_to_tsquery',
    'ts_matches',
    'ts_matches_expr',
    'ts_rank',
    'ts_rank_cd',
    'ts_headline',
    'tsvector_concat',
    'tsvector_strip',
    'tsvector_setweight',
    'tsvector_length',
]
