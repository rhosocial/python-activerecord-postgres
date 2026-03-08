# src/rhosocial/activerecord/backend/impl/postgres/types/json.py
"""PostgreSQL jsonpath type representation.

This module provides PostgresJsonPath for representing PostgreSQL
jsonpath values in Python.

jsonpath is SQL/JSON path language for querying JSON data:
- $ represents the root variable
- . accesses object members
- [] accesses array elements
- @ is the current element in filter expressions

Available since PostgreSQL 12.

Note: For JSON functions (json_path_root, jsonb_path_query, etc.),
see the functions.json module.

For type adapter (conversion between Python and database),
see adapters.json.PostgresJsonPathAdapter.
"""
from dataclasses import dataclass
from typing import Union
import re


@dataclass
class PostgresJsonPath:
    """PostgreSQL jsonpath type representation.

    jsonpath is a path language for SQL/JSON that allows you to navigate
    and query JSON data. It's similar to XPath for XML.

    Basic syntax:
    - $: Root of the JSON document
    - .key: Access object member named 'key'
    - [n]: Access array element at index n
    - [*]: Access all array elements (wildcard)
    - .*: Access all object members (wildcard)
    - @: Current node in filter expressions
    - ?(...): Filter expression

    Attributes:
        path: JSON path expression string

    Examples:
        # Root path
        PostgresJsonPath('$')

        # Access nested key
        PostgresJsonPath('$.store.book')

        # Access array element
        PostgresJsonPath('$.items[0]')

        # With filter
        PostgresJsonPath('$.items[*]?(@.price < 10)')

        # Using builder functions from functions.json
        from rhosocial.activerecord.backend.impl.postgres.functions import json_path_root, json_path_key
        path = json_path_root()
        path = json_path_key(path, 'store')
    """
    path: str

    def __post_init__(self):
        """Validate that path starts with $."""
        if not self.path:
            raise ValueError("jsonpath cannot be empty")
        if not self.path.startswith('$'):
            raise ValueError("jsonpath must start with '$'")

    def __str__(self) -> str:
        """Return path expression."""
        return self.path

    def __repr__(self) -> str:
        return f"PostgresJsonPath({self.path!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PostgresJsonPath):
            return self.path == other.path
        if isinstance(other, str):
            return self.path == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.path)

    def is_valid(self) -> bool:
        """Check if path syntax is valid (basic check).

        This performs a basic syntax validation. PostgreSQL will
        perform full validation when using the path.

        Returns:
            True if path appears to be valid
        """
        if not self.path.startswith('$'):
            return False

        path = self.path

        valid_chars = r"[$.\[\]@*?\(\)'\"\w\s<>!=&|]"
        if not re.match(f"^{valid_chars}+$", path):
            return False

        bracket_count = 0
        paren_count = 0
        for char in path:
            if char == '[':
                bracket_count += 1
            elif char == ']':
                bracket_count -= 1
                if bracket_count < 0:
                    return False
            elif char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
                if paren_count < 0:
                    return False

        return bracket_count == 0 and paren_count == 0

    def key(self, key: str) -> 'PostgresJsonPath':
        """Add object member access.

        Args:
            key: Member name to access

        Returns:
            New PostgresJsonPath with key appended

        Examples:
            PostgresJsonPath('$').key('name')  # -> '$.name'
        """
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', key):
            return PostgresJsonPath(f"{self.path}.{key}")
        else:
            return PostgresJsonPath(f"{self.path}.\"{key}\"")

    def index(self, index: Union[int, str]) -> 'PostgresJsonPath':
        """Add array index access.

        Args:
            index: Array index (integer or 'last' or 'last-N')

        Returns:
            New PostgresJsonPath with index appended

        Examples:
            PostgresJsonPath('$').index(0)  # -> '$[0]'
            PostgresJsonPath('$').index('last')  # -> '$[last]'
        """
        return PostgresJsonPath(f"{self.path}[{index}]")

    def wildcard_array(self) -> 'PostgresJsonPath':
        """Add array wildcard ([*]).

        Returns:
            New PostgresJsonPath with wildcard appended
        """
        return PostgresJsonPath(f"{self.path}[*]")

    def wildcard_object(self) -> 'PostgresJsonPath':
        """Add object member wildcard (.*).

        Returns:
            New PostgresJsonPath with wildcard appended
        """
        return PostgresJsonPath(f"{self.path}.*")

    def filter(self, condition: str) -> 'PostgresJsonPath':
        """Add filter expression.

        Args:
            condition: Filter condition (use @ for current element)

        Returns:
            New PostgresJsonPath with filter appended

        Examples:
            PostgresJsonPath('$').filter('@.price < 10')
        """
        return PostgresJsonPath(f"{self.path}?({condition})")

    def to_sql_string(self) -> str:
        """Convert to SQL string literal.

        Returns:
            SQL string with proper quoting
        """
        escaped = self.path.replace("'", "''")
        return f"'{escaped}'"


__all__ = ['PostgresJsonPath']
