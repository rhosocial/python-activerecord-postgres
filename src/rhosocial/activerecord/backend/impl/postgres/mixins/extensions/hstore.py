# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/hstore.py
"""PostgreSQL hstore key-value storage functionality mixin.

This module provides functionality to check hstore extension features,
including type support and operators.
"""

from typing import Dict, Optional


class PostgresHstoreMixin:
    """hstore key-value storage functionality implementation."""

    def supports_hstore_type(self) -> bool:
        """Check if hstore supports hstore type."""
        return self.check_extension_feature("hstore", "type")

    def supports_hstore_operators(self) -> bool:
        """Check if hstore supports operators."""
        return self.check_extension_feature("hstore", "operators")

    def format_hstore_literal(self, data: Dict[str, str]) -> str:
        """Format an hstore literal value.

        Args:
            data: Dictionary of key-value pairs

        Returns:
            SQL hstore literal string

        Example:
            >>> format_hstore_literal({'a': '1', 'b': '2'})
            "'a=>\"1\", b=>\"2\"'"
        """
        pairs = []
        for key, value in data.items():
            # Escape quotes in values
            escaped_value = str(value).replace('"', '\\"')
            pairs.append(f'{key}=>"{escaped_value}"')
        return f"'{', '.join(pairs)}'"

    def format_hstore_constructor(self, pairs: list) -> str:
        """Format hstore constructor from array of key-value pairs.

        Args:
            pairs: List of (key, value) tuples or arrays

        Returns:
            SQL hstore constructor expression

        Example:
            >>> format_hstore_constructor([['a', '1'], ['b', '2']])
            "hstore(ARRAY[['a', '1'], ['b', '2']])"
        """
        if not pairs:
            return "hstore('{}'::text[])"

        formatted_pairs = []
        for pair in pairs:
            if isinstance(pair, (list, tuple)):
                k, v = pair[0], pair[1]
                formatted_pairs.append(f"['{k}', '{v}']")
            else:
                formatted_pairs.append(str(pair))

        return f"hstore(ARRAY[{', '.join(formatted_pairs)}])"

    def format_hstore_operator(
        self, column: str, operator: str, value: str, right_operand: Optional[str] = None
    ) -> str:
        """Format an hstore operator expression.

        Args:
            column: The hstore column name
            operator: The operator symbol
            value: The value to operate with
            right_operand: Optional right operand for binary operators

        Returns:
            SQL operator expression

        Example:
            >>> format_hstore_operator('data', '->', 'key')
            "data->'key'"
            >>> format_hstore_operator('data', '@>', "'a=>1'")
            "data @> 'a=>1'"
        """

        if operator in ("@>", "<@"):
            return f"{column} {operator} {value}"
        elif operator in ("?", "?|", "?&"):
            return f"{column} {operator} {value}"
        elif operator == "||":
            return f"{column} {operator} {value}"
        elif operator == "-":
            return f"{column} {operator} {value}"
        elif operator == "#-":
            return f"{column} {operator} {value}"
        else:
            # Default: key access operators
            return f"{column}{operator}{value}"

    def format_hstore_get_value(self, column: str, key: str, as_text: bool = False) -> str:
        """Format hstore key value retrieval.

        Args:
            column: The hstore column name
            key: The key to retrieve
            as_text: If True, return as text instead of value

        Returns:
            SQL expression

        Example:
            >>> format_hstore_get_value('data', 'name')
            "data->'name'"
            >>> format_hstore_get_value('data', 'name', as_text=True)
            "data->>'name'"
        """
        op = "->>" if as_text else "->"
        return f"{column}{op}'{key}'"

    def format_hstore_contains_key(self, column: str, key: str) -> str:
        """Format hstore key existence check.

        Args:
            column: The hstore column name
            key: The key to check

        Returns:
            SQL existence expression

        Example:
            >>> format_hstore_contains_key('data', 'name')
            "data ? 'name'"
        """
        return f"{column} ? '{key}'"

    def format_hstore_contains_all_keys(self, column: str, keys: list) -> str:
        """Format hstore check for all keys existing.

        Args:
            column: The hstore column name
            keys: List of keys that must all exist

        Returns:
            SQL expression

        Example:
            >>> format_hstore_contains_all_keys('data', ['a', 'b'])
            "data ?& ARRAY['a', 'b']"
        """
        keys_str = ", ".join(f"'{k}'" for k in keys)
        return f"{column} ?& ARRAY[{keys_str}]"

    def format_hstore_contains_any_keys(self, column: str, keys: list) -> str:
        """Format hstore check for any key existing.

        Args:
            column: The hstore column name
            keys: List of keys where any must exist

        Returns:
            SQL expression

        Example:
            >>> format_hstore_contains_any_keys('data', ['a', 'b'])
            "data ?| ARRAY['a', 'b']"
        """
        keys_str = ", ".join(f"'{k}'" for k in keys)
        return f"{column} ?| ARRAY[{keys_str}]"
