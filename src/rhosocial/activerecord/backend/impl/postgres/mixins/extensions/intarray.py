# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/intarray.py
"""PostgreSQL intarray integer array functionality mixin.

This module provides functionality to check intarray extension features,
including operators, functions, and index support.
"""


class PostgresIntarrayMixin:
    """intarray integer array implementation."""

    def supports_intarray_operators(self) -> bool:
        """Check if intarray supports operators."""
        return self.check_extension_feature("intarray", "operators")

    def supports_intarray_functions(self) -> bool:
        """Check if intarray supports functions."""
        return self.check_extension_feature("intarray", "functions")

    def supports_intarray_index(self) -> bool:
        """Check if intarray supports index."""
        return self.check_extension_feature("intarray", "index")

    def format_intarray_function(self, function_name: str, *args) -> str:
        """Format an intarray function call.

        Args:
            function_name: Name of the function
            *args: Function arguments

        Returns:
            SQL function call string

        Example:
            >>> format_intarray_function('idx', 'array_col', 5)
            "idx(array_col, 5)"
            >>> format_intarray_function('uniq', 'array_col')
            "uniq(array_col)"
        """
        args_str = ", ".join(str(arg) for arg in args)
        return f"{function_name}({args_str})"

    def format_intarray_operator(self, column: str, operator: str, value: str) -> str:
        """Format an intarray operator expression.

        Args:
            column: The intarray column name
            operator: The operator symbol
            value: The value to compare with

        Returns:
            SQL operator expression

        Example:
            >>> format_intarray_operator('ids', '@>', 'ARRAY[1,2]')
            "ids @> ARRAY[1,2]"
            >>> format_intarray_operator('ids', '&&', 'ARRAY[3,4]')
            "ids && ARRAY[3,4]"
        """
        return f"{column} {operator} {value}"

    def format_intarray_contains(self, column: str, values: list) -> str:
        """Format intarray contains check (column contains all values).

        Args:
            column: The intarray column name
            values: List of integers to check

        Returns:
            SQL contains expression

        Example:
            >>> format_intarray_contains('ids', [1, 2, 3])
            "ids @> ARRAY[1,2,3]"
        """
        arr_str = f"ARRAY[{','.join(str(v) for v in values)}]"
        return f"{column} @> {arr_str}"

    def format_intarray_overlaps(self, column: str, values: list) -> str:
        """Format intarray overlaps check (any common elements).

        Args:
            column: The intarray column name
            values: List of integers to check

        Returns:
            SQL overlaps expression

        Example:
            >>> format_intarray_overlaps('ids', [1, 2, 3])
            "ids && ARRAY[1,2,3]"
        """
        arr_str = f"ARRAY[{','.join(str(v) for v in values)}]"
        return f"{column} && {arr_str}"

    def format_intarray_idx(self, column: str, value: int) -> str:
        """Format idx function (find index of value in array).

        Args:
            column: The intarray column name
            value: The value to find

        Returns:
            SQL idx function call

        Example:
            >>> format_intarray_idx('ids', 5)
            "idx(ids, 5)"
        """
        return f"idx({column}, {value})"

    def format_intarray_subarray(self, column: str, start: int, length: int = None) -> str:
        """Format subarray function.

        Args:
            column: The intarray column name
            start: Starting index (1-based)
            length: Number of elements

        Returns:
            SQL subarray function call

        Example:
            >>> format_intarray_subarray('ids', 1, 5)
            "subarray(ids, 1, 5)"
        """
        if length is not None:
            return f"subarray({column}, {start}, {length})"
        return f"subarray({column}, {start})"

    def format_intarray_uniq(self, column: str) -> str:
        """Format uniq function (remove duplicates).

        Args:
            column: The intarray column name

        Returns:
            SQL uniq function call

        Example:
            >>> format_intarray_uniq('ids')
            "uniq(ids)"
        """
        return f"uniq({column})"

    def format_intarray_sort(self, column: str, ascending: bool = True) -> str:
        """Format sort function.

        Args:
            column: The intarray column name
            ascending: Sort ascending (default) or descending

        Returns:
            SQL sort function call

        Example:
            >>> format_intarray_sort('ids')
            "sort(ids)"
            >>> format_intarray_sort('ids', ascending=False)
            "sort(ids, 'desc')"
        """
        if ascending:
            return f"sort({column})"
        return f"sort({column}, 'desc')"
