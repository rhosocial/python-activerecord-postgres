"""pageinspect extension support protocol.

This module defines the protocol for pageinspect page-level inspection
functionality in PostgreSQL.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/pageinspect.html
"""

from typing import Optional, Protocol, Tuple, runtime_checkable


@runtime_checkable
class PostgresPageinspectSupport(Protocol):
    """pageinspect page-level data inspection protocol.

    Feature Source: Extension support (requires pageinspect extension)

    pageinspect provides functions to inspect database pages at a low level:
    - get_raw_page: Get raw page data
    - page_header: Show page header information
    - heap_page_items: Show heap tuple information
    - bt_page_items: Show B-tree index page items
    - bt_metap: Show B-tree meta page information

    Extension Information:
    - Extension name: pageinspect
    - Install command: CREATE EXTENSION pageinspect;
    - Minimum version: 1.0
    - Documentation: https://www.postgresql.org/docs/current/pageinspect.html
    """

    def supports_pageinspect_heap(self) -> bool:
        """Whether heap page inspection functions are available."""
        ...

    def supports_pageinspect_btree(self) -> bool:
        """Whether B-tree page inspection functions are available."""
        ...

    def supports_pageinspect_brin(self) -> bool:
        """Whether BRIN page inspection functions are available (PG 11+ / pageinspect 1.7+)."""
        ...

    def format_heap_page_statement(
        self, table_name: str, page_number: int, schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format statement to inspect heap page items."""
        ...

    def format_btree_metapage_statement(
        self, index_name: str, schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format statement to show B-tree meta page information."""
        ...