# src/rhosocial/activerecord/backend/impl/postgres/types/xml.py
"""
PostgreSQL XML type representation.

This module provides PostgresXML for representing PostgreSQL
XML values in Python.

The XML type stores well-formed XML documents.
PostgreSQL validates XML content on input and provides XML
functions and XPath support.

Available since early PostgreSQL versions.

For type adapter (conversion between Python and database),
see adapters.xml.PostgresXMLAdapter.
"""

from dataclasses import dataclass


@dataclass
class PostgresXML:
    """PostgreSQL XML type representation.

    The XML type stores well-formed XML documents.
    PostgreSQL validates XML content on input.

    Attributes:
        content: XML content as string

    Examples:
        PostgresXML('<root><item>value</item></root>')
    """

    content: str

    def __post_init__(self):
        """Validate that content is not empty."""
        if not self.content:
            raise ValueError("XML content cannot be empty")

    def __str__(self) -> str:
        """Return XML content."""
        return self.content

    def __repr__(self) -> str:
        return f"PostgresXML({self.content!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PostgresXML):
            return self.content == other.content
        if isinstance(other, str):
            return self.content == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.content)

    def is_well_formed(self) -> bool:
        """Check if XML is well-formed (basic check).

        Note: This is a basic heuristic check. PostgreSQL
        does full validation on insert.

        Returns:
            True if appears to be well-formed
        """
        # Basic check for matching tags
        content = self.content.strip()
        if not content.startswith("<") or not content.endswith(">"):
            return False
        return True


__all__ = ["PostgresXML"]
