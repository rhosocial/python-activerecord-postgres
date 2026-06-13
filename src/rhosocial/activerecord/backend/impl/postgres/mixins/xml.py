# src/rhosocial/activerecord/backend/impl/postgres/mixins/xml.py
"""PostgreSQL xml feature support implementation."""


class PostgresXMLMixin:
    """PostgreSQL xml override implementation.

    All features are native, using version number for detection.
    """

    def supports_xmlparse(self) -> bool:
        return self.version >= (8, 3, 0)

    def supports_xmlserialize(self) -> bool:
        return self.version >= (8, 3, 0)

    def supports_xmlelement(self) -> bool:
        return self.version >= (8, 3, 0)

    def supports_xmlattributes(self) -> bool:
        return self.version >= (8, 3, 0)

    def supports_xmlforest(self) -> bool:
        return self.version >= (8, 3, 0)

    def supports_xmlconcat(self) -> bool:
        return self.version >= (8, 3, 0)

    def supports_xmlcomment(self) -> bool:
        return self.version >= (8, 3, 0)

    def supports_xmlpi(self) -> bool:
        return self.version >= (8, 3, 0)

    def supports_xmlroot(self) -> bool:
        return self.version >= (8, 3, 0)

    def supports_xmlagg(self) -> bool:
        return self.version >= (8, 3, 0)

    def supports_xmlquery(self) -> bool:
        return False

    def supports_xmlexists(self) -> bool:
        return self.version >= (8, 4, 0)

    def supports_xmltable(self) -> bool:
        return self.version >= (10, 0, 0)
