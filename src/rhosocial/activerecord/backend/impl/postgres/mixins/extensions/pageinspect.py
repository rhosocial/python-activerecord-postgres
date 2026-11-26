# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pageinspect.py
"""Capability mixin for the pageinspect extension."""

class PostgresPageinspectMixin:
    """Low-level inspection of database pages."""

    def supports_pageinspect(self) -> bool:
        """Whether pageinspect capabilities are available."""
        return self.check_extension_feature("pageinspect", "page")
