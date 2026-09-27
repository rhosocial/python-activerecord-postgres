# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_materialized_view_docs.py
"""Documentation and example consistency for materialized views.

Docs that drift from the implementation are a defect: the previous MV
documentation promised ``storage_options`` support that the MRO bug silently
disabled. These tests keep the docs, the example and the capability surface
aligned with the code.
"""

from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[6]
MV_DOC_PAGES = [
    PROJECT_ROOT / "docs" / "en_US" / "ddl" / "materialized_view.md",
    PROJECT_ROOT / "docs" / "zh_CN" / "ddl" / "materialized_view.md",
]
DDL_INDEX_PAGES = [
    PROJECT_ROOT / "docs" / "en_US" / "ddl" / "README.md",
    PROJECT_ROOT / "docs" / "zh_CN" / "ddl" / "README.md",
]
PROTOCOL_DOC_PAGES = [
    PROJECT_ROOT / "docs" / "en_US" / "postgres_specific_features" / "protocol_support.md",
    PROJECT_ROOT / "docs" / "zh_CN" / "postgres_specific_features" / "protocol_support.md",
]
EXAMPLE = (
    PROJECT_ROOT
    / "src" / "rhosocial" / "activerecord" / "backend" / "impl" / "postgres"
    / "examples" / "ddl" / "materialized_view.py"
)
EXAMPLES_META = (
    PROJECT_ROOT
    / "src" / "rhosocial" / "activerecord" / "backend" / "impl" / "postgres"
    / "examples" / "conftest.py"
)


class TestMaterializedViewDocs:
    """T-54: a dedicated page exists and documents every supported feature."""

    @pytest.mark.parametrize("page", MV_DOC_PAGES, ids=lambda p: p.parent.parent.name)
    def test_page_exists(self, page):
        assert page.is_file(), f"missing documentation page: {page}"

    @pytest.mark.parametrize("page", MV_DOC_PAGES, ids=lambda p: p.parent.parent.name)
    def test_page_documents_supported_features(self, page):
        text = page.read_text(encoding="utf-8")
        for token in (
            "PostgresCreateMaterializedViewExpression",
            "PostgresRefreshMaterializedViewExpression",
            "PostgresAlterMaterializedViewExpression",
            "PostgresDropMaterializedViewExpression",
            "if_not_exists",
            "concurrently",
            "TABLESPACE",
            "storage",
            "PostgresStorageParameter",
            "list_materialized_views",
            "is_populated",
            "has_unique_index",
        ):
            assert token in text, f"{page.name} does not mention {token}"

    @pytest.mark.parametrize(
        "page,marker",
        [
            (MV_DOC_PAGES[0], "cannot be relocated"),
            (MV_DOC_PAGES[1], "无法迁移到其他表空间"),
        ],
        ids=["en_US", "zh_CN"],
    )
    def test_page_documents_alter_boundary(self, page, marker):
        """SET TABLESPACE must be documented as unsupported for MVs.

        The code-side boundary is locked by
        ``TestAlterMaterializedView::test_set_tablespace_not_offered``.
        """
        text = page.read_text(encoding="utf-8")
        assert "SET TABLESPACE" in text
        assert marker in text

    @pytest.mark.parametrize("page", MV_DOC_PAGES, ids=lambda p: p.parent.parent.name)
    def test_page_documents_version_requirements(self, page):
        text = page.read_text(encoding="utf-8")
        assert "9.3" in text
        assert "9.4" in text

    @pytest.mark.parametrize("page", DDL_INDEX_PAGES, ids=lambda p: p.parent.parent.name)
    def test_ddl_index_lists_mv_expressions(self, page):
        text = page.read_text(encoding="utf-8")
        assert "PostgresCreateMaterializedViewExpression" in text
        assert "PostgresAlterMaterializedViewExpression" in text
        assert "materialized_view.md" in text

    @pytest.mark.parametrize("page", PROTOCOL_DOC_PAGES, ids=lambda p: p.parent.parent.name)
    def test_protocol_doc_lists_mv_support(self, page):
        text = page.read_text(encoding="utf-8")
        assert "PostgresMaterializedViewSupport" in text
        assert "materialized_view.md" in text


class TestMaterializedViewExample:
    """T-55 / T-56: the runnable example exercises the documented features."""

    def test_example_exists(self):
        assert EXAMPLE.is_file()

    def test_example_exercises_new_features(self):
        text = EXAMPLE.read_text(encoding="utf-8")
        for token in (
            "PostgresCreateMaterializedViewExpression",
            "if_not_exists=True",
            "PostgresStorageParameter",
            "PostgresAlterMaterializedViewExpression",
            "PostgresRenameMaterializedViewAction",
            "PostgresSetMaterializedViewSchemaAction",
            "PostgresSetMaterializedViewPropertiesAction",
            "PostgresResetMaterializedViewPropertiesAction",
            "PostgresChangeMaterializedViewOwnerAction",
            "PostgresDropMaterializedViewExpression",
            "list_materialized_views",
        ):
            assert token in text, f"example does not exercise {token}"

    def test_example_documents_sections(self):
        text = EXAMPLE.read_text(encoding="utf-8")
        assert "IF NOT EXISTS" in text
        assert "ALTER MATERIALIZED VIEW" in text
        assert "Introspection" in text

    def test_example_registered_in_metadata(self):
        assert EXAMPLE.is_file()
        assert "'ddl/materialized_view.py'" in EXAMPLES_META.read_text(encoding="utf-8")
