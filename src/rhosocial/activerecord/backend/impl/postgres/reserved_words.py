# src/rhosocial/activerecord/backend/impl/postgresql/reserved_words.py
"""
PostgreSQL reserved words list.

Source: PostgreSQL 16 Documentation (https://www.postgresql.org/docs/16/sql-keywords-appendix.html)
"""

POSTGRESQL_RESERVED_WORDS = frozenset({
    "all", "analyse", "analyze", "and", "any", "array", "as", "asc",
    "asymmetric", "both", "case", "cast", "check", "collate", "column",
    "constraint", "create", "cross", "current_catalog", "current_date",
    "current_role", "current_schema", "current_time", "current_timestamp",
    "current_user", "default", "deferrable", "desc", "distinct", "do",
    "else", "end", "except", "false", "fetch", "for", "foreign", "from",
    "full", "grant", "group", "having", "ilike", "in", "initially",
    "inner", "intersect", "into", "is", "isnull", "join", "lateral",
    "leading", "left", "like", "limit", "localtime", "localtimestamp",
    "natural", "not", "notnull", "null", "offset", "on", "only", "or",
    "order", "outer", "overlaps", "placing", "primary", "references",
    "returning", "right", "select", "session_user", "similar", "some",
    "symmetric", "table", "then", "to", "trailing", "true", "union",
    "unique", "user", "using", "variadic", "verbose", "when", "where",
    "window", "with",
})
