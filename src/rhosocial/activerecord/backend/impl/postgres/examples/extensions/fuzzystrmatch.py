# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/fuzzystrmatch.py
"""
fuzzystrmatch extension - fuzzy string matching.

This example demonstrates:
1. Check if fuzzystrmatch extension is available
2. Create extension using CreateExtensionExpression
3. Use fuzzystrmatch functions with expression-based API:
   - levenshtein(): Calculate Levenshtein distance
   - soundex(): Soundex encoding
   - difference(): Similarity of Soundex codes (0-4)
   - metaphone(): Metaphone encoding
   - dmetaphone(): Double Metaphone encoding
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import (
    PostgresConnectionConfig,
)

config = PostgresConnectionConfig(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    database=os.getenv("PG_DATABASE", "test"),
    username=os.getenv("PG_USERNAME", "postgres"),
    password=os.getenv("PG_PASSWORD", ""),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
backend.introspect_and_adapt()
dialect = backend.dialect

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.impl.postgres.expression import (
    CreateExtensionExpression,
)
from rhosocial.activerecord.backend.expression import (
    QueryExpression,
)
from rhosocial.activerecord.backend.expression.core import FunctionCall, Literal
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

opts = ExecutionOptions(stmt_type=StatementType.DQL)

# Check if fuzzystrmatch extension is available
available = dialect.is_extension_available("fuzzystrmatch")
installed = dialect.is_extension_installed("fuzzystrmatch")
print(f"Extension: fuzzystrmatch available = {available}, installed = {installed}")

# Create extension using expression
if available and not installed:
    create_ext = CreateExtensionExpression(
        dialect=dialect,
        name="fuzzystrmatch",
    )
    sql, params = create_ext.to_sql()
    print(f"\n--- CREATE EXTENSION ---")
    print(f"SQL: {sql}")
    backend.execute(sql, params)
    # Re-detect extensions after creation
    backend.introspect_and_adapt()

# Re-check after creation
installed = dialect.is_extension_installed("fuzzystrmatch")

if installed:
    # Example 1: Levenshtein distance
    # levenshtein(s1, s2) returns the minimum number of single-character
    # edits needed to change s1 into s2
    lev_query = QueryExpression(
        dialect=dialect,
        select=[
            FunctionCall(
                dialect, "levenshtein",
                Literal(dialect, "hello"),
                Literal(dialect, "hallo"),
            ).as_("distance"),
        ],
    )
    sql, params = lev_query.to_sql()
    print(f"\n--- Levenshtein distance ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    print(f"Levenshtein('hello', 'hallo') = {result.data}")

    # More Levenshtein examples
    lev_query2 = QueryExpression(
        dialect=dialect,
        select=[
            FunctionCall(
                dialect, "levenshtein",
                Literal(dialect, "kitten"),
                Literal(dialect, "sitting"),
            ).as_("kitten_to_sitting"),
            FunctionCall(
                dialect, "levenshtein",
                Literal(dialect, "sunday"),
                Literal(dialect, "saturday"),
            ).as_("sunday_to_saturday"),
        ],
    )
    sql, params = lev_query2.to_sql()
    print(f"\nSQL: {sql}")
    result = backend.execute(sql, params, options=opts)
    print(f"Levenshtein('kitten', 'sitting') = {result.data[0][0] if result.data else 'N/A'}")
    print(f"Levenshtein('sunday', 'saturday') = {result.data[0][1] if result.data else 'N/A'}")

    # Example 2: Soundex encoding
    # soundex(text) returns a Soundex code (4-character string)
    # Names with similar sounds have the same Soundex code
    soundex_query = QueryExpression(
        dialect=dialect,
        select=[
            FunctionCall(dialect, "soundex", Literal(dialect, "Robert")).as_("soundex_robert"),
            FunctionCall(dialect, "soundex", Literal(dialect, "Rupert")).as_("soundex_rupert"),
            FunctionCall(dialect, "soundex", Literal(dialect, "Ashcraft")).as_("soundex_ashcraft"),
            FunctionCall(dialect, "soundex", Literal(dialect, "Ashcroft")).as_("soundex_ashcroft"),
        ],
    )
    sql, params = soundex_query.to_sql()
    print(f"\n--- Soundex encoding ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    if result.data:
        print(f"Soundex('Robert')   = {result.data[0][0]}")
        print(f"Soundex('Rupert')   = {result.data[0][1]}")
        print(f"Soundex('Ashcraft') = {result.data[0][2]}")
        print(f"Soundex('Ashcroft') = {result.data[0][3]}")

    # Example 3: Difference function
    # difference(s1, s2) returns an integer (0-4) indicating how similar
    # the Soundex codes of two strings are (4 = identical codes)
    diff_query = QueryExpression(
        dialect=dialect,
        select=[
            FunctionCall(
                dialect, "difference",
                Literal(dialect, "hello"),
                Literal(dialect, "hallo"),
            ).as_("diff_hello_hallo"),
            FunctionCall(
                dialect, "difference",
                Literal(dialect, "Robert"),
                Literal(dialect, "Rupert"),
            ).as_("diff_robert_rupert"),
            FunctionCall(
                dialect, "difference",
                Literal(dialect, "hello"),
                Literal(dialect, "world"),
            ).as_("diff_hello_world"),
        ],
    )
    sql, params = diff_query.to_sql()
    print(f"\n--- Difference (Soundex similarity) ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    if result.data:
        print(f"Difference('hello', 'hallo')   = {result.data[0][0]} (0-4 scale)")
        print(f"Difference('Robert', 'Rupert')  = {result.data[0][1]} (0-4 scale)")
        print(f"Difference('hello', 'world')    = {result.data[0][2]} (0-4 scale)")

    # Example 4: Metaphone encoding
    # metaphone(text, max_output_length) returns a Metaphone code
    meta_query = QueryExpression(
        dialect=dialect,
        select=[
            FunctionCall(
                dialect, "metaphone",
                Literal(dialect, "Robert"),
                Literal(dialect, 4),
            ).as_("meta_robert"),
            FunctionCall(
                dialect, "metaphone",
                Literal(dialect, "Rupert"),
                Literal(dialect, 4),
            ).as_("meta_rupert"),
            FunctionCall(
                dialect, "metaphone",
                Literal(dialect, "telephone"),
                Literal(dialect, 6),
            ).as_("meta_telephone"),
        ],
    )
    sql, params = meta_query.to_sql()
    print(f"\n--- Metaphone encoding ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    if result.data:
        print(f"Metaphone('Robert', 4)    = {result.data[0][0]}")
        print(f"Metaphone('Rupert', 4)    = {result.data[0][1]}")
        print(f"Metaphone('telephone', 6) = {result.data[0][2]}")

    # Example 5: Double Metaphone encoding
    # dmetaphone(text) returns the primary Double Metaphone code
    # dmetaphone_alt(text) returns the alternate Double Metaphone code
    dmeta_query = QueryExpression(
        dialect=dialect,
        select=[
            FunctionCall(
                dialect, "dmetaphone",
                Literal(dialect, "Robert"),
            ).as_("dmeta_robert"),
            FunctionCall(
                dialect, "dmetaphone",
                Literal(dialect, "Smith"),
            ).as_("dmeta_smith"),
            FunctionCall(
                dialect, "dmetaphone",
                Literal(dialect, "Schmidt"),
            ).as_("dmeta_schmidt"),
        ],
    )
    sql, params = dmeta_query.to_sql()
    print(f"\n--- Double Metaphone encoding ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    if result.data:
        print(f"Dmetaphone('Robert')  = {result.data[0][0]}")
        print(f"Dmetaphone('Smith')   = {result.data[0][1]}")
        print(f"Dmetaphone('Schmidt') = {result.data[0][2]}")

    # Example 6: Double Metaphone alternate codes
    dmeta_alt_query = QueryExpression(
        dialect=dialect,
        select=[
            FunctionCall(
                dialect, "dmetaphone_alt",
                Literal(dialect, "Robert"),
            ).as_("dmeta_alt_robert"),
            FunctionCall(
                dialect, "dmetaphone_alt",
                Literal(dialect, "Smith"),
            ).as_("dmeta_alt_smith"),
            FunctionCall(
                dialect, "dmetaphone_alt",
                Literal(dialect, "Schmidt"),
            ).as_("dmeta_alt_schmidt"),
        ],
    )
    sql, params = dmeta_alt_query.to_sql()
    print(f"\n--- Double Metaphone alternate codes ---")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    result = backend.execute(sql, params, options=opts)
    if result.data:
        print(f"Dmetaphone_alt('Robert')  = {result.data[0][0]}")
        print(f"Dmetaphone_alt('Smith')   = {result.data[0][1]}")
        print(f"Dmetaphone_alt('Schmidt') = {result.data[0][2]}")

else:
    print("\nSkipping - fuzzystrmatch not available on this server")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
