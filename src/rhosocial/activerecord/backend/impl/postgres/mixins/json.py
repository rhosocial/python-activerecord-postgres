# src/rhosocial/activerecord/backend/impl/postgres/mixins/json.py
"""PostgreSQL JSON feature support mixin."""


class PostgresJSONMixin:
    """PostgreSQL JSON feature support."""

    def supports_json_arrow_operators(self) -> bool:
        """Whether the ``->`` and ``->>`` operators can be spelled. 9.3+.

        Both operators arrived in 9.3 for the ``json`` type (``jsonb`` came
        with the type itself in 9.4, and the operators with it). What is being
        asked here is whether the server can parse an arrow at all, so 9.3 is
        the gate: on 9.2 a JSON value exists and can only be navigated with
        the functions ``json_extract_path`` / ``json_extract_path_text``, and
        an arrow the core formatter renders would be a syntax error.

        The probe previously returned True unconditionally, so on 9.2 and
        older -- where the type exists and the operators do not -- it
        advertised SQL the parser rejects. It is asked through
        :meth:`format_json_expression` (ARROW mode) and by callers deciding
        whether to request that mode, so the wrong answer was ready to reach
        SQL.

        An unadapted dialect raises from ``self.version``
        (``DialectNotAdaptedException``), as every other JSON probe in this
        backend does: the question is about the server version, and a dialect
        that has not been told one has no answer to give.
        """
        return self.version >= (9, 3, 0)
