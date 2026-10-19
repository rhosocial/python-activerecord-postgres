# tests/rhosocial/activerecord_postgres_test/feature/backend/expression/conftest.py
"""The expression tests are parallel by configuration (expression-dialect §12).

An expression test builds a tree, renders it through its own dialect and
asserts the text; it starts no connection, creates no schema and reads no
file. Nothing one case does can therefore be observed by another, which is
what makes the cases independent enough to run at the same time -- so the
only distribution mode this directory uses is ``loadgroup``: every case is
scheduled freely, and the one thing that serialises anything is an explicit
``xdist_group``. A case that genuinely has to run alone names its group and
says why beside it; the directory is never serialised to accommodate one.

The distribution mode is pinned here rather than left to each caller's
flags (``--dist=loadscope`` / ``loadfile`` would bundle independent cases
by module, which is exactly what §12 forbids), and the suite's CI leg
already runs the tree with ``-n 8 --dist=loadgroup``.

The worker *count* is deliberately not defaulted here. Measured on this
machine (16 cores): spawning workers automatically -- ``-n auto``, the
obvious default -- kills the session, because every worker re-imports this
backend's whole fixture world at bootstrap and the box cannot stand up
sixteen of them at once (``INTERNALERROR ... EOFError: expected 1 bytes,
got 0`` from execnet's gateway bootstrap). ``-n 2 --dist=loadgroup`` runs
the directory green. So parallelism is the configuration, and the number
of workers stays the caller's choice.
"""


def pytest_configure(config):
    """Pin the distribution mode whenever workers are active."""
    if config.getoption("numprocesses", None) and config.getoption("dist", "no") == "no":
        config.option.dist = "loadgroup"
