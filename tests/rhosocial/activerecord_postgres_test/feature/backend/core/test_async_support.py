# tests/rhosocial/activerecord_postgres_test/feature/backend/test_async_support.py
"""The loop psycopg's async mode needs, and the runner that provides it.

No database is involved: what is pinned here is the contract the driver
depends on. psycopg waits on the libpq socket through
``loop.add_reader``/``loop.add_writer`` (psycopg/waiting.py), and on Windows
the platform default loop does not implement them -- which is why the
helper builds the loop explicitly instead of asking the policy for one.

The capability assertions matter more than the class-name ones: if CPython
ever ships a different loop that keeps the methods, these tests should stay
green, and if a loop loses them, these tests are what says so first.
"""

import asyncio

import pytest

from rhosocial.activerecord.backend.impl.postgres.async_support import (
    run_coroutine,
    selector_event_loop,
)


class TestTheLoopTheDriverNeeds:
    def test_the_loop_implements_what_psycopg_calls_on_it(self):
        loop = selector_event_loop()
        try:
            assert callable(loop.add_reader)
            assert callable(loop.add_writer)
        finally:
            loop.close()

    def test_the_loop_is_selector_based_where_that_is_the_fix(self):
        """On Windows the selector loop is the fix itself; elsewhere it is
        the same family the platform already defaults to, so one code path
        behaves identically everywhere."""
        loop = selector_event_loop()
        try:
            assert isinstance(loop, asyncio.SelectorEventLoop)
            assert type(loop).__module__.startswith("asyncio")
        finally:
            loop.close()


class TestRunCoroutine:
    def test_runs_the_coroutine_and_returns_its_value(self):
        async def answer():
            await asyncio.sleep(0)
            return 42

        assert run_coroutine(answer()) == 42

    def test_propagates_the_coroutine_s_exception(self):
        async def boom():
            await asyncio.sleep(0)
            raise ValueError("from the coroutine")

        with pytest.raises(ValueError, match="from the coroutine"):
            run_coroutine(boom())

    def test_the_loop_the_coroutine_ran_on_is_closed_afterwards(self):
        """Nothing is left running: the loop the coroutine used is closed by
        the time run_coroutine returns."""
        captured = {}

        async def grab():
            captured["loop"] = asyncio.get_running_loop()
            return "done"

        assert run_coroutine(grab()) == "done"
        assert captured["loop"].is_closed()

    def test_refuses_to_run_inside_a_running_loop(self):
        """Same precondition asyncio.run states: a runner is for the top of
        an async stack, not for nesting one loop inside another."""

        async def nested():
            coro = _trivial()
            try:
                return run_coroutine(coro)
            finally:
                coro.close()

        async def _trivial():
            return 1

        with pytest.raises(RuntimeError):
            asyncio.run(nested())

    def test_interrupt_cancels_what_is_pending_before_the_loop_closes(self):
        """Ctrl-C in a CLI must not leave tasks on a loop that then goes
        away: the pending ones are cancelled and drained.

        The interrupt is raised from a scheduled callback, which is how a
        real one arrives -- it propagates out of the running loop rather
        than surfacing as an ordinary exception inside the coroutine.
        """
        cancelled = []

        def interrupt():
            raise KeyboardInterrupt

        async def long_lived():
            loop = asyncio.get_running_loop()
            loop.call_soon(interrupt)
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.append("cancelled")
                raise

        with pytest.raises(KeyboardInterrupt):
            run_coroutine(long_lived())
        assert cancelled == ["cancelled"]
