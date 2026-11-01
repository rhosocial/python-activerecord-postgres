# src/rhosocial/activerecord/backend/impl/postgres/async_support.py
"""Running psycopg's async mode on Windows, and one way to run it everywhere.

psycopg waits on the libpq socket with ``loop.add_reader`` /
``loop.add_writer`` (see ``psycopg/waiting.py``), and those two methods are
the selector-based event loop's -- on Windows the platform default since
3.8 is the ProactorEventLoop, which does not implement them. So every
"await psycopg async under the default loop" path fails there, including
this backend's own CLI in its ``--async`` modes.

psycopg 3.3+ detects the mismatch at connect time and raises with the fix
spelled out; older versions surface asyncio's raw ``NotImplementedError``.
This module applies the fix once, for the CLI and for callers who would
rather not memorize it: construct the loop psycopg's async needs, and run
coroutines on it.

Why the loop is constructed rather than asked of the policy: the only
supported way to obtain one on Windows is to build it, and
``asyncio.run(..., loop_factory=...)`` is 3.13+, while this backend still
supports 3.8. One explicit construction serves every supported version.
Why no policy switch: swapping the platform policy is a global side effect,
it would take asyncio subprocess and pipe I/O down with it, and the policy
class is deprecated for removal in 3.16 besides.

Not needed by the sync path at all, and not needed by aiosqlite either --
that driver blocks in a thread rather than registering the socket with the
loop.
"""

import asyncio
import selectors
from typing import Awaitable, TypeVar

__all__ = ["run_coroutine", "selector_event_loop"]

_T = TypeVar("_T")


def selector_event_loop() -> asyncio.AbstractEventLoop:
    """The event loop psycopg's async mode requires.

    Constructed on every platform: on Windows this is the fix itself (the
    ProactorEventLoop has no ``add_reader``/``add_writer``), elsewhere it is
    the same family of loop the platform already defaults to, so one code
    path behaves identically everywhere.

    Returns:
        A ``SelectorEventLoop`` bound to a ``SelectSelector``. What matters
        about it is not its class name but the two methods psycopg calls on
        it -- ``add_reader`` and ``add_writer`` -- which this loop has.
    """
    return asyncio.SelectorEventLoop(selectors.SelectSelector())


def run_coroutine(coro: Awaitable[_T]) -> _T:
    """Run *coro* to completion on the loop psycopg's async mode requires.

    The lifecycle mirrors ``asyncio.run``: the loop is installed as the
    current one for the duration, a normal or exceptional return still
    shuts async generators down and closes the loop, and ``KeyboardInterrupt``
    cancels what is still pending before the loop goes away, so a Ctrl-C in
    a CLI leaves no task attached to a loop that no longer exists.

    May only be called when no loop is running -- exactly the precondition
    ``asyncio.run`` states, and enforced the same way (a ``RuntimeError``
    from the loop underneath).

    Args:
        coro: The coroutine (or awaitable) to run to completion.

    Returns:
        Whatever *coro* returns.

    Raises:
        RuntimeError: If called while an event loop is already running.
        KeyboardInterrupt: Propagated after the pending tasks are cancelled.
    """
    loop = selector_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    except KeyboardInterrupt:
        _cancel_pending(loop)
        raise
    finally:
        _shutdown(loop)


def _cancel_pending(loop: asyncio.AbstractEventLoop) -> None:
    """Cancel what the interrupted run left pending, and drain it.

    The same shape as asyncio's own interrupt handling: cancellations are
    delivered and awaited once, so nothing outlives the loop.
    """
    try:
        pending = asyncio.all_tasks(loop)
    except RuntimeError:  # no running loop: nothing is pending
        return
    for task in pending:
        task.cancel()
    if pending:
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))


def _shutdown(loop: asyncio.AbstractEventLoop) -> None:
    """Close *loop* and detach it as the current one, swallowing shutdown noise.

    An async generator that fails to shut down must not replace the
    coroutine's own outcome; asyncio.run makes the same trade.
    """
    try:
        loop.run_until_complete(loop.shutdown_asyncgens())
    except BaseException:  # noqa: BLE001 - shutdown must not mask the result
        pass
    asyncio.set_event_loop(None)
    loop.close()
