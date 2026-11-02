"""Bounded admission control with per-backend FIFO waiting.

The controller is the single owner of execution capacity.  It is intentionally
small and run-loop local: every state transition happens synchronously inside
one event-loop call, so no lock is held while network I/O is performed (the
controller never performs I/O at all).

Lifecycle:

    ARRIVED -> ADMITTED -> FINISHED
        |          |
        |          +-> FAILED      (upstream error)
        |          +-> CANCELLED   (stream client disconnect)
        +-> QUEUED -> ADMITTED
              |
              +-> REJECTED         (queue full)
              +-> TIMED_OUT        (queue wait exceeded)
              +-> CANCELLED        (waiter cancelled / client gone)

Invariants, checked by tests and exposed through ``stats()``:

* ``active_total <= max_in_flight``
* ``queued_total <= max_queue_size``
* ``active_by_backend[b] <= per_backend_limit[b]``

FIFO semantics are per target backend.  A request is routed *before* it is
queued, so a saturated backend cannot block a request for another backend.
When global capacity frees up, the oldest eligible waiter wins; if the oldest
waiter's backend is still saturated, the next eligible waiter is considered.
This avoids head-of-line blocking across backends while preserving FIFO within
each backend queue.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Deque, Mapping, Sequence

from .config import AdmissionConfig


class AdmissionState(str, Enum):
    ARRIVED = "arrived"
    QUEUED = "queued"
    ADMITTED = "admitted"
    FINISHED = "finished"
    REJECTED = "rejected"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    FAILED = "failed"


class AdmissionError(Exception):
    def __init__(self, backend_id: str, message: str) -> None:
        super().__init__(message)
        self.backend_id = backend_id


class AdmissionRejected(AdmissionError):
    """The bounded queue is full; the request was never queued."""


class AdmissionTimedOut(AdmissionError):
    """The request waited longer than ``queue_timeout_ms``."""


class AdmissionDeadlineExceeded(AdmissionError):
    """The whole-request deadline expired before the ticket was admitted."""


class ClientDisconnected(AdmissionError):
    """The client went away while its request was waiting for capacity."""


@dataclass(eq=False)
class AdmissionTicket:
    ticket_id: int
    backend_id: str
    state: AdmissionState = AdmissionState.ARRIVED
    arrived_at: float = field(default_factory=time.perf_counter)
    enqueued_at: float | None = None
    admitted_at: float | None = None
    finished_at: float | None = None
    future: asyncio.Future | None = None

    @property
    def queue_wait_s(self) -> float | None:
        if self.admitted_at is None:
            return None
        return max(0.0, self.admitted_at - self.arrived_at)

    @property
    def execution_s(self) -> float | None:
        if self.admitted_at is None or self.finished_at is None:
            return None
        return max(0.0, self.finished_at - self.admitted_at)


@dataclass(frozen=True)
class AdmissionStats:
    enabled: bool
    max_in_flight: int
    max_queue_size: int
    active_total: int
    queued_total: int
    active_by_backend: Mapping[str, int]
    queued_by_backend: Mapping[str, int]
    peak_active: int
    peak_queued: int


class AdmissionController:
    def __init__(
        self,
        config: AdmissionConfig,
        backend_ids: Sequence[str],
        on_change: Callable[[], None] | None = None,
    ) -> None:
        if not backend_ids:
            raise ValueError("admission controller needs at least one backend")
        self.config = config
        self._backend_ids = tuple(backend_ids)
        self._limits = {bid: config.limit_for(bid) for bid in self._backend_ids}
        self._active: dict[str, int] = {bid: 0 for bid in self._backend_ids}
        self._queues: dict[str, Deque[AdmissionTicket]] = {
            bid: deque() for bid in self._backend_ids
        }
        self._active_total = 0
        self._queued_total = 0
        self._next_ticket_id = 0
        self._peak_active = 0
        self._peak_queued = 0
        self._on_change = on_change

    def set_on_change(self, callback: Callable[[], None] | None) -> None:
        self._on_change = callback

    # ------------------------------------------------------------------
    # acquisition
    # ------------------------------------------------------------------
    def try_acquire(self, backend_id: str) -> AdmissionTicket | None:
        """Admit immediately if possible, otherwise return None.

        This is the fast path used by the proxy for non-contended requests; it
        never queues and never raises.  A None result means the caller must use
        :meth:`acquire` (or :func:`acquire_with_disconnect`).
        """
        self._check_backend(backend_id)
        ticket = self._new_ticket(backend_id)
        if not self.config.enabled or self._has_capacity(backend_id):
            self._admit(ticket)
            return ticket
        return None

    async def acquire(
        self, backend_id: str, deadline_at: float | None = None
    ) -> AdmissionTicket:
        """Admit or queue the request, honouring queue bound, timeout, deadline.

        ``deadline_at`` is a monotonic absolute timestamp for the *whole*
        request.  Queue wait is bounded by the smaller of ``queue_timeout_ms``
        and the remaining total budget; the latter is reported as a request
        deadline, not a queue timeout.
        """
        self._check_backend(backend_id)
        ticket = self._new_ticket(backend_id)
        now = time.perf_counter()
        if deadline_at is not None and now >= deadline_at:
            ticket.state = AdmissionState.TIMED_OUT
            raise AdmissionDeadlineExceeded(
                backend_id, "request deadline expired before admission"
            )
        if not self.config.enabled or self._has_capacity(backend_id):
            self._admit(ticket)
            return ticket

        if self._queued_total >= self.config.max_queue_size:
            ticket.state = AdmissionState.REJECTED
            self._notify()
            raise AdmissionRejected(backend_id, "admission queue is full")

        wait_s = self.config.queue_timeout_ms / 1000.0
        if deadline_at is not None:
            remaining = deadline_at - now
            if remaining < wait_s:
                wait_s = remaining

        ticket.state = AdmissionState.QUEUED
        ticket.enqueued_at = time.perf_counter()
        self._queues[backend_id].append(ticket)
        self._queued_total += 1
        self._peak_queued = max(self._peak_queued, self._queued_total)
        ticket.future = asyncio.get_running_loop().create_future()
        self._notify()

        try:
            if wait_s <= 0:
                raise asyncio.TimeoutError
            async with asyncio.timeout(wait_s):
                await asyncio.shield(ticket.future)
        except asyncio.TimeoutError:
            if self._remove_queued(ticket, AdmissionState.TIMED_OUT):
                if deadline_at is not None and time.perf_counter() >= deadline_at:
                    raise AdmissionDeadlineExceeded(
                        backend_id, "request deadline expired while queued"
                    ) from None
                raise AdmissionTimedOut(
                    backend_id, "admission queue wait timed out"
                ) from None
            # The ticket was admitted concurrently with the timeout; treat it
            # as admitted and let the caller release it normally.
        except asyncio.CancelledError:
            if self._remove_queued(ticket, AdmissionState.CANCELLED):
                raise
            # Admitted concurrently: the caller will not receive the ticket,
            # so capacity must be returned here before propagating.
            if ticket.state is AdmissionState.ADMITTED:
                self.release(ticket, AdmissionState.CANCELLED)
            raise

        return ticket

    def release(
        self,
        ticket: AdmissionTicket,
        state: AdmissionState = AdmissionState.FINISHED,
    ) -> bool:
        """Return capacity exactly once.  Idempotent for finished tickets."""
        if ticket.state is not AdmissionState.ADMITTED:
            return False
        if state not in (
            AdmissionState.FINISHED,
            AdmissionState.FAILED,
            AdmissionState.CANCELLED,
            AdmissionState.TIMED_OUT,
        ):
            raise ValueError(f"invalid release state: {state}")
        ticket.state = state
        ticket.finished_at = time.perf_counter()
        self._active_total -= 1
        self._active[ticket.backend_id] -= 1
        if self._active_total < 0 or self._active[ticket.backend_id] < 0:
            raise RuntimeError("admission accounting underflow")
        self._drain()
        self._notify()
        return True

    # ------------------------------------------------------------------
    # internals (synchronous; no I/O, no awaits)
    # ------------------------------------------------------------------
    def _new_ticket(self, backend_id: str) -> AdmissionTicket:
        ticket = AdmissionTicket(self._next_ticket_id, backend_id)
        self._next_ticket_id += 1
        return ticket

    def _check_backend(self, backend_id: str) -> None:
        if backend_id not in self._queues:
            raise KeyError(f"unknown backend: {backend_id}")

    def _has_capacity(self, backend_id: str) -> bool:
        return (
            self._active_total < self.config.max_in_flight
            and self._active[backend_id] < self._limits[backend_id]
        )

    def _admit(self, ticket: AdmissionTicket) -> None:
        ticket.state = AdmissionState.ADMITTED
        ticket.admitted_at = time.perf_counter()
        self._active_total += 1
        self._active[ticket.backend_id] += 1
        self._peak_active = max(self._peak_active, self._active_total)
        if ticket.future is not None and not ticket.future.done():
            ticket.future.set_result(None)
        self._notify()

    def _remove_queued(
        self, ticket: AdmissionTicket, state: AdmissionState
    ) -> bool:
        if ticket.state is not AdmissionState.QUEUED:
            return False
        try:
            self._queues[ticket.backend_id].remove(ticket)
        except ValueError:
            return False
        ticket.state = state
        self._queued_total -= 1
        if self._queued_total < 0:
            raise RuntimeError("admission queue accounting underflow")
        self._notify()
        return True

    def _drain(self) -> None:
        # Admit oldest eligible waiters until global capacity is exhausted.
        # Eligibility includes the per-backend limit, so one saturated backend
        # cannot consume slots that another backend could use.
        while self._active_total < self.config.max_in_flight:
            candidate: AdmissionTicket | None = None
            for backend_id, queue in self._queues.items():
                if not queue:
                    continue
                if self._active[backend_id] >= self._limits[backend_id]:
                    continue
                head = queue[0]
                if candidate is None or head.ticket_id < candidate.ticket_id:
                    candidate = head
            if candidate is None:
                return
            queue = self._queues[candidate.backend_id]
            queue.popleft()
            self._queued_total -= 1
            if self._queued_total < 0:
                raise RuntimeError("admission queue accounting underflow")
            self._admit(candidate)

    def _notify(self) -> None:
        if self._on_change is not None:
            self._on_change()

    # ------------------------------------------------------------------
    # introspection
    # ------------------------------------------------------------------
    def stats(self) -> AdmissionStats:
        return AdmissionStats(
            enabled=self.config.enabled,
            max_in_flight=self.config.max_in_flight,
            max_queue_size=self.config.max_queue_size,
            active_total=self._active_total,
            queued_total=self._queued_total,
            active_by_backend=dict(self._active),
            queued_by_backend={
                backend_id: len(queue) for backend_id, queue in self._queues.items()
            },
            peak_active=self._peak_active,
            peak_queued=self._peak_queued,
        )

    @property
    def backend_ids(self) -> tuple[str, ...]:
        return self._backend_ids


async def acquire_with_disconnect(
    controller: AdmissionController,
    request,
    backend_id: str,
    poll_interval_s: float = 0.05,
    deadline_at: float | None = None,
) -> AdmissionTicket:
    """Acquire capacity while watching for a client disconnect.

    ASGI does not cancel a handler when the client goes away, so a queued
    request would otherwise occupy a queue slot until timeout.  The watcher
    polls ``Request.is_disconnected`` only while the request is waiting; once
    admitted, the normal proxy lifecycle takes over.
    """
    task = asyncio.create_task(
        controller.acquire(backend_id, deadline_at=deadline_at),
        name=f"admission-{backend_id}",
    )
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=poll_interval_s)
            if task in done:
                return task.result()
            try:
                disconnected = await request.is_disconnected()
            except Exception:
                disconnected = True
            if disconnected:
                await _discard_task(task, controller)
                raise ClientDisconnected(
                    backend_id, "client disconnected while waiting for capacity"
                )
    except asyncio.CancelledError:
        await _discard_task(task, controller)
        raise


async def _discard_task(task: asyncio.Task, controller: AdmissionController) -> None:
    """Cancel an admission task and return capacity if it was already admitted."""
    task.cancel()
    try:
        ticket = await task
    except (asyncio.CancelledError, AdmissionError):
        return
    except Exception:
        return
    controller.release(ticket, AdmissionState.CANCELLED)
