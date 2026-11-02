import asyncio

import pytest

from gateway.admission import (
    AdmissionController,
    AdmissionDeadlineExceeded,
    AdmissionRejected,
    AdmissionState,
    AdmissionTimedOut,
    ClientDisconnected,
    acquire_with_disconnect,
)
from gateway.config import AdmissionConfig
import time


def make_controller(
    max_in_flight=1,
    max_queue_size=1,
    queue_timeout_ms=500,
    per_backend=(),
    enabled=True,
):
    config = AdmissionConfig(
        enabled=enabled,
        max_in_flight=max_in_flight,
        max_queue_size=max_queue_size,
        queue_timeout_ms=queue_timeout_ms,
        per_backend=per_backend,
    )
    return AdmissionController(config, ["a", "b"])


async def wait_until(predicate, timeout=2.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition was not reached before timeout")
        await asyncio.sleep(0)


async def test_immediate_admission_and_release():
    controller = make_controller(max_in_flight=2, max_queue_size=0)
    ticket = await controller.acquire("a")
    assert ticket.state is AdmissionState.ADMITTED
    assert ticket.queue_wait_s is not None
    assert controller.stats().active_total == 1
    assert controller.release(ticket) is True
    assert controller.stats().active_total == 0
    assert controller.release(ticket) is False


async def test_capacity_exhaustion_queues_then_release_admits():
    controller = make_controller(max_in_flight=1, max_queue_size=1)
    first = await controller.acquire("a")
    waiter = asyncio.create_task(controller.acquire("a"))
    await wait_until(lambda: controller.stats().queued_total == 1)
    assert controller.stats().active_total == 1
    assert not waiter.done()

    controller.release(first)
    second = await asyncio.wait_for(waiter, timeout=1)
    assert second.state is AdmissionState.ADMITTED
    assert second.queue_wait_s is not None and second.queue_wait_s >= 0
    assert controller.stats().active_total == 1
    controller.release(second)
    assert controller.stats().active_total == 0


async def test_queue_full_rejects_without_enqueueing():
    controller = make_controller(max_in_flight=1, max_queue_size=1)
    first = await controller.acquire("a")
    waiter = asyncio.create_task(controller.acquire("a"))
    await wait_until(lambda: controller.stats().queued_total == 1)

    with pytest.raises(AdmissionRejected):
        await controller.acquire("a")
    stats = controller.stats()
    assert stats.queued_total == 1
    assert stats.active_total == 1

    controller.release(first)
    second = await asyncio.wait_for(waiter, timeout=1)
    controller.release(second)


async def test_fifo_order_within_backend():
    controller = make_controller(max_in_flight=1, max_queue_size=3)
    first = await controller.acquire("a")
    order: list[str] = []

    async def worker(label: str):
        ticket = await controller.acquire("a")
        order.append(label)
        controller.release(ticket)

    tasks = [asyncio.create_task(worker(label)) for label in ("A", "B", "C")]
    await wait_until(lambda: controller.stats().queued_total == 3)
    controller.release(first)
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=2)
    assert order == ["A", "B", "C"]


async def test_cancelled_waiter_leaves_queue():
    controller = make_controller(max_in_flight=1, max_queue_size=1)
    first = await controller.acquire("a")
    waiter = asyncio.create_task(controller.acquire("a"))
    await wait_until(lambda: controller.stats().queued_total == 1)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    await wait_until(lambda: controller.stats().queued_total == 0)
    assert controller.stats().active_total == 1
    controller.release(first)


async def test_queue_timeout_does_not_consume_capacity():
    controller = make_controller(
        max_in_flight=1, max_queue_size=1, queue_timeout_ms=30
    )
    first = await controller.acquire("a")
    with pytest.raises(AdmissionTimedOut):
        await controller.acquire("a")
    stats = controller.stats()
    assert stats.queued_total == 0
    assert stats.active_total == 1
    controller.release(first)
    assert controller.stats().active_total == 0


async def test_request_deadline_shorter_than_queue_timeout():
    controller = make_controller(
        max_in_flight=1, max_queue_size=1, queue_timeout_ms=5000
    )
    first = await controller.acquire("a")
    deadline_at = time.perf_counter() + 0.03
    with pytest.raises(AdmissionDeadlineExceeded):
        await controller.acquire("a", deadline_at=deadline_at)
    stats = controller.stats()
    assert stats.queued_total == 0
    assert stats.active_total == 1
    controller.release(first)


async def test_expired_deadline_rejects_before_queueing():
    controller = make_controller(max_in_flight=1, max_queue_size=1)
    with pytest.raises(AdmissionDeadlineExceeded):
        await controller.acquire("a", deadline_at=time.perf_counter() - 0.01)
    stats = controller.stats()
    assert stats.active_total == 0
    assert stats.queued_total == 0


async def test_release_accepts_timed_out_state_once():
    controller = make_controller(max_in_flight=1, max_queue_size=1)
    ticket = await controller.acquire("a")
    assert controller.release(ticket, AdmissionState.TIMED_OUT) is True
    assert controller.release(ticket, AdmissionState.TIMED_OUT) is False
    assert controller.stats().active_total == 0


async def test_per_backend_limits_are_independent():
    controller = make_controller(
        max_in_flight=2,
        max_queue_size=2,
        per_backend=(("a", 1), ("b", 1)),
    )
    ticket_a = await controller.acquire("a")
    ticket_b = await controller.acquire("b")
    assert controller.stats().active_total == 2

    queued_a = asyncio.create_task(controller.acquire("a"))
    await wait_until(lambda: controller.stats().queued_total == 1)
    assert not queued_a.done()

    controller.release(ticket_a)
    second_a = await asyncio.wait_for(queued_a, timeout=1)
    assert controller.stats().active_by_backend["a"] == 1
    assert controller.stats().active_by_backend["b"] == 1
    controller.release(second_a)
    controller.release(ticket_b)


async def test_global_limit_can_queue_a_free_backend():
    controller = make_controller(max_in_flight=1, max_queue_size=1)
    first = await controller.acquire("a")
    queued_b = asyncio.create_task(controller.acquire("b"))
    await wait_until(lambda: controller.stats().queued_total == 1)
    assert not queued_b.done()
    controller.release(first)
    second = await asyncio.wait_for(queued_b, timeout=1)
    assert second.backend_id == "b"
    controller.release(second)


async def test_burst_never_exceeds_capacity_and_queue_bounds():
    controller = make_controller(max_in_flight=3, max_queue_size=5)
    gate = asyncio.Event()

    async def worker(index: int):
        try:
            ticket = await controller.acquire("a" if index % 2 else "b")
        except AdmissionRejected:
            return "rejected"
        await gate.wait()
        controller.release(ticket)
        return "admitted"

    tasks = [asyncio.create_task(worker(index)) for index in range(20)]
    await wait_until(
        lambda: controller.stats().active_total + controller.stats().queued_total == 8
    )
    stats = controller.stats()
    assert stats.active_total <= 3
    assert stats.queued_total <= 5
    assert stats.peak_active <= 3
    assert stats.peak_queued <= 5

    gate.set()
    results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=3)
    assert results.count("admitted") == 8
    assert results.count("rejected") == 12
    final = controller.stats()
    assert final.active_total == 0
    assert final.queued_total == 0


async def test_disabled_admission_never_queues():
    controller = make_controller(
        max_in_flight=1, max_queue_size=0, enabled=False
    )
    tickets = [await controller.acquire("a") for _ in range(5)]
    assert controller.stats().active_total == 5
    assert controller.stats().queued_total == 0
    for ticket in tickets:
        controller.release(ticket)
    assert controller.stats().active_total == 0


class _FakeRequest:
    def __init__(self) -> None:
        self.disconnected = asyncio.Event()

    async def is_disconnected(self) -> bool:
        return self.disconnected.is_set()


async def test_disconnect_while_queued_removes_ticket():
    controller = make_controller(max_in_flight=1, max_queue_size=1)
    first = await controller.acquire("a")
    request = _FakeRequest()
    waiter = asyncio.create_task(acquire_with_disconnect(controller, request, "a"))
    await wait_until(lambda: controller.stats().queued_total == 1)
    request.disconnected.set()
    with pytest.raises(ClientDisconnected):
        await asyncio.wait_for(waiter, timeout=2)
    await wait_until(lambda: controller.stats().queued_total == 0)
    assert controller.stats().active_total == 1
    controller.release(first)
