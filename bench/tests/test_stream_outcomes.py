"""Benchmark outcomes must preserve incomplete streams and transport failures."""
from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from bench.run_bench import run_request
from bench.run_reliability_bench import aggregate_reliability, stream_request
from bench.run_soak import WorkerStats, disconnect_worker, normal_worker, run_phase
from bench.workload import WorkloadRequest


class Stream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes], failure: Exception | None = None):
        self.chunks = chunks
        self.failure = failure

    async def __aiter__(self):
        await asyncio.sleep(0.005)
        for chunk in self.chunks:
            yield chunk
        if self.failure:
            raise self.failure


def client_for(chunks: list[bytes], *, failure=None, status=200):
    def handle(request):
        return httpx.Response(
            status, headers={"content-type": "text/event-stream", "x-gateway-backend": "a"},
            stream=Stream(chunks, failure),
        )
    return httpx.AsyncClient(base_url="http://gateway", transport=httpx.MockTransport(handle))


@pytest.mark.parametrize("tail,completed", [
    (b"", False),
    (b'data: {"stop":true}', False),
    (b'data: [DONE]\n\n', False),  # native /completion requires stop=true
    (b'data: {"stop":false}\n\n', False),
    (b'data: {"stop":true}\n\n', True),
])
async def test_native_stream_requires_framed_terminal_event(tail, completed):
    async with client_for([b'data: {"content":"hello"}\n\n', tail]) as client:
        result = await stream_request(client, "prompt")
    assert result.status == 200
    assert result.completed is completed
    assert result.error == (None if completed else "incomplete_sse")
    stats = WorkerStats("normal", 0)
    stats.record(result.status, result.total_s, result.first_content_s,
                 completed=result.completed, error=result.error)
    assert stats.completed == int(completed)
    assert stats.errors == int(not completed)
    assert stats.cancelled == 0


async def test_transport_error_after_terminal_event_still_fails():
    async with client_for([b'data: {"stop":true}\n\n'], failure=httpx.ReadError("closed")) as client:
        result = await stream_request(client, "prompt")
    assert not result.completed
    assert result.error == "transport_error:ReadError"


async def test_fragmented_terminal_event_completes():
    async with client_for([b'data: {"stop":tr', b'ue}\r\n', b'\r\n']) as client:
        result = await stream_request(client, "prompt")
    assert result.completed


async def test_soak_normal_transport_failure_is_error_not_cancelled():
    stats = WorkerStats("normal", 0)
    async with client_for([], failure=httpx.ReadError("closed")) as client:
        await normal_worker(client, 0, time.monotonic() + 0.001, stats)
    assert stats.offered == stats.errors == 1
    assert stats.completed == stats.cancelled == 0
    assert stats.error_types == {"transport_error:ReadError": 1}


@pytest.mark.parametrize("chunks,failure,cancelled,errors,completed", [
    ([b'data: {"content":"hi"}\n\n'], None, 1, 0, 0),
    ([], httpx.ReadError("closed"), 0, 1, 0),
    ([], None, 0, 1, 0),
    ([b'data: {"stop":true}\n\n'], None, 0, 0, 1),
])
async def test_disconnect_only_counts_deliberate_early_close(chunks, failure, cancelled, errors, completed):
    stats = WorkerStats("disconnect", 0)
    async with client_for(chunks, failure=failure) as client:
        await disconnect_worker(client, 0, time.monotonic() + 0.001, stats)
    assert stats.offered == 1
    assert (stats.cancelled, stats.errors, stats.completed) == (cancelled, errors, completed)


@pytest.mark.parametrize("status,field", [(429, "rejected"), (504, "timed_out")])
def test_http_rejections_and_gateway_timeouts_are_distinct(status, field):
    stats = WorkerStats("normal", 0)
    stats.record(status, 0.1, None, error=f"http_{status}")
    assert getattr(stats, field) == 1
    assert stats.errors == stats.cancelled == 0


async def test_cache_timings_without_terminal_event_are_not_benchmark_success():
    async with client_for([b'data: {"timings":{"cache_n":12}}\n\n']) as client:
        result = await run_request(client, WorkloadRequest("s", 0, "p", "r"), time.perf_counter(), 1)
    assert result["cache_n"] == 12
    assert result["success"] is False
    assert result["stream_completed"] is False


def test_reliability_aggregate_does_not_restore_status_only_success():
    records = [{"status": 200, "completed": False, "total_s": .01, "first_content_s": .001}]
    result = aggregate_reliability([{
        "scenario": "normal", "requests": records,
        "summary": {"active_at_end": 0, "queue_at_end": 0},
    }])
    assert result["normal"]["completed"] == 0
    assert result["normal"]["errors"] == 1


async def test_soak_phase_fails_when_worker_raises():
    finished = []

    async def worker(client, worker_id, stop_at, stats):
        if worker_id == 0:
            raise ValueError("worker crashed")
        await asyncio.sleep(0)
        finished.append(worker_id)

    async with client_for([]) as client:
        with pytest.raises(RuntimeError, match="worker crashed"):
            await run_phase("overload", 2, 0.01, worker, client, {})
    assert finished == [1]
