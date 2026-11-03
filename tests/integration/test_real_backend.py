"""End-to-end test against two real llama-server processes.

Skipped unless ``GATEWAY_REAL_BACKENDS=1``.  The default binary/model paths are
the ones used by the Week 1 benchmark; override with ``LLAMA_SERVER``,
``GATEWAY_MODEL`` and ``GATEWAY_E2E_PORTS``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.request

import httpx
import pytest
from prometheus_client.parser import text_string_to_metric_families

from gateway.config import BackendConfig, GatewayConfig
from gateway.server import create_app
from tests.support import start_server

pytestmark = [
    pytest.mark.real_backend,
    pytest.mark.skipif(
        os.environ.get("GATEWAY_REAL_BACKENDS") != "1",
        reason="set GATEWAY_REAL_BACKENDS=1 to run against real llama-server",
    ),
]

WORKSPACE = os.environ.get("GATEWAY_WORKSPACE", str(Path.home() / "gateway-assets"))
LLAMA_SERVER = os.environ.get(
    "LLAMA_SERVER", f"{WORKSPACE}/llama.cpp/build/bin/llama-server"
)
MODEL = os.environ.get(
    "GATEWAY_MODEL", f"{WORKSPACE}/models/qwen2.5-0.5b-instruct-q4_0.gguf"
)
PORTS = tuple(
    int(part) for part in os.environ.get("GATEWAY_E2E_PORTS", "18190,18191").split(",")
)

LONG_PREFIX = (
    "You are a careful travel planning assistant. Respect the budget, avoid "
    "impossible transfers, keep daily activity time bounded, and explain every "
    "factual claim with a tool result. "
) * 8


def start_backend(port: int, log_path: str) -> subprocess.Popen:
    command = [
        LLAMA_SERVER,
        "-m",
        MODEL,
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "-c",
        "4096",
        "-n",
        "16",
        "-t",
        "4",
        "--threads-batch",
        "4",
        "--parallel",
        "1",
        "--device",
        "none",
        "-ngl",
        "0",
        "--metrics",
        "--no-webui",
    ]
    with open(log_path, "w") as log:
        process = subprocess.Popen(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if process.poll() is not None:
                tail = Path(log_path).read_text(errors="replace")[-2000:]
                raise RuntimeError(
                    f"llama-server exited with {process.returncode}: {tail}"
                )
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/health", timeout=1
                ) as response:
                    if response.status == 200:
                        return process
            except Exception:
                time.sleep(0.25)
        raise RuntimeError(
            f"llama-server on port {port} did not become healthy; see {log_path}"
        )
    except BaseException:
        stop_backend(process)
        raise


def stop_backend(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except PermissionError:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except PermissionError:
            process.kill()
        process.wait(timeout=5)


@pytest.fixture(scope="module")
def real_backends(tmp_path_factory):
    if not os.path.isfile(LLAMA_SERVER):
        pytest.skip(f"llama-server not found: {LLAMA_SERVER}")
    if not os.path.isfile(MODEL):
        pytest.skip(f"model not found: {MODEL}")
    log_dir = tmp_path_factory.mktemp("llama_logs")
    processes = []
    try:
        for port in PORTS:
            processes.append(start_backend(port, str(log_dir / f"server_{port}.log")))
        yield PORTS
    finally:
        for process in processes:
            stop_backend(process)


@pytest.fixture
async def gateway(real_backends):
    config = GatewayConfig(
        backends=tuple(
            BackendConfig(name, f"http://127.0.0.1:{port}")
            for name, port in zip(("a", "b"), real_backends)
        ),
        strategy="session_affinity",
        health_interval_s=3600.0,
    )
    server = await start_server(create_app(config))
    try:
        yield server
    finally:
        await server.stop()


def sse_events(raw: bytes) -> list[dict]:
    events = []
    for block in raw.decode("utf-8", "replace").split("\n\n"):
        for line in block.splitlines():
            if line.startswith("data:"):
                payload = line[len("data:") :].strip()
                if payload and payload != "[DONE]":
                    events.append(json.loads(payload))
    return events


async def stream_completion(
    client, prompt: str, session: str
) -> tuple[str, list[dict]]:
    raw = b""
    async with client.stream(
        "POST",
        "/completion",
        json={
            "prompt": prompt,
            "n_predict": 2,
            "temperature": 0,
            "seed": 42,
            "cache_prompt": True,
            "stream": True,
        },
        headers={"X-Session-ID": session},
    ) as response:
        assert response.status_code == 200
        backend = response.headers["x-gateway-backend"]
        async for chunk in response.aiter_raw():
            raw += chunk
    return backend, sse_events(raw)


def metric_sum(text: str, name: str, **labels) -> float:
    total = 0.0
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            if sample.name != name:
                continue
            if all(sample.labels.get(key) == value for key, value in labels.items()):
                total += sample.value
    return total


async def test_streaming_cache_reuse_same_session(gateway, real_backends):
    async with httpx.AsyncClient(base_url=gateway.url, timeout=180) as client:
        prompt = LONG_PREFIX + " Turn one: propose an outline."
        backend_one, first = await stream_completion(client, prompt, "e2e-session")
        backend_two, second = await stream_completion(client, prompt, "e2e-session")

        assert backend_one == backend_two
        assert first and second
        assert first[-1].get("stop") is True
        assert second[-1].get("stop") is True
        assert any(event.get("content") for event in first)

        first_timings = first[-1]["timings"]
        second_timings = second[-1]["timings"]
        assert second_timings["cache_n"] > 0, second_timings
        assert second_timings["cache_n"] >= first_timings["cache_n"]

        text = (await client.get("/metrics")).text
        cached = metric_sum(text, "gateway_prompt_tokens_cached_total")
        prompt_total = metric_sum(text, "gateway_prompt_tokens_total")
        assert cached > 0
        assert cached <= prompt_total
        assert (
            metric_sum(text, "gateway_requests_total", route="completion", status="200")
            == 2
        )
        assert metric_sum(text, "gateway_sse_events_total") >= 4


async def test_non_stream_completion(gateway, real_backends):
    async with httpx.AsyncClient(base_url=gateway.url, timeout=180) as client:
        response = await client.post(
            "/completion",
            json={
                "prompt": "Say hello in one short sentence.",
                "n_predict": 4,
                "temperature": 0,
                "seed": 42,
                "stream": False,
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["content"]
        assert "timings" in payload
        assert response.headers["x-gateway-backend"] in {"a", "b"}


async def test_openai_chat_completion(gateway, real_backends):
    async with httpx.AsyncClient(base_url=gateway.url, timeout=180) as client:
        response = await client.post(
            "/v1/chat/completions",
            json={
                "messages": [{"role": "user", "content": "Say hello."}],
                "max_tokens": 4,
                "temperature": 0,
                "stream": False,
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["choices"]
        assert payload["choices"][0]["message"]["content"]


async def test_health_reports_both_real_backends(gateway, real_backends):
    async with httpx.AsyncClient(base_url=gateway.url, timeout=30) as client:
        response = await client.get("/health")
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "ok"
        assert payload["backends"]["a"]["healthy"] is True
        assert payload["backends"]["b"]["healthy"] is True
