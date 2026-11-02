"""Helpers for integration tests: real uvicorn servers in the test loop."""
from __future__ import annotations

import asyncio
import contextlib
import json
import socket
from dataclasses import dataclass, field

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route


@dataclass
class ServerHandle:
    server: uvicorn.Server
    task: asyncio.Task
    port: int
    sock: socket.socket

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def stop(self) -> None:
        if not self.task.done():
            self.server.should_exit = True
        try:
            await asyncio.wait_for(self.task, timeout=10)
        finally:
            self.sock.close()


def free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


async def start_server(app: Starlette) -> ServerHandle:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    config = uvicorn.Config(app, log_level="warning", access_log=False, lifespan="on")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve(sockets=[sock]))
    for _ in range(1000):
        if server.started:
            return ServerHandle(server=server, task=task, port=port, sock=sock)
        if task.done():
            task.result()
        await asyncio.sleep(0.01)
    raise RuntimeError("uvicorn did not start in time")


@dataclass
class FakeBackend:
    name: str
    requests: list[dict] = field(default_factory=list)
    stream_closed: asyncio.Event = field(default_factory=asyncio.Event)
    stream_started: asyncio.Event = field(default_factory=asyncio.Event)
    started: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    disconnected: asyncio.Event = field(default_factory=asyncio.Event)
    health_ok: bool = True
    sent_events: int = 0

    def app(self) -> Starlette:
        async def health(request: Request) -> JSONResponse:
            if not self.health_ok:
                return JSONResponse({"status": "down"}, status_code=503)
            return JSONResponse({"status": "ok"})

        async def completion(request: Request) -> JSONResponse | StreamingResponse:
            body = await request.json()
            self.requests.append(
                {"body": body, "headers": dict(request.headers), "path": request.url.path}
            )
            self.started.set()
            if body.get("hold_before"):
                await self.release.wait()
            response_delay = float(body.get("response_delay", 0.0))
            if response_delay:
                await asyncio.sleep(response_delay)
            status = int(body.get("fake_status", 200))
            if status != 200:
                return JSONResponse({"error": f"fake {status}"}, status_code=status)

            if body.get("stream"):
                return StreamingResponse(
                    self._events(body),
                    media_type="text/event-stream",
                    headers={"x-fake-backend": self.name},
                )

            return JSONResponse(
                {
                    "backend": self.name,
                    "timings": {
                        "cache_n": int(body.get("fake_cache_n", 0)),
                        "prompt_n": int(body.get("fake_prompt_n", 1)),
                        "prompt_ms": 1.0,
                    },
                },
                headers={"x-fake-backend": self.name},
            )

        return Starlette(
            routes=[
                Route("/health", health, methods=["GET"]),
                Route("/completion", completion, methods=["POST"]),
                Route("/v1/chat/completions", completion, methods=["POST"]),
            ]
        )

    async def _events(self, body: dict):
        count = int(body.get("events", 3))
        hold_after = body.get("hold_after")
        fail_after = body.get("fail_after")
        delay = float(body.get("delay", 0.0))
        padding = int(body.get("padding", 0))
        try:
            self.stream_started.set()
            for index in range(count):
                if delay:
                    await asyncio.sleep(delay)
                payload = {"content": f"{self.name}-{index}", "stop": False}
                if padding:
                    payload["pad"] = "x" * padding
                yield f"data: {json.dumps(payload)}\n\n".encode()
                self.sent_events += 1
                if fail_after is not None and index >= int(fail_after):
                    raise RuntimeError("fake stream failure")
                if hold_after is not None and index >= int(hold_after):
                    await self.release.wait()
            final = {
                "content": "",
                "stop": True,
                "timings": {
                    "cache_n": int(body.get("fake_cache_n", 0)),
                    "prompt_n": int(body.get("fake_prompt_n", 1)),
                },
            }
            yield f"data: {json.dumps(final)}\n\n".encode()
            yield b"data: [DONE]\n\n"
        finally:
            self.stream_closed.set()


class FakeBackends:
    """A pair of fake backends with running servers."""

    def __init__(self) -> None:
        self.a = FakeBackend("a")
        self.b = FakeBackend("b")
        self.servers: list[ServerHandle] = []

    async def start(self) -> "FakeBackends":
        self.servers = [
            await start_server(self.a.app()),
            await start_server(self.b.app()),
        ]
        return self

    @property
    def urls(self) -> tuple[str, str]:
        return self.servers[0].url, self.servers[1].url

    async def stop(self) -> None:
        for server in self.servers:
            await server.stop()


def parse_sse(raw: str) -> list[dict]:
    events: list[dict] = []
    for block in raw.split("\n\n"):
        for line in block.splitlines():
            if line.startswith("data:"):
                payload = line[len("data:") :].strip()
                if payload == "[DONE]":
                    events.append({"done": True})
                elif payload:
                    events.append(json.loads(payload))
    return events


class RawBackend:
    """A low-level TCP backend for precise connection-failure scenarios.

    ``/health`` always answers 200.  ``/completion`` follows ``behavior``:

    * ``close_before_headers`` - accept the request, close without any response;
    * ``close_mid_stream`` - send SSE headers plus one event, then close without
      the terminating chunk.

    This is deliberately not Starlette: it must be able to violate HTTP in
    controlled ways that a normal ASGI server would turn into a 500.
    """

    def __init__(
        self,
        name: str = "raw",
        behavior: str = "close_before_headers",
        *,
        events: int = 200,
        padding: int = 16384,
    ):
        self.name = name
        self.behavior = behavior
        self.events = events
        self.padding = padding
        self.server: asyncio.AbstractServer | None = None
        self.port: int | None = None
        self.requests: list[dict] = []
        self.connections = 0
        self.sent_events = 0

    @property
    def url(self) -> str:
        if self.port is None:
            raise RuntimeError("raw backend not started")
        return f"http://127.0.0.1:{self.port}"

    async def start(self) -> "RawBackend":
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.connections += 1
        try:
            header = b""
            while b"\r\n\r\n" not in header:
                chunk = await reader.read(4096)
                if not chunk:
                    return
                header += chunk
            head, _, body = header.partition(b"\r\n\r\n")
            lines = head.split(b"\r\n")
            method, path, _ = lines[0].decode().split(" ")
            content_length = 0
            for line in lines[1:]:
                if line.lower().startswith(b"content-length:"):
                    content_length = int(line.split(b":", 1)[1].strip())
            while len(body) < content_length:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                body += chunk
            self.requests.append({"method": method, "path": path, "body": body})
            if path == "/health":
                payload = b'{"status": "ok"}'
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                    + f"Content-Length: {len(payload)}\r\n".encode()
                    + b"Connection: close\r\n\r\n"
                    + payload
                )
                await writer.drain()
                return
            if self.behavior == "close_before_headers":
                return
            if self.behavior == "close_mid_stream":
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                    b"Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                )
                event = b'data: {"content": "raw-0", "stop": false}\n\n'
                writer.write(f"{len(event):x}\r\n".encode() + event + b"\r\n")
                await writer.drain()
                await asyncio.sleep(0.02)
                return
            if self.behavior == "stream_many":
                sock = writer.get_extra_info("socket")
                if sock is not None:
                    with contextlib.suppress(OSError):
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                    b"Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                )
                await writer.drain()
                for index in range(self.events):
                    payload = json.dumps(
                        {"content": f"raw-{index}", "pad": "x" * self.padding}
                    ).encode()
                    event = b"data: " + payload + b"\n\n"
                    writer.write(
                        f"{len(event):x}\r\n".encode() + event + b"\r\n"
                    )
                    await writer.drain()
                    self.sent_events += 1
                writer.write(b"0\r\n\r\n")
                await writer.drain()
                return
            raise RuntimeError(f"unknown raw behavior: {self.behavior}")
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()
