"""HTTP/SSE proxying with bounded admission, deadlines and safe failover.

Execution phases tracked per attempt:

* NOT_DISPATCHED - no upstream request has been sent;
* AWAITING_HEADERS - request bytes may have been sent, response headers not seen;
* HEADERS_RECEIVED - upstream answered; no retry from here on;
* STREAMING - bytes already forwarded to the client; never retry.

Only ``ConnectError`` / ``ConnectTimeout`` before any request bytes are known
to have been sent are retried.  A read timeout, protocol error, upstream 5xx or
mid-stream failure is treated as an unknown execution outcome and is never
retried automatically.

Deadlines are monotonic absolute timestamps.  The total request deadline is
fixed at arrival and is never reset by queueing, retries or failover.  The
execution deadline is fixed at first admission and is also never reset.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import suppress
from dataclasses import dataclass

import httpx
from starlette.requests import ClientDisconnect as StarletteClientDisconnect
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from .admission import (
    AdmissionController,
    AdmissionDeadlineExceeded,
    AdmissionRejected,
    AdmissionState,
    AdmissionTicket,
    AdmissionTimedOut,
    ClientDisconnected,
    acquire_with_disconnect,
)
from .backends import Backend, BackendPool
from .config import GatewayConfig
from .metrics import Metrics
from .router import NoHealthyBackend, Router, RoutingDecision, session_fingerprint
from .sse import SSEEventDecoder, first_content

logger = logging.getLogger("gateway.proxy")

HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}
# Gateway-internal header; it must not leak into upstream logs.
INTERNAL_HEADERS = {"x-session-id"}
PASS_RESPONSE_HEADERS = {"content-type", "cache-control", "etag", "last-modified"}

RETRY_AFTER_SECONDS = 1
NO_BACKEND = Backend(id="none", base_url="")


def _error(
    status: int,
    kind: str,
    message: str,
    *,
    error_extra: dict | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    payload: dict = {"error": {"type": kind, "message": message}}
    if error_extra:
        payload["error"].update(error_extra)
    return JSONResponse(payload, status_code=status, headers=headers)


class _AttemptTimeout(Exception):
    """The execution/total deadline expired while waiting for upstream headers."""


class _ClientGone(Exception):
    """The client disconnected while the gateway awaited upstream headers."""


@dataclass
class _StreamState:
    events: int = 0
    first_content_seen: bool = False
    disconnected: bool = False
    failed: bool = False
    timed_out: bool = False
    deadline_kind: str = "execution_timeout"
    response_status: int | None = None
    finalized: bool = False


def _exception_is_timeout(exc: BaseException) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, BaseExceptionGroup):
        return bool(exc.exceptions) and all(
            _exception_is_timeout(child) for child in exc.exceptions
        )
    return False


class _FinalizingStreamingResponse(StreamingResponse):
    """StreamingResponse with an idempotent finalizer and stream deadline.

    Starlette cancels the body iterator when the client disconnects; the
    generator's state records that.  The response-level ``finally`` also covers
    a body iterator that never starts.  The deadline wraps the whole response
    send loop, so a slow client cannot extend execution beyond the budget.
    """

    def __init__(
        self,
        content,
        finalize,
        *,
        deadline_at: float,
        timeout_kind: str,
        state: _StreamState,
        **kwargs,
    ) -> None:
        super().__init__(content, **kwargs)
        self._finalize = finalize
        self._deadline_at = deadline_at
        self._timeout_kind = timeout_kind
        self._state = state

    async def __call__(self, scope, receive, send) -> None:
        try:
            remaining = self._deadline_at - time.perf_counter()
            if remaining <= 0:
                self._state.timed_out = True
                self._state.deadline_kind = self._timeout_kind
                # Headers have not been committed yet, so a real timeout status
                # is still possible.  The finalizer releases capacity and closes
                # the upstream response.
                self._state.response_status = 504
                body = json.dumps(
                    {
                        "error": {
                            "type": self._timeout_kind,
                            "message": "execution deadline expired before streaming",
                        }
                    }
                ).encode()
                await send(
                    {
                        "type": "http.response.start",
                        "status": 504,
                        "headers": [
                            (b"content-type", b"application/json"),
                            (b"content-length", str(len(body)).encode()),
                        ],
                    }
                )
                await send(
                    {
                        "type": "http.response.body",
                        "body": body,
                        "more_body": False,
                    }
                )
                return
            try:
                async with asyncio.timeout(remaining):
                    await super().__call__(scope, receive, send)
            except TimeoutError:
                self._state.timed_out = True
                self._state.deadline_kind = self._timeout_kind
            except BaseExceptionGroup as group:
                if _exception_is_timeout(group):
                    self._state.timed_out = True
                    self._state.deadline_kind = self._timeout_kind
                else:
                    raise
        finally:
            await self._finalize()


class Proxy:
    def __init__(
        self,
        config: GatewayConfig,
        pool: BackendPool,
        router: Router,
        metrics: Metrics,
        admission: AdmissionController,
    ) -> None:
        self.config = config
        self.pool = pool
        self.router = router
        self.metrics = metrics
        self.admission = admission

    async def handle(self, request: Request, route: str) -> Response:
        started = time.perf_counter()
        request_deadline = (
            started + self.config.deadline.request_timeout_ms / 1000.0
        )
        session_id = request.headers.get("x-session-id")

        try:
            async with asyncio.timeout(
                max(0.000001, request_deadline - time.perf_counter())
            ):
                body = await request.body()
        except TimeoutError:
            return self._no_backend_timeout(route, started, "request_deadline")
        except StarletteClientDisconnect:
            self._finish(route, NO_BACKEND, 499, started, None)
            return Response(status_code=499)

        try:
            decision = self.router.decide(session_id, self.pool.healthy_ids())
        except NoHealthyBackend:
            self.metrics.requests_total.labels(route, "503", NO_BACKEND.id).inc()
            self.metrics.request_duration.labels(route, NO_BACKEND.id).observe(
                time.perf_counter() - started
            )
            logger.warning("proxy no healthy backend route=%s", route)
            return _error(
                503,
                "no_healthy_backend",
                "no healthy backend is available",
                headers={"Retry-After": str(RETRY_AFTER_SECONDS)},
            )

        backend = decision.backend
        if decision.failover:
            self.metrics.routing_failovers.labels(
                decision.preferred_backend_id, backend.id
            ).inc()
            logger.warning(
                "proxy routing failover route=%s preferred=%s selected=%s session=%s",
                route,
                decision.preferred_backend_id,
                backend.id,
                session_fingerprint(session_id),
            )
        logger.info(
            "proxy start route=%s backend=%s session=%s",
            route,
            backend.id,
            session_fingerprint(session_id),
        )

        try:
            ticket = await self._acquire(
                request, route, backend, request_deadline
            )
        except AdmissionRejected:
            self.metrics.admission_rejected.labels(backend.id).inc()
            self.sync_capacity_metrics()
            return self._reject(
                route, backend, started, "queue_full", "admission queue is full"
            )
        except AdmissionTimedOut:
            self.metrics.admission_timed_out.labels(backend.id).inc()
            self.sync_capacity_metrics()
            return self._reject(
                route,
                backend,
                started,
                "queue_timeout",
                "admission queue wait timed out",
            )
        except AdmissionDeadlineExceeded:
            self.metrics.execution_timeouts.labels(
                backend.id, "request_deadline"
            ).inc()
            self.sync_capacity_metrics()
            self._finish(route, backend, 504, started, None)
            return _error(
                504,
                "request_deadline",
                "request deadline expired while waiting for capacity",
            )
        except ClientDisconnected:
            self.metrics.admission_cancelled.labels(backend.id).inc()
            self.metrics.client_cancelled.labels(route, "queued").inc()
            self.sync_capacity_metrics()
            self._finish(route, backend, 499, started, None)
            logger.info(
                "proxy client disconnected while queued route=%s backend=%s",
                route,
                backend.id,
            )
            return Response(status_code=499)
        except asyncio.CancelledError:
            self.metrics.admission_cancelled.labels(backend.id).inc()
            self.sync_capacity_metrics()
            raise

        admitted_at = time.perf_counter()
        execution_deadline = min(
            request_deadline,
            admitted_at + self.config.deadline.execution_timeout_ms / 1000.0,
        )
        if admitted_at >= request_deadline:
            return self._timeout_with_ticket(
                route,
                backend,
                ticket,
                started,
                "request_deadline",
                "request deadline expired before execution",
            )

        try:
            headers = self._upstream_headers(request)
        except Exception:
            self._release(ticket, route, backend, AdmissionState.FAILED)
            raise

        return await self._execute(
            request=request,
            route=route,
            session_id=session_id,
            body=body,
            headers=headers,
            started=started,
            request_deadline=request_deadline,
            execution_deadline=execution_deadline,
            decision=decision,
            ticket=ticket,
        )

    async def _execute(
        self,
        *,
        request: Request,
        route: str,
        session_id: str | None,
        body: bytes,
        headers: dict[str, str],
        started: float,
        request_deadline: float,
        execution_deadline: float,
        decision: RoutingDecision,
        ticket: AdmissionTicket,
    ) -> Response:
        backend = decision.backend
        attempt = 0
        max_attempts = 1 + self.config.retry.max_retries
        retry_from: str | None = None

        while True:
            remaining = execution_deadline - time.perf_counter()
            if remaining <= 0:
                return self._timeout_with_ticket(
                    route,
                    backend,
                    ticket,
                    started,
                    self._deadline_kind(request_deadline, execution_deadline),
                    "execution deadline expired before dispatch",
                )

            try:
                upstream_request = backend.client.build_request(
                    "POST",
                    self._upstream_path(request),
                    content=body,
                    headers=headers,
                )
            except Exception:
                self._release(ticket, route, backend, AdmissionState.FAILED)
                raise

            try:
                upstream, upstream_started = await self._send_with_disconnect(
                    request, backend, upstream_request, remaining
                )
                break
            except _ClientGone:
                self.metrics.client_cancelled.labels(route, "connecting").inc()
                self._release(ticket, route, backend, AdmissionState.CANCELLED)
                self._finish(route, backend, 499, started, None)
                logger.info(
                    "proxy client disconnected before headers route=%s backend=%s",
                    route,
                    backend.id,
                )
                return Response(status_code=499)
            except _AttemptTimeout:
                kind = self._deadline_kind(request_deadline, execution_deadline)
                return self._timeout_with_ticket(
                    route,
                    backend,
                    ticket,
                    started,
                    kind,
                    "execution deadline expired before upstream headers",
                )
            except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
                self.pool.mark_failure(backend.id, type(exc).__name__)
                self.metrics.upstream_errors.labels(backend.id, "connect").inc()
                self.metrics.upstream_disconnects.labels(backend.id, "connect").inc()
                self._release(ticket, route, backend, AdmissionState.FAILED)
                if retry_from is not None:
                    self.metrics.retry_results.labels(
                        retry_from, backend.id, "failed"
                    ).inc()
                logger.warning(
                    "proxy connect failure route=%s backend=%s error=%s attempt=%d",
                    route,
                    backend.id,
                    type(exc).__name__,
                    attempt + 1,
                )
                if attempt + 1 >= max_attempts or time.perf_counter() >= execution_deadline:
                    self._finish(route, backend, 502, started, None)
                    return _error(
                        502,
                        "upstream_connect_error",
                        f"backend {backend.id}: {type(exc).__name__}",
                    )

                attempt += 1
                self.metrics.retries.labels(backend.id, "connect").inc()
                failed_backend = backend
                healthy = self.pool.healthy_ids() - {failed_backend.id}
                try:
                    next_decision = self.router.decide(session_id, healthy)
                except NoHealthyBackend:
                    self.metrics.retry_results.labels(
                        failed_backend.id, "none", "no_backend"
                    ).inc()
                    self._finish(route, failed_backend, 502, started, None)
                    return _error(
                        502,
                        "upstream_connect_error",
                        "no healthy backend remains after connect failure",
                    )

                next_backend = next_decision.backend
                try:
                    next_ticket = await self._acquire(
                        request, route, next_backend, request_deadline
                    )
                except AdmissionRejected:
                    self.metrics.retry_results.labels(
                        failed_backend.id, next_backend.id, "rejected"
                    ).inc()
                    self.metrics.admission_rejected.labels(next_backend.id).inc()
                    self.sync_capacity_metrics()
                    return self._reject(
                        route,
                        next_backend,
                        started,
                        "queue_full",
                        "admission queue is full on retry target",
                    )
                except AdmissionTimedOut:
                    self.metrics.retry_results.labels(
                        failed_backend.id, next_backend.id, "timed_out"
                    ).inc()
                    self.metrics.admission_timed_out.labels(next_backend.id).inc()
                    self.sync_capacity_metrics()
                    return self._reject(
                        route,
                        next_backend,
                        started,
                        "queue_timeout",
                        "admission queue wait timed out on retry target",
                    )
                except AdmissionDeadlineExceeded:
                    self.metrics.retry_results.labels(
                        failed_backend.id, next_backend.id, "deadline"
                    ).inc()
                    self.metrics.execution_timeouts.labels(
                        next_backend.id, "request_deadline"
                    ).inc()
                    self._finish(route, next_backend, 504, started, None)
                    return _error(
                        504,
                        "request_deadline",
                        "request deadline expired before retry execution",
                    )
                except ClientDisconnected:
                    self.metrics.retry_results.labels(
                        failed_backend.id, next_backend.id, "cancelled"
                    ).inc()
                    self.metrics.client_cancelled.labels(route, "queued").inc()
                    self.metrics.admission_cancelled.labels(next_backend.id).inc()
                    self.sync_capacity_metrics()
                    self._finish(route, next_backend, 499, started, None)
                    return Response(status_code=499)

                logger.info(
                    "proxy retry route=%s from=%s to=%s attempt=%d",
                    route,
                    failed_backend.id,
                    next_backend.id,
                    attempt,
                )
                retry_from = failed_backend.id
                backend = next_backend
                ticket = next_ticket
                decision = next_decision
                continue
            except httpx.HTTPError as exc:
                # The request may have been received; do not retry.
                self.pool.mark_failure(backend.id, type(exc).__name__)
                self.metrics.upstream_errors.labels(backend.id, "stream").inc()
                self.metrics.upstream_disconnects.labels(backend.id, "connect").inc()
                self._release(ticket, route, backend, AdmissionState.FAILED)
                self._finish(route, backend, 502, started, None)
                logger.warning(
                    "proxy upstream protocol failure route=%s backend=%s error=%s",
                    route,
                    backend.id,
                    type(exc).__name__,
                )
                return _error(
                    502,
                    "upstream_error",
                    f"backend {backend.id}: {type(exc).__name__}",
                )
            except asyncio.CancelledError:
                self._release(ticket, route, backend, AdmissionState.CANCELLED)
                raise

        if retry_from is not None:
            self.metrics.retry_results.labels(
                retry_from,
                backend.id,
                "success" if upstream.status_code < 500 else "http_error",
            ).inc()
            retry_from = None

        content_type = upstream.headers.get("content-type", "")
        if upstream.status_code >= 500:
            # A business 5xx is request-specific; it is not a health failure.
            self.metrics.upstream_errors.labels(backend.id, "http_status").inc()

        if "text/event-stream" in content_type.lower():
            state = _StreamState()
            timeout_kind = self._deadline_kind(request_deadline, execution_deadline)

            async def finalize() -> None:
                await self._finalize_stream(
                    state, upstream, ticket, backend, route, started, upstream_started
                )

            try:
                return _FinalizingStreamingResponse(
                    self._stream(upstream, backend, route, started, state),
                    finalize,
                    deadline_at=execution_deadline,
                    timeout_kind=timeout_kind,
                    state=state,
                    status_code=upstream.status_code,
                    headers=self._response_headers(upstream, backend, ticket),
                )
            except Exception:
                self._release(ticket, route, backend, AdmissionState.FAILED)
                await self._aclose(upstream)
                raise

        remaining = execution_deadline - time.perf_counter()
        if remaining <= 0:
            await self._aclose(upstream)
            return self._timeout_with_ticket(
                route,
                backend,
                ticket,
                started,
                self._deadline_kind(request_deadline, execution_deadline),
                "execution deadline expired before reading the body",
            )
        try:
            async with asyncio.timeout(remaining):
                raw = await upstream.aread()
        except TimeoutError:
            await self._aclose(upstream)
            return self._timeout_with_ticket(
                route,
                backend,
                ticket,
                started,
                self._deadline_kind(request_deadline, execution_deadline),
                "execution deadline expired while reading the body",
            )
        except httpx.HTTPError as exc:
            await self._aclose(upstream)
            self.pool.mark_failure(backend.id, type(exc).__name__)
            self.metrics.upstream_errors.labels(backend.id, "stream").inc()
            self.metrics.upstream_disconnects.labels(backend.id, "body").inc()
            self._release(ticket, route, backend, AdmissionState.FAILED)
            self._finish(route, backend, 502, started, None)
            return _error(
                502, "upstream_error", f"backend {backend.id}: {type(exc).__name__}"
            )
        except asyncio.CancelledError:
            await self._aclose(upstream)
            self.metrics.client_cancelled.labels(route, "body").inc()
            self._release(ticket, route, backend, AdmissionState.CANCELLED)
            raise

        await self._aclose(upstream)
        self._observe_json_usage(backend, raw, content_type)
        self._release(ticket, route, backend, AdmissionState.FINISHED)
        self._finish(route, backend, upstream.status_code, started, upstream_started)
        return Response(
            content=raw,
            status_code=upstream.status_code,
            headers=self._response_headers(upstream, backend, ticket),
        )

    async def _acquire(
        self,
        request: Request,
        route: str,
        backend: Backend,
        request_deadline: float,
    ) -> AdmissionTicket:
        if time.perf_counter() >= request_deadline:
            raise AdmissionDeadlineExceeded(
                backend.id, "request deadline expired before admission"
            )
        ticket = self.admission.try_acquire(backend.id)
        if ticket is None:
            ticket = await acquire_with_disconnect(
                self.admission, request, backend.id, deadline_at=request_deadline
            )
        self.metrics.admission_admitted.labels(backend.id).inc()
        self.metrics.queue_wait.labels(backend.id).observe(
            ticket.queue_wait_s or 0.0
        )
        self.sync_capacity_metrics()
        return ticket

    async def _send_with_disconnect(
        self,
        request: Request,
        backend: Backend,
        upstream_request: httpx.Request,
        remaining: float,
    ) -> tuple[httpx.Response, float]:
        send_task = asyncio.create_task(
            backend.client.send(upstream_request, stream=True),
            name=f"upstream-send-{backend.id}",
        )
        upstream_started = time.perf_counter()
        try:
            async with asyncio.timeout(remaining):
                while True:
                    done, _ = await asyncio.wait({send_task}, timeout=0.05)
                    if done:
                        return send_task.result(), upstream_started
                    if await request.is_disconnected():
                        send_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await send_task
                        raise _ClientGone()
        except TimeoutError:
            send_task.cancel()
            with suppress(asyncio.CancelledError):
                await send_task
            raise _AttemptTimeout() from None
        except asyncio.CancelledError:
            send_task.cancel()
            with suppress(asyncio.CancelledError):
                await send_task
            raise

    def _timeout_with_ticket(
        self,
        route: str,
        backend: Backend,
        ticket: AdmissionTicket,
        started: float,
        kind: str,
        message: str,
    ) -> JSONResponse:
        self.metrics.execution_timeouts.labels(backend.id, kind).inc()
        self._release(ticket, route, backend, AdmissionState.TIMED_OUT)
        self._finish(route, backend, 504, started, None)
        logger.warning(
            "proxy deadline exceeded route=%s backend=%s kind=%s", route, backend.id, kind
        )
        return _error(504, kind, message)

    def _no_backend_timeout(self, route: str, started: float, kind: str) -> JSONResponse:
        self.metrics.execution_timeouts.labels(NO_BACKEND.id, kind).inc()
        self.metrics.requests_total.labels(route, "504", NO_BACKEND.id).inc()
        self.metrics.request_duration.labels(route, NO_BACKEND.id).observe(
            time.perf_counter() - started
        )
        return _error(504, kind, "request deadline expired while reading the request")

    def _reject(
        self,
        route: str,
        backend: Backend,
        started: float,
        kind: str,
        message: str,
    ) -> JSONResponse:
        self._finish(route, backend, 429, started, None)
        logger.info(
            "proxy admission rejected route=%s backend=%s kind=%s", route, backend.id, kind
        )
        return _error(
            429,
            kind,
            message,
            error_extra={"retry_after_ms": RETRY_AFTER_SECONDS * 1000},
            headers={"Retry-After": str(RETRY_AFTER_SECONDS)},
        )

    def _release(
        self,
        ticket: AdmissionTicket,
        route: str,
        backend: Backend,
        state: AdmissionState,
    ) -> None:
        if ticket.state is not AdmissionState.ADMITTED:
            return
        execution_s = time.perf_counter() - (ticket.admitted_at or time.perf_counter())
        if not self.admission.release(ticket, state):
            return
        self.metrics.backend_execution.labels(route, backend.id).observe(
            max(0.0, execution_s)
        )
        self.sync_capacity_metrics()

    def _finish(
        self,
        route: str,
        backend: Backend,
        status: int,
        started: float,
        upstream_started: float | None,
    ) -> None:
        now = time.perf_counter()
        self.metrics.requests_total.labels(route, str(status), backend.id).inc()
        self.metrics.request_duration.labels(route, backend.id).observe(now - started)
        if upstream_started is not None:
            self.metrics.upstream_duration.labels(route, backend.id).observe(
                now - upstream_started
            )
        logger.info(
            "proxy done route=%s backend=%s status=%s duration_ms=%.2f",
            route,
            backend.id,
            status,
            (now - started) * 1000.0,
        )

    async def _stream(
        self,
        upstream: httpx.Response,
        backend: Backend,
        route: str,
        started: float,
        state: _StreamState,
    ):
        decoder = SSEEventDecoder()
        try:
            async for chunk in upstream.aiter_raw():
                for event in decoder.feed(chunk):
                    state.events += 1
                    state.first_content_seen = self._observe_event(
                        event, backend, started, state.first_content_seen
                    )
                yield chunk
            for event in decoder.flush():
                state.events += 1
                state.first_content_seen = self._observe_event(
                    event, backend, started, state.first_content_seen
                )
        except asyncio.CancelledError:
            state.disconnected = True
            raise
        except (httpx.HTTPError, OSError) as exc:
            state.failed = True
            self.pool.mark_failure(backend.id, type(exc).__name__)
            self.metrics.upstream_errors.labels(backend.id, "stream").inc()
            self.metrics.upstream_disconnects.labels(backend.id, "stream").inc()
            logger.warning(
                "proxy stream error route=%s backend=%s error=%s",
                route,
                backend.id,
                type(exc).__name__,
            )
        except Exception:
            state.failed = True
            raise

    async def _finalize_stream(
        self,
        state: _StreamState,
        upstream: httpx.Response,
        ticket: AdmissionTicket,
        backend: Backend,
        route: str,
        started: float,
        upstream_started: float | None,
    ) -> None:
        if state.finalized:
            return
        state.finalized = True
        if state.events:
            self.metrics.sse_events.labels(backend.id).inc(state.events)
        await self._aclose(upstream)
        if state.timed_out:
            exit_state = AdmissionState.TIMED_OUT
            self.metrics.execution_timeouts.labels(
                backend.id, state.deadline_kind
            ).inc()
            self.metrics.stream_terminations.labels(backend.id, "deadline").inc()
        elif state.disconnected:
            exit_state = AdmissionState.CANCELLED
            self.metrics.client_disconnects.labels(route).inc()
            self.metrics.client_cancelled.labels(route, "streaming").inc()
            self.metrics.stream_terminations.labels(
                backend.id, "client_disconnect"
            ).inc()
        elif state.failed:
            exit_state = AdmissionState.FAILED
            self.metrics.stream_terminations.labels(
                backend.id, "upstream_error"
            ).inc()
        else:
            exit_state = AdmissionState.FINISHED
            self.metrics.stream_terminations.labels(backend.id, "completed").inc()
        self._release(ticket, route, backend, exit_state)
        self._finish(
            route,
            backend,
            state.response_status or upstream.status_code,
            started,
            upstream_started,
        )
        if state.timed_out:
            logger.warning(
                "proxy stream deadline route=%s backend=%s kind=%s",
                route,
                backend.id,
                state.deadline_kind,
            )
        elif state.disconnected:
            logger.info(
                "proxy client disconnected route=%s backend=%s", route, backend.id
            )

    def _observe_event(
        self,
        event,
        backend: Backend,
        started: float,
        first_content_seen: bool,
    ) -> bool:
        obj = event.json()
        if obj is None:
            return first_content_seen
        if not first_content_seen and first_content(obj) is not None:
            self.metrics.ttft.labels(backend.id).observe(time.perf_counter() - started)
            first_content_seen = True
        self._observe_timings(backend, obj)
        return first_content_seen

    def _observe_json_usage(self, backend: Backend, raw: bytes, content_type: str) -> None:
        if "json" not in content_type.lower():
            return
        try:
            obj = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return
        self._observe_timings(backend, obj)

    def _observe_timings(self, backend: Backend, obj: object) -> None:
        if not isinstance(obj, dict):
            return
        timings = obj.get("timings")
        if not isinstance(timings, dict):
            return
        cached = timings.get("cache_n")
        fresh = timings.get("prompt_n")
        if isinstance(cached, int) and isinstance(fresh, int):
            self.metrics.observe_usage(backend.id, cached, fresh)

    async def _aclose(self, upstream: httpx.Response) -> None:
        with suppress(Exception):
            await upstream.aclose()

    @staticmethod
    def _deadline_kind(request_deadline: float, execution_deadline: float) -> str:
        return (
            "request_deadline"
            if request_deadline <= execution_deadline
            else "execution_timeout"
        )

    def sync_capacity_metrics(self) -> None:
        stats = self.admission.stats()
        self.metrics.active_requests.set(stats.active_total)
        self.metrics.queue_depth.set(stats.queued_total)
        for backend_id in self.admission.backend_ids:
            self.metrics.in_flight.labels(backend_id).set(
                stats.active_by_backend.get(backend_id, 0)
            )
            self.metrics.queue_depth_backend.labels(backend_id).set(
                stats.queued_by_backend.get(backend_id, 0)
            )
        self.metrics.peak_in_flight.set(stats.peak_active)
        self.metrics.peak_queue_depth.set(stats.peak_queued)

    def _upstream_headers(self, request: Request) -> dict[str, str]:
        headers: dict[str, str] = {}
        for key, value in request.headers.items():
            lowered = key.lower()
            if lowered in HOP_BY_HOP or lowered in INTERNAL_HEADERS:
                continue
            headers[key] = value
        # Keep SSE bytes opaque: no compression and no automatic decoding.
        headers["accept-encoding"] = "identity"
        return headers

    def _upstream_path(self, request: Request) -> str:
        path = request.url.path
        if request.url.query:
            path = f"{path}?{request.url.query}"
        return path

    def _response_headers(
        self, upstream: httpx.Response, backend: Backend, ticket: AdmissionTicket
    ) -> dict[str, str]:
        headers = {
            key: value
            for key, value in upstream.headers.items()
            if key.lower() in PASS_RESPONSE_HEADERS
        }
        headers["x-gateway-backend"] = backend.id
        if ticket.queue_wait_s is not None:
            headers["x-gateway-queue-ms"] = f"{ticket.queue_wait_s * 1000.0:.1f}"
        return headers
