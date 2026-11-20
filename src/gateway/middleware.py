"""Request correlation for logs, responses and upstream calls."""

from __future__ import annotations

import contextvars
import logging
import re
import uuid

from starlette.datastructures import Headers, MutableHeaders

REQUEST_ID_HEADER = "x-request-id"
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "gateway_request_id", default="-"
)


class RequestIDFilter(logging.Filter):
    """Inject the current request id into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        return True


def new_request_id() -> str:
    return uuid.uuid4().hex


def sanitize_request_id(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if REQUEST_ID_PATTERN.fullmatch(value):
        return value
    return None


class RequestIDMiddleware:
    """Add ``X-Request-ID`` to every response and the log context.

    Client-supplied ids are accepted only when they match a bounded safe
    character set; otherwise the gateway generates one.  The id is also
    forwarded upstream, and existing ``traceparent``/``tracestate`` headers
    continue to pass through as ordinary end-to-end trace context.
    """

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = Headers(scope=scope)
        request_id = sanitize_request_id(headers.get(REQUEST_ID_HEADER))
        if request_id is None:
            request_id = new_request_id()
        state = scope.setdefault("state", {})
        state["request_id"] = request_id
        token = request_id_var.set(request_id)

        async def send_with_request_id(message) -> None:
            if message["type"] == "http.response.start":
                response_headers = MutableHeaders(scope=message)
                response_headers[REQUEST_ID_HEADER] = request_id
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        finally:
            request_id_var.reset(token)
