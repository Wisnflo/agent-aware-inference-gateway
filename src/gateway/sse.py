"""Incremental SSE parser.

HTTP chunks do not align with SSE events: one chunk may contain several
events, or half an event.  ``SSEEventDecoder`` buffers raw bytes and only
yields complete events (terminated by a blank line), so event order is the
upstream order and nothing is parsed twice.

The proxy forwards raw chunks unchanged; this decoder is used only for
metrics such as event count, time-to-first-content and cache counters.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SSEEvent:
    data: str
    event: str | None = None
    id: str | None = None

    def json(self) -> object | None:
        """Parse ``data`` as JSON, returning None when it is not JSON."""
        import json

        if not self.data or self.data == "[DONE]":
            return None
        try:
            return json.loads(self.data)
        except (ValueError, RecursionError):
            return None


class SSEEventDecoder:
    def __init__(self, max_event_bytes: int = 64 * 1024) -> None:
        if max_event_bytes < 1:
            raise ValueError("max_event_bytes must be positive")
        self._max_event_bytes = max_event_bytes
        self._buffer = bytearray()
        self._scan_from = 0
        self._discarding = False

    def feed(self, chunk: bytes) -> list[SSEEvent]:
        events: list[SSEEvent] = []
        # This parser observes metrics only. Oversized events are skipped while
        # the proxy continues forwarding the original bytes unchanged.
        for offset in range(0, len(chunk), self._max_event_bytes):
            self._buffer.extend(chunk[offset : offset + self._max_event_bytes])
            while True:
                boundary = _find_boundary(self._buffer, self._scan_from)
                if boundary is None:
                    if len(self._buffer) > self._max_event_bytes + 3:
                        self._discarding = True
                        del self._buffer[:-3]
                    self._scan_from = max(0, len(self._buffer) - 3)
                    break
                index, length = boundary
                if not self._discarding and index <= self._max_event_bytes:
                    event = _parse_event(self._buffer[:index])
                    if event is not None:
                        events.append(event)
                del self._buffer[: index + length]
                self._scan_from = 0
                self._discarding = False
        return events

    def flush(self) -> list[SSEEvent]:
        """Emit a trailing event that was not terminated by a blank line."""
        raw, self._buffer = self._buffer, bytearray()
        event = (
            _parse_event(raw)
            if not self._discarding
            and len(raw) <= self._max_event_bytes
            and raw.strip()
            else None
        )
        self._scan_from = 0
        self._discarding = False
        return [event] if event is not None else []


def _find_boundary(buffer: bytes | bytearray, start: int = 0) -> tuple[int, int] | None:
    candidates: list[tuple[int, int]] = []
    for marker, length in ((b"\r\n\r\n", 4), (b"\n\n", 2), (b"\r\r", 2)):
        index = buffer.find(marker, start)
        if index != -1:
            candidates.append((index, length))
    if not candidates:
        return None
    return min(candidates, key=lambda item: (item[0], -item[1]))


def _parse_event(raw: bytes) -> SSEEvent | None:
    text = raw.decode("utf-8", "replace")
    data_lines: list[str] = []
    event_name: str | None = None
    event_id: str | None = None
    for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        if not line or line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "data":
            data_lines.append(value)
        elif field == "event":
            event_name = value
        elif field == "id":
            event_id = value
        # ``retry`` and unknown fields are irrelevant to proxy metrics.
    if not data_lines and event_name is None and event_id is None:
        return None
    return SSEEvent(data="\n".join(data_lines), event=event_name, id=event_id)


def first_content(obj: object) -> str | None:
    """Return generated text from a llama.cpp or OpenAI streaming event."""
    if not isinstance(obj, dict):
        return None
    content = obj.get("content")
    if isinstance(content, str) and content:
        return content
    choices = obj.get("choices")
    if isinstance(choices, list) and choices:
        choice = choices[0]
        if isinstance(choice, dict):
            delta = choice.get("delta")
            if isinstance(delta, dict):
                text = delta.get("content")
                if isinstance(text, str) and text:
                    return text
            text = choice.get("text")
            if isinstance(text, str) and text:
                return text
    return None
