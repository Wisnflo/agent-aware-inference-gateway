"""Routing policies with health-aware selection.

* ``round_robin`` - each request advances the counter by one; if that backend
  is unhealthy, the next healthy backend in configuration order is used.
* ``session_affinity`` - rendezvous SHA-256 over all backends.  The preferred
  backend is stable; if it is unhealthy, the same rendezvous is applied to the
  currently healthy subset.  When the preferred backend recovers it becomes
  eligible again immediately (explicit immediate failback).

Health filtering is passed in by the caller as a set of backend ids.  ``None``
means "all eligible" (used by unit tests and when health is unknown).
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Protocol, Sequence

from .backends import Backend
from .config import ROUND_ROBIN, SESSION_AFFINITY


class NoHealthyBackend(Exception):
    """No configured backend is currently eligible for routing."""


@dataclass(frozen=True)
class RoutingDecision:
    backend: Backend
    preferred_backend_id: str
    failover: bool


class Router(Protocol):
    strategy: str

    def decide(
        self, session_id: str | None, healthy_ids: set[str] | None = None
    ) -> RoutingDecision:
        """Return the backend plus whether health forced a migration."""

    def select(
        self, session_id: str | None = None, healthy_ids: set[str] | None = None
    ) -> Backend:
        """Return only the selected backend."""


def _eligible(
    backends: Sequence[Backend], healthy_ids: set[str] | None
) -> list[Backend]:
    if healthy_ids is None:
        return list(backends)
    return [backend for backend in backends if backend.id in healthy_ids]


class RoundRobinRouter:
    strategy = ROUND_ROBIN

    def __init__(self, backends: Sequence[Backend]) -> None:
        if not backends:
            raise ValueError("round-robin router needs at least one backend")
        self._backends = tuple(backends)
        self._next = 0

    def decide(
        self, session_id: str | None = None, healthy_ids: set[str] | None = None
    ) -> RoutingDecision:
        eligible = _eligible(self._backends, healthy_ids)
        if not eligible:
            raise NoHealthyBackend("no healthy backend available")
        start = self._next % len(self._backends)
        self._next += 1
        preferred = self._backends[start]
        if preferred.id in {backend.id for backend in eligible}:
            return RoutingDecision(preferred, preferred.id, False)
        for offset in range(1, len(self._backends) + 1):
            candidate = self._backends[(start + offset) % len(self._backends)]
            if candidate.id in {backend.id for backend in eligible}:
                return RoutingDecision(candidate, preferred.id, True)
        raise NoHealthyBackend("no healthy backend available")

    def select(
        self, session_id: str | None = None, healthy_ids: set[str] | None = None
    ) -> Backend:
        return self.decide(session_id, healthy_ids).backend


class SessionAffinityRouter:
    """Rendezvous hashing over ``sha256(backend_id NUL session_id)``."""

    strategy = SESSION_AFFINITY

    def __init__(self, backends: Sequence[Backend]) -> None:
        if not backends:
            raise ValueError("session-affinity router needs at least one backend")
        self._backends = tuple(backends)
        self._fallback = RoundRobinRouter(backends)

    def decide(
        self, session_id: str | None = None, healthy_ids: set[str] | None = None
    ) -> RoutingDecision:
        if not session_id:
            return self._fallback.decide(None, healthy_ids)

        eligible = _eligible(self._backends, healthy_ids)
        if not eligible:
            raise NoHealthyBackend("no healthy backend available")
        preferred = self._select_for_key(self._backends, session_id)
        if any(backend.id == preferred.id for backend in eligible):
            return RoutingDecision(preferred, preferred.id, False)
        fallback = self._select_for_key(eligible, session_id)
        return RoutingDecision(fallback, preferred.id, True)

    def select(
        self, session_id: str | None = None, healthy_ids: set[str] | None = None
    ) -> Backend:
        return self.decide(session_id, healthy_ids).backend

    @staticmethod
    def _select_for_key(backends: Sequence[Backend], session_id: str) -> Backend:
        # The tuple key makes ties deterministic: highest score wins, and on a
        # (astronomically unlikely) tie the lexicographically larger id wins.
        return max(
            backends,
            key=lambda backend: (_score(session_id, backend.id), backend.id),
        )

    @classmethod
    def mapping_for(cls, backends: Sequence[Backend], session_ids: Sequence[str]) -> dict[str, str]:
        """Helper used by tests and migration checks."""
        return {sid: cls._select_for_key(backends, sid).id for sid in session_ids}


def _score(session_id: str, backend_id: str) -> int:
    digest = hashlib.sha256(f"{backend_id}\x00{session_id}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def make_router(strategy: str, backends: Sequence[Backend]) -> Router:
    if strategy == ROUND_ROBIN:
        return RoundRobinRouter(backends)
    if strategy == SESSION_AFFINITY:
        return SessionAffinityRouter(backends)
    raise ValueError(f"unknown strategy: {strategy!r}")


def session_fingerprint(session_id: str | None) -> str:
    """Short, non-reversible tag for logs.  Never log the raw session id."""
    if not session_id:
        return "none"
    return hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:8]
