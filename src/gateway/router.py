"""Routing policies with health-aware and prefix-aware selection.

Policies:

* ``round_robin`` - one counter, health-aware scanning.
* ``session_affinity`` - SHA-256 rendezvous over ``X-Session-ID``.
* ``prefix_affinity`` - SHA-256 character-chunk trie of the prompt prefix;
  requests with the same textual prefix prefer the backend that served it.
* ``kv_cache_aware`` - the same prefix trie, but cache-hit feedback from
  llama.cpp ``timings.cache_n`` is used to prefer *verified* cache locations.
  A text match alone is never called a KV-cache location.

Health filtering is passed in by the caller as a set of backend ids.  ``None``
means "all eligible" (unit tests/unknown health).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Callable, Mapping, Protocol, Sequence

from .backends import Backend
from .config import (
    KV_CACHE_AWARE,
    PREFIX_AFFINITY,
    ROUND_ROBIN,
    SESSION_AFFINITY,
    PrefixConfig,
)
from .prefix import PrefixHashTrie, PrefixTrieStats


class NoHealthyBackend(Exception):
    """No configured backend is currently eligible for routing."""


@dataclass(frozen=True)
class RoutingDecision:
    backend: Backend
    preferred_backend_id: str
    failover: bool
    prefix_match_chars: int = 0
    cache_verified: bool = False
    prefix_match_tokens: int = 0
    load_overflow: bool = False


class Router(Protocol):
    strategy: str

    def decide(
        self,
        session_id: str | None,
        healthy_ids: set[str] | None = None,
        prompt: str | None = None,
        loads: Mapping[str, int] | None = None,
        prefix_tokens: Sequence[int] | None = None,
    ) -> RoutingDecision:
        """Return the backend plus routing metadata."""

    def select(
        self,
        session_id: str | None = None,
        healthy_ids: set[str] | None = None,
        prompt: str | None = None,
        loads: Mapping[str, int] | None = None,
        prefix_tokens: Sequence[int] | None = None,
    ) -> Backend:
        """Return only the selected backend."""

    def record_prompt(
        self,
        prompt: str | None,
        backend_id: str,
        prefix_tokens: Sequence[int] | None = None,
    ) -> None:
        """Record that a backend was routed a prompt (after headers)."""

    def record_cache_result(
        self,
        prompt: str | None,
        backend_id: str,
        cached_tokens: int,
        prefix_tokens: Sequence[int] | None = None,
    ) -> None:
        """Record observed llama.cpp cache evidence."""

    def remove_backend(self, backend_id: str) -> None:
        """Forget a backend; used by dynamic registry changes."""

    def add_backend(self, backend: Backend) -> None:
        """Add a backend without discarding learned routing state."""

    def prefix_stats(self) -> PrefixTrieStats | None:
        """Prefix trie state for metrics; None for non-prefix routers."""


def _eligible(
    backends: Sequence[Backend], healthy_ids: set[str] | None
) -> list[Backend]:
    if healthy_ids is None:
        return list(backends)
    return [backend for backend in backends if backend.id in healthy_ids]


def _pick_candidate(
    candidates: Sequence[Backend],
    session_id: str | None,
    loads: Mapping[str, int] | None,
) -> Backend:
    if session_id:
        return max(
            candidates,
            key=lambda backend: (_score(session_id, backend.id), backend.id),
        )
    if loads is not None:
        return min(
            candidates,
            key=lambda backend: (loads.get(backend.id, 0), backend.id),
        )
    return min(candidates, key=lambda backend: backend.id)


class RoundRobinRouter:
    strategy = ROUND_ROBIN

    def __init__(self, backends: Sequence[Backend]) -> None:
        if not backends:
            raise ValueError("round-robin router needs at least one backend")
        self._backends = tuple(backends)
        self._next = 0

    def decide(
        self,
        session_id: str | None = None,
        healthy_ids: set[str] | None = None,
        prompt: str | None = None,
        loads: Mapping[str, int] | None = None,
        prefix_tokens: Sequence[int] | None = None,
    ) -> RoutingDecision:
        eligible = _eligible(self._backends, healthy_ids)
        if not eligible:
            raise NoHealthyBackend("no healthy backend available")
        start = self._next % len(self._backends)
        self._next += 1
        preferred = self._backends[start]
        eligible_ids = {backend.id for backend in eligible}
        if preferred.id in eligible_ids:
            return RoutingDecision(preferred, preferred.id, False)
        for offset in range(1, len(self._backends) + 1):
            candidate = self._backends[(start + offset) % len(self._backends)]
            if candidate.id in eligible_ids:
                return RoutingDecision(candidate, preferred.id, True)
        raise NoHealthyBackend("no healthy backend available")

    def select(
        self,
        session_id: str | None = None,
        healthy_ids: set[str] | None = None,
        prompt: str | None = None,
        loads: Mapping[str, int] | None = None,
        prefix_tokens: Sequence[int] | None = None,
    ) -> Backend:
        return self.decide(session_id, healthy_ids, prompt, loads, prefix_tokens).backend

    def record_prompt(
        self,
        prompt: str | None,
        backend_id: str,
        prefix_tokens: Sequence[int] | None = None,
    ) -> None:
        return None

    def record_cache_result(
        self,
        prompt: str | None,
        backend_id: str,
        cached_tokens: int,
        prefix_tokens: Sequence[int] | None = None,
    ) -> None:
        return None

    def remove_backend(self, backend_id: str) -> None:
        self._backends = tuple(
            backend for backend in self._backends if backend.id != backend_id
        )

    def add_backend(self, backend: Backend) -> None:
        if any(item.id == backend.id for item in self._backends):
            raise ValueError(f"backend already exists: {backend.id!r}")
        self._backends = self._backends + (backend,)

    def prefix_stats(self) -> PrefixTrieStats | None:
        return None


class SessionAffinityRouter:
    """Rendezvous hashing over ``sha256(backend_id NUL session_id)``."""

    strategy = SESSION_AFFINITY

    def __init__(self, backends: Sequence[Backend]) -> None:
        if not backends:
            raise ValueError("session-affinity router needs at least one backend")
        self._backends = tuple(backends)
        self._fallback = RoundRobinRouter(backends)

    def decide(
        self,
        session_id: str | None = None,
        healthy_ids: set[str] | None = None,
        prompt: str | None = None,
        loads: Mapping[str, int] | None = None,
        prefix_tokens: Sequence[int] | None = None,
    ) -> RoutingDecision:
        if not session_id:
            return self._fallback.decide(None, healthy_ids, prompt, loads)
        eligible = _eligible(self._backends, healthy_ids)
        if not eligible:
            raise NoHealthyBackend("no healthy backend available")
        preferred = self._select_for_key(self._backends, session_id)
        if any(backend.id == preferred.id for backend in eligible):
            return RoutingDecision(preferred, preferred.id, False)
        fallback = self._select_for_key(eligible, session_id)
        return RoutingDecision(fallback, preferred.id, True)

    def select(
        self,
        session_id: str | None = None,
        healthy_ids: set[str] | None = None,
        prompt: str | None = None,
        loads: Mapping[str, int] | None = None,
        prefix_tokens: Sequence[int] | None = None,
    ) -> Backend:
        return self.decide(session_id, healthy_ids, prompt, loads, prefix_tokens).backend

    def record_prompt(
        self,
        prompt: str | None,
        backend_id: str,
        prefix_tokens: Sequence[int] | None = None,
    ) -> None:
        return None

    def record_cache_result(
        self,
        prompt: str | None,
        backend_id: str,
        cached_tokens: int,
        prefix_tokens: Sequence[int] | None = None,
    ) -> None:
        return None

    def remove_backend(self, backend_id: str) -> None:
        self._backends = tuple(
            backend for backend in self._backends if backend.id != backend_id
        )
        self._fallback.remove_backend(backend_id)

    def add_backend(self, backend: Backend) -> None:
        if any(item.id == backend.id for item in self._backends):
            raise ValueError(f"backend already exists: {backend.id!r}")
        self._backends = self._backends + (backend,)
        self._fallback.add_backend(backend)

    def prefix_stats(self) -> PrefixTrieStats | None:
        return None

    @staticmethod
    def _select_for_key(backends: Sequence[Backend], session_id: str) -> Backend:
        return max(
            backends,
            key=lambda backend: (_score(session_id, backend.id), backend.id),
        )

    @classmethod
    def mapping_for(
        cls, backends: Sequence[Backend], session_ids: Sequence[str]
    ) -> dict[str, str]:
        return {sid: cls._select_for_key(backends, sid).id for sid in session_ids}


class PrefixAffinityRouter:
    """Text-prefix affinity backed by a bounded SHA-256 chunk trie."""

    strategy = PREFIX_AFFINITY

    def __init__(
        self,
        backends: Sequence[Backend],
        prefix: PrefixConfig | None = None,
        on_prefix_event: Callable[[str, int], None] | None = None,
    ) -> None:
        if not backends:
            raise ValueError("prefix-affinity router needs at least one backend")
        self._backends = tuple(backends)
        self._prefix = prefix or PrefixConfig()
        self._trie = PrefixHashTrie(
            chunk_chars=self._prefix.chunk_chars,
            max_nodes=self._prefix.max_nodes,
            ttl_seconds=self._prefix.ttl_seconds,
            token_block_size=self._prefix.token_block_size,
            on_evict=on_prefix_event,
        )
        self._fallback = SessionAffinityRouter(backends)

    def _select_with_overflow(
        self,
        eligible: Sequence[Backend],
        matched_ids: frozenset[str],
        verified_ids: frozenset[str],
        session_id: str | None,
        loads: Mapping[str, int] | None,
        *,
        prefer_verified: bool,
    ) -> tuple[Backend, str, bool, bool]:
        """Pick a prefix-matched backend, or overflow to a lighter healthy backend.

        Returns ``(chosen, preferred_id, cache_verified, load_overflow)``.
        """
        threshold = self._prefix.load_overflow_threshold
        all_matched = [b for b in eligible if b.id in matched_ids]
        verified_matched = (
            [b for b in eligible if b.id in verified_ids]
            if prefer_verified and verified_ids
            else []
        )

        if threshold > 0 and loads is not None:
            if verified_matched:
                under_verified = [
                    b for b in verified_matched if loads.get(b.id, 0) < threshold
                ]
                if under_verified:
                    chosen = _pick_candidate(under_verified, session_id, loads)
                    return chosen, chosen.id, True, False
            under_matched = [
                b for b in all_matched if loads.get(b.id, 0) < threshold
            ]
            if under_matched:
                chosen = _pick_candidate(under_matched, session_id, loads)
                return chosen, chosen.id, False, False

            min_matched_load = min(loads.get(b.id, 0) for b in all_matched)
            least_loaded = min(
                eligible, key=lambda b: (loads.get(b.id, 0), b.id)
            )
            if loads.get(least_loaded.id, 0) < min_matched_load:
                preferred = _pick_candidate(
                    verified_matched or all_matched, session_id, loads
                )
                return least_loaded, preferred.id, False, True

        pool = verified_matched if verified_matched else all_matched
        chosen = _pick_candidate(pool, session_id, loads)
        return chosen, chosen.id, bool(verified_matched), False

    def decide(
        self,
        session_id: str | None = None,
        healthy_ids: set[str] | None = None,
        prompt: str | None = None,
        loads: Mapping[str, int] | None = None,
        prefix_tokens: Sequence[int] | None = None,
    ) -> RoutingDecision:
        eligible = _eligible(self._backends, healthy_ids)
        if not eligible:
            raise NoHealthyBackend("no healthy backend available")
        available = {backend.id for backend in eligible}
        token_mode = prefix_tokens is not None
        if token_mode:
            match = self._trie.match_tokens(prefix_tokens, available)
            minimum = self._prefix.min_match_tokens
        else:
            match = self._trie.match(prompt, available)
            minimum = self._prefix.min_match_chars
        if match.length >= minimum and match.backends:
            chosen, preferred_id, _, overflow = self._select_with_overflow(
                eligible,
                match.backends,
                frozenset(),
                session_id,
                loads,
                prefer_verified=False,
            )
            return RoutingDecision(
                chosen,
                preferred_id,
                False,
                prefix_match_chars=0 if token_mode else match.length,
                prefix_match_tokens=match.length if token_mode else 0,
                load_overflow=overflow,
            )
        base = self._fallback.decide(session_id, healthy_ids, prompt, loads)
        return RoutingDecision(
            base.backend,
            base.preferred_backend_id,
            base.failover,
        )

    def select(
        self,
        session_id: str | None = None,
        healthy_ids: set[str] | None = None,
        prompt: str | None = None,
        loads: Mapping[str, int] | None = None,
        prefix_tokens: Sequence[int] | None = None,
    ) -> Backend:
        return self.decide(session_id, healthy_ids, prompt, loads, prefix_tokens).backend

    def record_prompt(
        self,
        prompt: str | None,
        backend_id: str,
        prefix_tokens: Sequence[int] | None = None,
    ) -> None:
        if prefix_tokens is not None:
            self._trie.insert_tokens(prefix_tokens, backend_id)
        else:
            self._trie.insert(prompt, backend_id)

    def record_cache_result(
        self,
        prompt: str | None,
        backend_id: str,
        cached_tokens: int,
        prefix_tokens: Sequence[int] | None = None,
    ) -> None:
        if prefix_tokens is not None:
            self._trie.record_cache_result_tokens(
                prefix_tokens, backend_id, cached_tokens
            )
        else:
            self._trie.record_cache_result(prompt, backend_id, cached_tokens)

    def remove_backend(self, backend_id: str) -> None:
        self._backends = tuple(
            backend for backend in self._backends if backend.id != backend_id
        )
        self._trie.remove_backend(backend_id)
        self._fallback.remove_backend(backend_id)

    def add_backend(self, backend: Backend) -> None:
        if any(item.id == backend.id for item in self._backends):
            raise ValueError(f"backend already exists: {backend.id!r}")
        self._backends = self._backends + (backend,)
        self._fallback.add_backend(backend)

    def prefix_stats(self) -> PrefixTrieStats | None:
        return self._trie.stats()


class KvCacheAwareRouter(PrefixAffinityRouter):
    """Prefix routing that prefers backends with observed cache reuse.

    A textual prefix match is only a candidate.  ``verified`` comes from a
    real llama.cpp ``cache_n > 0`` timing, so it is evidence about this
    gateway's observed cache behavior, not a claim that the gateway can query
    KV-cache block locations.
    """

    strategy = KV_CACHE_AWARE

    def decide(
        self,
        session_id: str | None = None,
        healthy_ids: set[str] | None = None,
        prompt: str | None = None,
        loads: Mapping[str, int] | None = None,
        prefix_tokens: Sequence[int] | None = None,
    ) -> RoutingDecision:
        eligible = _eligible(self._backends, healthy_ids)
        if not eligible:
            raise NoHealthyBackend("no healthy backend available")
        available = {backend.id for backend in eligible}
        token_mode = prefix_tokens is not None
        if token_mode:
            match = self._trie.match_tokens(prefix_tokens, available)
            minimum = self._prefix.min_match_tokens
        else:
            match = self._trie.match(prompt, available)
            minimum = self._prefix.min_match_chars
        if match.length >= minimum and match.backends:
            chosen, preferred_id, cache_verified, overflow = (
                self._select_with_overflow(
                    eligible,
                    match.backends,
                    match.verified,
                    session_id,
                    loads,
                    prefer_verified=True,
                )
            )
            return RoutingDecision(
                chosen,
                preferred_id,
                False,
                prefix_match_chars=0 if token_mode else match.length,
                cache_verified=cache_verified,
                prefix_match_tokens=match.length if token_mode else 0,
                load_overflow=overflow,
            )
        base = self._fallback.decide(session_id, healthy_ids, prompt, loads)
        return RoutingDecision(
            base.backend,
            base.preferred_backend_id,
            base.failover,
        )


def _score(session_id: str, backend_id: str) -> int:
    digest = hashlib.sha256(f"{backend_id}\x00{session_id}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def make_router(
    strategy: str,
    backends: Sequence[Backend],
    prefix: PrefixConfig | None = None,
    on_prefix_event: Callable[[str, int], None] | None = None,
) -> Router:
    if strategy == ROUND_ROBIN:
        return RoundRobinRouter(backends)
    if strategy == SESSION_AFFINITY:
        return SessionAffinityRouter(backends)
    if strategy == PREFIX_AFFINITY:
        return PrefixAffinityRouter(backends, prefix, on_prefix_event)
    if strategy == KV_CACHE_AWARE:
        return KvCacheAwareRouter(backends, prefix, on_prefix_event)
    raise ValueError(f"unknown strategy: {strategy!r}")


def session_fingerprint(session_id: str | None) -> str:
    """Short, non-reversible tag for logs.  Never log the raw session id."""
    if not session_id:
        return "none"
    return hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:8]
