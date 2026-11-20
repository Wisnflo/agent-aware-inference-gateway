"""Prompt-prefix hashing and a bounded prefix trie.

The trie is a local approximation of prefix-cache locality.  It hashes fixed
character chunks with SHA-256 and records which backends have served each
prefix.  It does not claim to know token boundaries, KV blocks, or llama.cpp's
internal cache state; cache-hit feedback is recorded separately as evidence.

Associations carry a timestamp.  ``ttl_seconds`` expires prefixes a backend has
not been observed serving, and reaching ``max_nodes`` triggers LRU eviction
instead of dropping the whole learned state.  The design is inspired by mature
prefix-aware routers (for example vLLM Router's HashTrie), but this
implementation is independent and uses only the standard library.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Sequence

logger = logging.getLogger("gateway.prefix")

MAX_PROMPT_CHARS = 65536
LRU_TARGET_FRACTION = 0.8
TOKEN_KEY_NAMESPACE = 1 << 64


@dataclass(frozen=True)
class PrefixMatch:
    length: int
    backends: frozenset[str]
    verified: frozenset[str]


@dataclass(frozen=True)
class PrefixTrieStats:
    nodes: int
    verified_entries: int
    ttl_evictions: int
    lru_evictions: int
    resets: int


@dataclass
class _Node:
    children: dict[int, "_Node"] = field(default_factory=dict)
    # Backends that have been routed a request with this prefix, and when each
    # association was last observed.
    backends: dict[str, float] = field(default_factory=dict)
    # Subset of ``backends`` for which a llama.cpp cache hit was observed.
    verified: dict[str, float] = field(default_factory=dict)
    # Logical access counter used for LRU even when the wall clock has not
    # advanced between two operations.
    last_used: int = 0


EvictCallback = Callable[[str, int], None]


class PrefixHashTrie:
    """Prefix index keyed by SHA-256 of fixed character chunks.

    Every node stores the backends that have seen that exact prefix and the
    time of the last observation.  ``verified`` is populated only from observed
    ``timings.cache_n > 0`` feedback, so it is evidence of real cache reuse
    rather than an assumption.
    """

    def __init__(
        self,
        chunk_chars: int = 128,
        max_nodes: int = 100_000,
        ttl_seconds: float = 0.0,
        *,
        token_block_size: int = 16,
        clock: Callable[[], float] = time.monotonic,
        on_evict: EvictCallback | None = None,
    ) -> None:
        if chunk_chars < 1:
            raise ValueError("chunk_chars must be positive")
        if max_nodes < 1:
            raise ValueError("max_nodes must be positive")
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must not be negative")
        if token_block_size < 1:
            raise ValueError("token_block_size must be positive")
        self.chunk_chars = chunk_chars
        self.max_nodes = max_nodes
        self.ttl_seconds = ttl_seconds
        self.token_block_size = token_block_size
        self._clock = clock
        self._on_evict = on_evict
        self._root = _Node()
        self._node_count = 1
        self._logical_clock = 0
        self._ttl_evictions = 0
        self._lru_evictions = 0
        self._resets = 0

    @property
    def node_count(self) -> int:
        return self._node_count

    @property
    def ttl_seconds_enabled(self) -> bool:
        return self.ttl_seconds > 0

    def stats(self) -> PrefixTrieStats:
        verified = 0
        now = self._clock()
        stack = [self._root]
        while stack:
            node = stack.pop()
            if self.ttl_seconds_enabled:
                verified += sum(
                    1 for ts in node.verified.values() if self._fresh(ts, now)
                )
            else:
                verified += len(node.verified)
            stack.extend(node.children.values())
        return PrefixTrieStats(
            nodes=self._node_count,
            verified_entries=verified,
            ttl_evictions=self._ttl_evictions,
            lru_evictions=self._lru_evictions,
            resets=self._resets,
        )

    def _fresh(self, timestamp: float, now: float) -> bool:
        return not self.ttl_seconds_enabled or now - timestamp <= self.ttl_seconds

    def _next_tick(self) -> int:
        self._logical_clock += 1
        return self._logical_clock

    def _touch(
        self, node: _Node, backend_id: str, now: float, tick: int | None = None
    ) -> None:
        node.backends[backend_id] = now
        node.last_used = self._next_tick() if tick is None else tick

    def _chunks(self, prompt: str) -> list[tuple[int, int]]:
        text = prompt[:MAX_PROMPT_CHARS]
        return [
            (
                int.from_bytes(
                    hashlib.sha256(
                        text[offset : offset + self.chunk_chars].encode("utf-8")
                    ).digest()[:8],
                    "big",
                ),
                len(text[offset : offset + self.chunk_chars]),
            )
            for offset in range(0, len(text), self.chunk_chars)
        ]

    def insert(self, prompt: str | None, backend_id: str) -> None:
        if not prompt:
            return
        self._insert(self._chunks(prompt), backend_id)

    def insert_tokens(self, tokens: Sequence[int], backend_id: str) -> None:
        """Insert a model-token prefix (token ids from a tokenizer)."""
        if not tokens:
            return
        self._insert(self._token_chunks(tokens), backend_id)

    def _insert(self, chunks: list[tuple[int, int]], backend_id: str) -> None:
        if not chunks or self.max_nodes < 2:
            return
        self._make_room()
        now = self._clock()
        tick = self._next_tick()
        self._touch(self._root, backend_id, now, tick)
        node = self._root
        for key, _ in chunks:
            child = node.children.get(key)
            if child is None:
                if self._node_count >= self.max_nodes:
                    # Do not evict during traversal: the current path could be
                    # removed, detaching the nodes we are still building.
                    break
                child = _Node()
                node.children[key] = child
                self._node_count += 1
            self._touch(child, backend_id, now, tick)
            node = child

    def record_cache_result(
        self, prompt: str | None, backend_id: str, cached_tokens: int
    ) -> None:
        """Record observed cache evidence for this prompt/backend.

        A hit marks every prefix of the prompt as verified for the backend.
        A miss removes the verified mark for this exact path, because the
        backend did not reuse the prefix on this observation.
        """
        if not prompt:
            return
        self._record_cache_result(self._chunks(prompt), backend_id, cached_tokens)

    def record_cache_result_tokens(
        self, tokens: Sequence[int], backend_id: str, cached_tokens: int
    ) -> None:
        if not tokens:
            return
        self._record_cache_result(
            self._token_chunks(tokens), backend_id, cached_tokens
        )

    def _record_cache_result(
        self, chunks: list[tuple[int, int]], backend_id: str, cached_tokens: int
    ) -> None:
        node = self._root
        path = [node]
        for key, _ in chunks:
            child = node.children.get(key)
            if child is None:
                break
            path.append(child)
            node = child
        if len(path) == 1:
            return
        now = self._clock()
        tick = self._next_tick()
        for item in path:
            if backend_id not in item.backends:
                continue
            self._touch(item, backend_id, now, tick)
            if cached_tokens > 0:
                item.verified[backend_id] = now
            else:
                item.verified.pop(backend_id, None)

    def match(self, prompt: str | None, available: set[str]) -> PrefixMatch:
        if not prompt:
            return PrefixMatch(0, frozenset(), frozenset())
        return self._match(self._chunks(prompt), available)

    def match_tokens(
        self, tokens: Sequence[int], available: set[str]
    ) -> PrefixMatch:
        if not tokens:
            return PrefixMatch(0, frozenset(), frozenset())
        return self._match(self._token_chunks(tokens), available)

    def _match(
        self, chunks: list[tuple[int, int]], available: set[str]
    ) -> PrefixMatch:
        if not available:
            return PrefixMatch(0, frozenset(), frozenset())
        now = self._clock()
        tick = self._next_tick()
        candidates = set(available)
        node = self._root
        length = 0
        matched: set[str] = set()
        verified: set[str] = set()
        for key, chunk_length in chunks:
            child = node.children.get(key)
            if child is None:
                break
            candidates = {
                backend_id
                for backend_id in candidates
                if backend_id in child.backends
                and self._fresh(child.backends[backend_id], now)
            }
            if not candidates:
                break
            # Update LRU recency without extending per-backend TTLs; TTL is
            # refreshed only when a backend is actually observed serving the
            # prefix in insert() or record_cache_result().
            child.last_used = tick
            node = child
            length += chunk_length
            matched = set(candidates)
            verified = {
                backend_id
                for backend_id in candidates
                if backend_id in child.verified
                and self._fresh(child.verified[backend_id], now)
            }
        return PrefixMatch(
            length,
            frozenset(matched),
            frozenset(verified),
        )

    def _token_chunks(self, tokens: Sequence[int]) -> list[tuple[int, int]]:
        # Hash fixed-size token blocks with SHA-256 and namespace keys above
        # the 64-bit character-chunk hash space so the two modes never collide.
        result: list[tuple[int, int]] = []
        step = self.token_block_size
        for offset in range(0, len(tokens), step):
            block = tokens[offset : offset + step]
            hasher = hashlib.sha256()
            for token in block:
                token_id = int(token)
                if token_id < 0:
                    raise ValueError("token id must be non-negative")
                hasher.update(token_id.to_bytes(8, "big", signed=False))
            key = TOKEN_KEY_NAMESPACE + int.from_bytes(hasher.digest()[:8], "big")
            result.append((key, len(block)))
        return result

    def remove_backend(self, backend_id: str) -> None:
        nodes = self._walk_nodes()
        for node in nodes:
            node.backends.pop(backend_id, None)
            node.verified.pop(backend_id, None)
        self._prune_empty(nodes)

    def _walk_nodes(self) -> list[_Node]:
        """Return every node in pre-order without recursion."""
        nodes: list[_Node] = []
        stack = [self._root]
        while stack:
            node = stack.pop()
            nodes.append(node)
            stack.extend(node.children.values())
        return nodes

    def _prune_empty(self, nodes: list[_Node]) -> None:
        for node in reversed(nodes):
            for key in list(node.children):
                child = node.children[key]
                if not child.children and not child.backends:
                    del node.children[key]
                    self._node_count -= 1

    def clear(self) -> None:
        self._root = _Node()
        self._node_count = 1

    # ------------------------------------------------------------------
    # bounded growth
    # ------------------------------------------------------------------
    def _make_room(self) -> None:
        if self._node_count < self.max_nodes:
            return
        before = self._node_count
        self._evict_expired()
        if self._node_count < self.max_nodes:
            return
        self._evict_lru()
        if self._node_count < self.max_nodes:
            return
        if self._node_count == before:
            # A single-node trie cannot make room for a child; keep the bound
            # rather than growing without limit.
            logger.warning(
                "prefix trie cannot evict below max_nodes=%d; resetting",
                self.max_nodes,
            )
            self.clear()
            self._resets += 1
            self._notify_evict("reset", 1)

    def _evict_expired(self) -> None:
        if not self.ttl_seconds_enabled:
            return
        now = self._clock()
        nodes = self._walk_nodes()
        removed = 0
        for node in nodes:
            stale = [
                backend_id
                for backend_id, timestamp in node.backends.items()
                if not self._fresh(timestamp, now)
            ]
            for backend_id in stale:
                node.backends.pop(backend_id, None)
                node.verified.pop(backend_id, None)
                removed += 1
        self._prune_empty(nodes)
        if removed:
            self._ttl_evictions += removed
            self._notify_evict("ttl", removed)

    def _evict_lru(self) -> None:
        target = max(1, int(self.max_nodes * LRU_TARGET_FRACTION))
        # Track parent links and depth iteratively, then prune oldest leaves
        # with a min-heap so newly emptied ancestors cascade in the same pass.
        parent_of: dict[int, tuple[_Node, int, int]] = {}
        heap: list[tuple[int, int, int, _Node, int, _Node]] = []
        seq = 0
        stack: list[tuple[_Node, int]] = [(self._root, 0)]
        while stack:
            parent, depth = stack.pop()
            for key, child in parent.children.items():
                child_depth = depth + 1
                parent_of[id(child)] = (parent, key, child_depth)
                if child.children:
                    stack.append((child, child_depth))
                else:
                    seq += 1
                    heapq.heappush(
                        heap, (child.last_used, -child_depth, seq, parent, key, child)
                    )

        removed = 0
        while self._node_count > target and heap:
            _, _, _, parent, key, child = heapq.heappop(heap)
            if child.children or parent.children.get(key) is not child:
                continue
            del parent.children[key]
            self._node_count -= 1
            removed += 1
            if parent is not self._root and not parent.children:
                grandparent_info = parent_of.get(id(parent))
                if grandparent_info is not None:
                    grandparent, parent_key, parent_depth = grandparent_info
                    seq += 1
                    heapq.heappush(
                        heap,
                        (
                            parent.last_used,
                            -parent_depth,
                            seq,
                            grandparent,
                            parent_key,
                            parent,
                        ),
                    )
        if removed:
            self._lru_evictions += removed
            self._notify_evict("lru", removed)

    def _notify_evict(self, reason: str, count: int) -> None:
        if self._on_evict is None:
            return
        try:
            self._on_evict(reason, count)
        except Exception:  # pragma: no cover - metrics must not break routing
            logger.exception("prefix eviction callback failed")


def _extract_text_or_str_list(value: object) -> str:
    if isinstance(value, str):
        return value[:MAX_PROMPT_CHARS]
    if isinstance(value, list):
        items = [item for item in value if isinstance(item, str) and item]
        if items:
            return "\n".join(items)[:MAX_PROMPT_CHARS]
    return ""


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def extract_prompt(route: str, body: bytes) -> str:
    """Extract a deterministic textual prefix from a supported request body."""
    if not body:
        return ""
    try:
        data = json.loads(body)
    except (ValueError, RecursionError, UnicodeDecodeError):
        return ""
    if not isinstance(data, dict):
        return ""

    if route in ("completion", "v1_completions"):
        return _extract_text_or_str_list(data.get("prompt"))

    if route == "chat_completions":
        parts: list[str] = []
        tools = data.get("tools")
        if isinstance(tools, list) and tools:
            parts.append(f"tools: {_canonical_json(tools)}")

        messages = data.get("messages")
        if isinstance(messages, list):
            for message in messages:
                if not isinstance(message, dict):
                    continue
                role = message.get("role")
                role_label = role if isinstance(role, str) and role else "unknown"
                tool_call_id = message.get("tool_call_id")
                name = message.get("name")
                if isinstance(tool_call_id, str) and tool_call_id:
                    header = f"{role_label}[{tool_call_id}]"
                elif isinstance(name, str) and name:
                    header = f"{role_label}({name})"
                else:
                    header = role_label

                content = message.get("content")
                if isinstance(content, str):
                    text = content
                elif isinstance(content, list):
                    text = " ".join(
                        item.get("text", "")
                        for item in content
                        if isinstance(item, dict) and isinstance(item.get("text"), str)
                    )
                else:
                    text = ""

                segments: list[str] = []
                if text:
                    segments.append(text)
                tool_calls = message.get("tool_calls")
                if isinstance(tool_calls, list) and tool_calls:
                    segments.append(f"tool_calls={_canonical_json(tool_calls)}")

                if segments:
                    parts.append(f"{header}: {' '.join(segments)}")
        if parts:
            return "\n".join(parts)[:MAX_PROMPT_CHARS]

    for key in ("prompt", "state", "input"):
        extracted = _extract_text_or_str_list(data.get(key))
        if extracted:
            return extracted
    return ""
