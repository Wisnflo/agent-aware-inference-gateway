"""Optional model-tokenizer prefix keys via llama.cpp ``/tokenize``.

Character chunks are always available and need no network call.  When
``prefix.tokenizer_url`` is configured, the gateway asks that llama.cpp server
for token ids and routes on the token prefix instead.  The tokenizer must run
the same model as the inference backends; this is a routing aid, not a
verification of KV-cache contents.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections import OrderedDict

import httpx

logger = logging.getLogger("gateway.tokenizer")

MAX_CACHED_TOKENS = 8192


class LlamaTokenizer:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_s: float = 1.0,
        cache_entries: int = 4096,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if cache_entries < 0:
            raise ValueError("cache_entries must not be negative")
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.cache_entries = cache_entries
        self._client = client or httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(
                connect=timeout_s, read=timeout_s, write=timeout_s, pool=timeout_s
            ),
            follow_redirects=False,
        )
        self._owns_client = client is None
        self._cache: OrderedDict[bytes, tuple[int, ...]] = OrderedDict()
        self._inflight: dict[bytes, asyncio.Task[tuple[int, ...] | None]] = {}

    async def _fetch_tokens(self, text: str) -> tuple[int, ...] | None:
        try:
            async with asyncio.timeout(self.timeout_s):
                response = await self._client.post(
                    "/tokenize",
                    json={"content": text, "add_special": False},
                )
                if response.status_code != 200:
                    logger.warning(
                        "tokenizer returned status %d; using character prefixes",
                        response.status_code,
                    )
                    return None
                payload = response.json()
        except (TimeoutError, httpx.HTTPError, ValueError) as exc:
            logger.warning("tokenizer unavailable; using character prefixes: %s", exc)
            return None
        tokens = payload.get("tokens") if isinstance(payload, dict) else None
        if (
            not isinstance(tokens, list)
            or not tokens
            or not all(
                isinstance(token, int)
                and not isinstance(token, bool)
                and token >= 0
                for token in tokens
            )
        ):
            return None
        return tuple(tokens[:MAX_CACHED_TOKENS])

    async def tokenize(
        self, text: str, remaining_s: float | None = None
    ) -> tuple[int, ...] | None:
        """Return token ids, or None when tokenization is unavailable.

        Failure is expected and non-fatal: callers fall back to character
        chunks, so a slow or old tokenizer cannot take routing down.
        """
        if not text:
            return None
        key = hashlib.sha256(text.encode("utf-8")).digest()
        cached = self._cache.get(key)
        if cached is not None:
            self._cache.move_to_end(key)
            return cached

        timeout = self.timeout_s
        if remaining_s is not None:
            timeout = min(timeout, remaining_s)
        if timeout <= 0:
            return None

        task = self._inflight.get(key)
        if task is None:
            task = asyncio.create_task(self._fetch_tokens(text))
            self._inflight[key] = task

            def _cleanup(done_task: asyncio.Task[tuple[int, ...] | None]) -> None:
                if self._inflight.get(key) is done_task:
                    del self._inflight[key]
                if not done_task.cancelled() and done_task.exception() is None:
                    res = done_task.result()
                    if res is not None and self.cache_entries > 0:
                        self._cache[key] = res
                        self._cache.move_to_end(key)
                        while len(self._cache) > self.cache_entries:
                            self._cache.popitem(last=False)

            task.add_done_callback(_cleanup)

        try:
            async with asyncio.timeout(timeout):
                return await asyncio.shield(task)
        except TimeoutError:
            logger.warning("tokenizer request timed out; using character prefixes")
            return None

    async def aclose(self) -> None:
        inflight = list(self._inflight.values())
        for task in inflight:
            task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
        self._inflight.clear()
        if self._owns_client:
            await self._client.aclose()
