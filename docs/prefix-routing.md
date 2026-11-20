# Prefix and KV-cache-aware routing

## What was added

The gateway now supports two additional routing strategies:

- `prefix_affinity` — hashes the prompt into fixed character chunks with
  SHA-256 and stores a trie of `(prefix chunk -> backend ids)`.  When
  `prefix.tokenizer_url` is set, it instead hashes llama.cpp token ids from
  `/tokenize`.
- `kv_cache_aware` — uses the same trie, but prefers backends for which a real
  llama.cpp `timings.cache_n > 0` observation has marked the prefix as
  *verified*.  A text/token-prefix match alone is not called a KV-cache
  location.

Learned associations carry timestamps.  `prefix.ttl_seconds` expires entries a
backend has not served recently, and the bounded trie evicts least-recently
used nodes instead of dropping all state when `max_nodes` is reached.

Both strategies were implemented independently with the standard library. The
design was informed by mature routers such as
[vLLM Router](https://github.com/vllm-project/production-stack/tree/main/src/vllm_router)
(SHA/xxHash prefix trie, prompt extraction, health filtering); no upstream code
was copied.

## Routing flow

1. `extract_prompt` reads `prompt` for `/completion` and `/v1/completions`,
   joins `messages[*].role/content` for `/v1/chat/completions`, or reads
   `input` for embeddings.
2. If `tokenizer_url` is configured, the prompt is sent to that llama.cpp
   server's `/tokenize` endpoint and token ids become the prefix key.  A
   bounded LRU cache avoids re-tokenizing repeated prompts.  Tokenizer errors
   or timeouts fall back to character chunks for that request.
3. Otherwise the prompt is split into `prefix.chunk_chars` character slices
   (default 128) and each slice is hashed.
4. `PrefixHashTrie.match` / `match_tokens` walks the trie, intersecting the
   backend sets at each matched chunk and stopping when the intersection is
   empty or an association has expired.
5. If the matched length is at least `min_match_chars` (character mode) or
   `min_match_tokens` (token mode) and a matched backend is healthy, the router
   picks among matched backends:
   - session id present: rendezvous hash among candidates;
   - otherwise: lowest current in-flight count, then backend id.
6. `kv_cache_aware` restricts candidates to `verified` backends when any exist;
   otherwise it uses the textual/token candidates to discover cache behavior.
7. If there is no usable match, the router falls back to
   `session_affinity` (or round-robin without a session).

After upstream response headers are received, the gateway records the
prompt/backend association. After a response timing is observed, it records
`cache_n > 0` as verified for that prompt path; a zero-cache observation
removes the verified mark for that path. This means:

- **first request** to a prompt is cold and falls back;
- **second request** with the same prefix is routed back to the same backend;
- once llama.cpp reports a cache hit, later requests prefer that verified
  backend.

This is a local approximation. It does not tokenize with the model tokenizer,
does not inspect KV blocks, and cannot see cache evictions inside llama.cpp.
A character prefix can differ from a token prefix when tokenization crosses a
chunk boundary. The `cache_n` feedback is the only evidence used for the
“verified” label.

## Configuration

```toml
[server]
strategy = "prefix_affinity"   # or "kv_cache_aware"

[prefix]
chunk_chars = 128
min_match_chars = 128
max_nodes = 100000
ttl_seconds = 900              # 0 disables time-based expiry
load_overflow_threshold = 0    # >0 overflows when matched backends reach this active+queued load

# Optional: route on model token ids from llama.cpp /tokenize.
# tokenizer_url = "http://127.0.0.1:18090"
# min_match_tokens = 16
# token_block_size = 16
# tokenizer_timeout_s = 1.0
# tokenizer_cache_entries = 4096
```

The trie is bounded by `max_nodes`; reaching the bound evicts least-recently
used nodes in batches rather than dropping the whole learned state.  The TTL
expires stale backend/prefix associations.  `min_match_chars` /
`min_match_tokens` prevent a short accidental common prefix from dominating
routing.  In token mode, tokens are hashed in fixed blocks of `token_block_size`
(default 16) to keep trie memory bounded.  Setting `load_overflow_threshold > 0`
overflows prefix-matched requests to the least-loaded healthy backend whenever
all matched backends reach that combined active+queued load.  For
`/v1/chat/completions`, `extract_prompt` also serializes `tools`, `tool_calls`,
`name`, and `tool_call_id` deterministically; `/v1/completions` and
`/v1/embeddings` accept both string and string-list inputs.  The tokenizer must
run the same model as the inference backends; tokenization adds one local HTTP
call per uncached prompt, which is why the character mode remains the default.

## Runtime backend registry

The loopback-only admin endpoints are:

```bash
GET    /admin/backends
POST   /admin/backends            {"id":"c","url":"http://127.0.0.1:18092"}
DELETE /admin/backends/{id}
POST   /admin/backends/{id}/drain?timeout_s=30
POST   /admin/backends/{id}/undrain
POST   /admin/health/check
GET    /admin/cache
```

- Additions probe the new backend before it is used.
- Deletion is refused if the backend has active or queued work, or if it is
  the last backend.
- `drain` marks the backend as draining (no new routing), waits for existing
  work, then removes it.  On timeout the endpoint returns 409 and leaves the
  backend draining; `undrain` restores it.
- Adding/removing a backend updates routing incrementally and keeps the
  learned prefix trie for the remaining backends.
- Set `admin.token` or `GATEWAY_ADMIN_TOKEN` to require
  `Authorization: Bearer <token>` in addition to the loopback check.  Without
  a token, any local process can administer the gateway; do not expose the
  admin endpoints to an untrusted network.
- A successful business response or health probe updates health state; failed
  probes/transport errors use the existing debounce thresholds. A single
  business 5xx does not remove a backend.  Health probes use a dedicated HTTP
  client and a total timeout, so a saturated inference pool cannot starve them.
- `GET /admin/cache` summarizes llama.cpp `/slots` cache token counters per
  backend.  It is observability only: `/slots` does not expose a prefix
  identity or KV-block index, so these counters never feed routing.

## Metrics

- `gateway_prefix_routing_total{strategy,result}` — `hit`, `verified`,
  `fallback`.
- `gateway_prefix_match_chars{backend}` — matched character count.
- `gateway_prefix_match_tokens{backend}` — matched token count when tokenizer
  mode is enabled.
- `gateway_cache_evidence_total{backend,result}` — observed `cache_n` hit/miss.
- `gateway_prefix_trie_nodes` and `gateway_prefix_verified_entries` — current
  trie size and verified associations.
- `gateway_prefix_evictions_total{reason}` — `ttl`, `lru` or `reset`.
- `gateway_retry_prefix_preserved_total{strategy}` — a retry that landed on a
  backend holding the same prefix.

The metrics intentionally avoid prompt text and session ids.

## Verification

Unit tests: `tests/unit/test_prefix.py`.
HTTP tests: `tests/integration/test_prefix_routing_http.py`,
`tests/integration/test_hardening.py`.
Admin tests: `tests/integration/test_admin_backends.py`.
Real llama.cpp tests: `tests/integration/test_real_backend.py` includes a
tokenizer-prefix case that calls `/tokenize` on a live backend.

A real 5-repeat comparison with two llama.cpp backends, no session IDs, seven
exact long prompts × four rounds:

| Strategy | Cache hit | First-content p50 | Completed |
|---|---:|---:|---:|
| `b0` round-robin | 50.229% | 258.66 ms | 140/140 |
| `p1` prefix affinity | 74.938% | 9.93 ms | 140/140 |
| `kv` cache-aware | 74.938% | 9.71 ms | 140/140 |

Cache hit and routing counts were identical in all five repeats. This is a
mechanism result, not a production performance claim. On this two-backend,
seven-prefix workload, cached rounds were already retained by the backend that
saw the prefix; `prefix_affinity` prevented round-robin from alternating them.
`kv_cache_aware` matched `prefix_affinity` because the trie had already
verified the same backend. A larger prefix set, more backends, cache eviction,
or concurrency would change the comparison.

## Limitations

- Character chunks are not model tokens; tokenization boundaries may differ.
  Token mode removes this approximation but requires a reachable llama.cpp
  `/tokenize` server and adds latency for uncached prompts.
- The trie only knows what this gateway routed; it cannot observe cache state
  created by another client directly against a backend.
- `cache_n == 0` is treated as a miss for that prompt path; llama.cpp may still
  hold other prefixes.
- Cache feedback is per prompt path, not per KV block.  llama.cpp `/slots`
  exposes cache token counts, not a prefix identity or KV-block index, so the
  gateway does not pretend to query cache locations.
- TTL and LRU bound memory and stale affinity, but eviction can remove a prefix
  the backend still has cached; that only loses an optimization.
- Immediate failback after recovery can still lose the cache.
- Safe retries preserve a prefix only when another healthy backend already has
  the same prefix in the trie; read timeouts and post-header failures are never
  retried.
- No prefix-aware routing across disaggregated prefill/decode or LMCache/KV
  controllers.
