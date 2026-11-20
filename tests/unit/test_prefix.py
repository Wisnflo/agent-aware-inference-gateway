"""Tests for prompt extraction and the prefix hash trie."""

import json

from gateway.backends import Backend
from gateway.config import PrefixConfig
from gateway.prefix import PrefixHashTrie, extract_prompt
from gateway.router import (
    KvCacheAwareRouter,
    PrefixAffinityRouter,
    RoundRobinRouter,
    SessionAffinityRouter,
)


def make_backends(*ids):
    return [
        Backend(id=backend_id, base_url=f"http://127.0.0.1:{9000 + index}")
        for index, backend_id in enumerate(ids)
    ]


def test_extract_completion_prompt():
    body = json.dumps({"prompt": "hello shared prefix", "n_predict": 1}).encode()
    assert extract_prompt("completion", body) == "hello shared prefix"


def test_extract_chat_messages_including_text_parts():
    body = json.dumps(
        {
            "messages": [
                {"role": "system", "content": "rules"},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "first"},
                        {"type": "image_url", "image_url": {"url": "x"}},
                        {"type": "text", "text": "second"},
                    ],
                },
            ]
        }
    ).encode()
    assert extract_prompt("chat_completions", body) == (
        "system: rules\nuser: first second"
    )


def test_extract_invalid_or_empty_body():
    assert extract_prompt("completion", b"not-json") == ""
    assert extract_prompt("completion", b"{}") == ""
    assert extract_prompt("completion", b'{"prompt": 42}') == ""


def test_trie_longest_prefix_and_shared_prefix():
    trie = PrefixHashTrie(chunk_chars=3)
    trie.insert("abcdef", "a")
    trie.insert("abcxyz", "b")

    match = trie.match("abcdef", {"a", "b"})
    assert match.length == 6
    assert match.backends == {"a"}

    shared = trie.match("abcdifferent", {"a", "b"})
    assert shared.length == 3
    assert shared.backends == {"a", "b"}


def test_trie_health_filter_and_no_match():
    trie = PrefixHashTrie(chunk_chars=4)
    trie.insert("shared prefix", "a")
    assert trie.match("shared prefix", {"b"}) == type(trie.match("", set()))(
        0, frozenset(), frozenset()
    )
    trie.remove_backend("a")
    assert trie.match("shared prefix", {"a"}).length == 0
    assert trie.node_count == 1


def test_trie_cache_evidence_marks_and_unmarks_verified():
    trie = PrefixHashTrie(chunk_chars=4)
    trie.insert("shared prefix one", "a")
    assert trie.match("shared prefix one", {"a"}).verified == frozenset()
    trie.record_cache_result("shared prefix one", "a", 10)
    match = trie.match("shared prefix one", {"a"})
    assert match.verified == {"a"}
    trie.record_cache_result("shared prefix one", "a", 0)
    assert trie.match("shared prefix one", {"a"}).verified == frozenset()


def test_prefix_affinity_routes_repeated_prompt_to_same_backend():
    backends = make_backends("a", "b")
    router = PrefixAffinityRouter(
        backends, PrefixConfig(chunk_chars=8, min_match_chars=8)
    )
    prompt = "a shared long prefix for a repeated request"
    first = router.decide(None, {"a", "b"}, prompt=prompt)
    router.record_prompt(prompt, first.backend.id)
    second = router.decide(None, {"a", "b"}, prompt=prompt)
    assert second.backend.id == first.backend.id
    assert second.prefix_match_chars >= 8


def test_prefix_affinity_falls_back_when_match_is_too_short():
    backends = make_backends("a", "b")
    router = PrefixAffinityRouter(
        backends, PrefixConfig(chunk_chars=8, min_match_chars=64)
    )
    router.record_prompt("short", "a")
    decision = router.decide(None, {"a", "b"}, prompt="short")
    assert decision.prefix_match_chars == 0


def test_prefix_affinity_respects_health():
    backends = make_backends("a", "b")
    router = PrefixAffinityRouter(
        backends, PrefixConfig(chunk_chars=8, min_match_chars=8)
    )
    prompt = "prefix only served by backend a"
    router.record_prompt(prompt, "a")
    decision = router.decide(None, {"b"}, prompt=prompt)
    assert decision.backend.id == "b"
    assert decision.prefix_match_chars == 0


def test_prefix_affinity_tie_breaks_by_load_then_id():
    backends = make_backends("a", "b")
    router = PrefixAffinityRouter(
        backends, PrefixConfig(chunk_chars=8, min_match_chars=8)
    )
    prompt = "same prefix both backends"
    router.record_prompt(prompt, "a")
    router.record_prompt(prompt, "b")
    decision = router.decide(
        None, {"a", "b"}, prompt=prompt, loads={"a": 5, "b": 0}
    )
    assert decision.backend.id == "b"


def test_kv_aware_prefers_verified_backend():
    backends = make_backends("a", "b")
    router = KvCacheAwareRouter(
        backends, PrefixConfig(chunk_chars=8, min_match_chars=8)
    )
    prompt = "verified cache prefix"
    router.record_prompt(prompt, "a")
    router.record_prompt(prompt, "b")
    router.record_cache_result(prompt, "b", 42)
    decision = router.decide(None, {"a", "b"}, prompt=prompt)
    assert decision.backend.id == "b"
    assert decision.cache_verified is True


def test_kv_aware_uses_prefix_before_any_verified_hit():
    backends = make_backends("a", "b")
    router = KvCacheAwareRouter(
        backends, PrefixConfig(chunk_chars=8, min_match_chars=8)
    )
    prompt = "not yet verified prefix"
    router.record_prompt(prompt, "a")
    decision = router.decide(None, {"a", "b"}, prompt=prompt)
    assert decision.backend.id == "a"
    assert decision.cache_verified is False


def test_session_affinity_unchanged_by_prompt_argument():
    backends = make_backends("a", "b")
    router = SessionAffinityRouter(backends)
    first = router.decide("s1", {"a", "b"}, prompt="x")
    second = router.decide("s1", {"a", "b"}, prompt="y")
    assert first.backend.id == second.backend.id


def test_trie_ttl_expires_unused_associations():
    now = [100.0]
    trie = PrefixHashTrie(chunk_chars=4, ttl_seconds=10, clock=lambda: now[0])
    trie.insert("shared prefix", "a")
    assert trie.match("shared prefix", {"a"}).length == 13
    now[0] += 11
    assert trie.match("shared prefix", {"a"}).length == 0
    # A new insert opportunistically drops the expired state.
    trie.insert("another prefix", "b")
    assert trie.node_count <= trie.max_nodes


def test_trie_lru_evicts_without_reset_and_keeps_bound():
    trie = PrefixHashTrie(chunk_chars=1, max_nodes=6, ttl_seconds=0)
    trie.insert("abcdef", "a")
    trie.insert("uvwxyz", "b")
    assert trie.node_count <= trie.max_nodes
    stats = trie.stats()
    assert stats.lru_evictions > 0
    assert stats.resets == 0
    # The newest prefix is still available.
    assert trie.match("uvwxyz", {"b"}).length >= 1


def test_trie_eviction_callback_reports_reason():
    events = []
    trie = PrefixHashTrie(
        chunk_chars=1,
        max_nodes=4,
        ttl_seconds=0,
        on_evict=lambda reason, count: events.append((reason, count)),
    )
    for index in range(8):
        trie.insert(f"prompt-{index}", "a")
    assert any(reason == "lru" for reason, _ in events)
    assert trie.node_count <= trie.max_nodes


def test_trie_token_prefix_matching_and_cache_evidence():
    trie = PrefixHashTrie(chunk_chars=8, token_block_size=1)
    trie.insert_tokens([11, 12, 13, 14], "a")
    match = trie.match_tokens([11, 12, 99], {"a"})
    assert match.length == 2
    assert match.backends == {"a"}
    trie.record_cache_result_tokens([11, 12, 13, 14], "a", 5)
    verified = trie.match_tokens([11, 12, 13, 14], {"a"})
    assert verified.length == 4
    assert verified.verified == {"a"}


def test_trie_deep_chain_remove_and_ttl_are_iterative():
    trie = PrefixHashTrie(chunk_chars=1, max_nodes=100_000, ttl_seconds=1)
    trie.insert("x" * 5000, "a")
    assert trie.node_count > 1000
    trie.remove_backend("a")
    assert trie.node_count == 1


def test_prefix_router_uses_token_prefix_when_supplied():
    backends = make_backends("a", "b")
    router = PrefixAffinityRouter(
        backends,
        PrefixConfig(chunk_chars=8, min_match_chars=8, min_match_tokens=3),
    )
    tokens = (101, 102, 103, 104)
    router.record_prompt("ignored text", "a", prefix_tokens=tokens)
    decision = router.decide(
        None, {"a", "b"}, prompt="anything", prefix_tokens=tokens
    )
    assert decision.backend.id == "a"
    assert decision.prefix_match_tokens == 4
    assert decision.prefix_match_chars == 0


def test_prefix_router_add_and_remove_backend_preserves_prefix_state():
    a = make_backends("a")[0]
    b = make_backends("b")[0]
    router = PrefixAffinityRouter([a], PrefixConfig(chunk_chars=8, min_match_chars=8))
    prompt = "a learned prefix survives registry updates"
    router.record_prompt(prompt, "a")

    router.add_backend(b)
    decision = router.decide(None, {"a", "b"}, prompt=prompt)
    assert decision.backend.id == "a"
    assert decision.prefix_match_chars >= 8

    router.remove_backend("a")
    decision = router.decide(None, {"b"}, prompt=prompt)
    assert decision.backend.id == "b"
    assert decision.prefix_match_chars == 0
    stats = router.prefix_stats()
    assert stats is not None
    assert stats.nodes >= 1


def test_round_robin_and_session_routers_support_incremental_add():
    a, b = make_backends("a", "b")
    for router in (RoundRobinRouter([a]), SessionAffinityRouter([a])):
        router.add_backend(b)
        seen = {router.decide(None, {"a", "b"}).backend.id for _ in range(8)}
        assert seen == {"a", "b"}
        router.remove_backend("b")
        assert router.decide(None, {"a"}).backend.id == "a"


def test_trie_lru_cascades_deep_chain_to_target_in_single_eviction():
    # One 10-node chain fills max_nodes=11 (1 root + 10 nodes).
    # Triggering eviction once must cascade through ancestors down to target (8),
    # not just remove a single leaf.
    trie = PrefixHashTrie(chunk_chars=1, max_nodes=11, ttl_seconds=0)
    trie.insert("abcdefghij", "a")
    assert trie.node_count == 11

    trie._evict_lru()
    assert trie.node_count <= int(11 * 0.8)
    assert trie.stats().lru_evictions >= 3
    assert trie.stats().resets == 0


def test_trie_record_cache_result_marks_partially_inserted_prefix():
    trie = PrefixHashTrie(chunk_chars=1, max_nodes=5, ttl_seconds=0)
    # Only 4 chars fit (1 root + 4 nodes); remaining chars are truncated.
    trie.insert("abcdefgh", "a")
    assert trie.node_count == 5

    trie.record_cache_result("abcdefgh", "a", 42)
    match = trie.match("abcd", {"a"})
    assert match.length == 4
    assert match.verified == {"a"}


def test_trie_match_does_not_extend_backend_ttl():
    now = [100.0]
    trie = PrefixHashTrie(chunk_chars=4, ttl_seconds=10, clock=lambda: now[0])
    trie.insert("shared prefix", "a")

    now[0] = 108.0
    assert trie.match("shared prefix", {"a"}).length == 13

    # At t=111 (11s after insert), backend 'a' must expire even though match() ran at t=108.
    now[0] = 111.0
    assert trie.match("shared prefix", {"a"}).length == 0


def test_trie_rejects_negative_token_ids():
    import pytest

    trie = PrefixHashTrie(chunk_chars=4)
    with pytest.raises(ValueError, match="non-negative"):
        trie.insert_tokens([10, -1, 12], "a")


def test_prefix_affinity_overflows_when_matched_backend_overloaded():
    backends = make_backends("a", "b")
    router = PrefixAffinityRouter(
        backends,
        PrefixConfig(chunk_chars=8, min_match_chars=8, load_overflow_threshold=3),
    )
    prompt = "shared long prefix on overloaded backend"
    router.record_prompt(prompt, "a")

    # Under threshold: stays on prefix-matched backend 'a'.
    under = router.decide(None, {"a", "b"}, prompt=prompt, loads={"a": 2, "b": 0})
    assert under.backend.id == "a"
    assert under.load_overflow is False

    # At threshold with lighter healthy backend 'b': overflows to 'b'.
    over = router.decide(None, {"a", "b"}, prompt=prompt, loads={"a": 3, "b": 1})
    assert over.backend.id == "b"
    assert over.preferred_backend_id == "a"
    assert over.load_overflow is True

    # All healthy backends equally loaded: do not overflow, keep prefix affinity.
    equal = router.decide(None, {"a", "b"}, prompt=prompt, loads={"a": 3, "b": 3})
    assert equal.backend.id == "a"
    assert equal.load_overflow is False


def test_kv_aware_prefers_unloaded_matched_backend_before_overflowing():
    backends = make_backends("a", "b", "c")
    router = KvCacheAwareRouter(
        backends,
        PrefixConfig(chunk_chars=8, min_match_chars=8, load_overflow_threshold=3),
    )
    prompt = "verified on a, also seen on b, never on c"
    router.record_prompt(prompt, "a")
    router.record_cache_result(prompt, "a", 20)
    router.record_prompt(prompt, "b")

    # Verified 'a' is overloaded, unverified matched 'b' is under threshold -> pick 'b' without overflow.
    fallback_matched = router.decide(
        None, {"a", "b", "c"}, prompt=prompt, loads={"a": 4, "b": 1, "c": 0}
    )
    assert fallback_matched.backend.id == "b"
    assert fallback_matched.cache_verified is False
    assert fallback_matched.load_overflow is False

    # Both matched backends 'a' and 'b' are overloaded -> overflow to 'c'.
    overflow = router.decide(
        None, {"a", "b", "c"}, prompt=prompt, loads={"a": 4, "b": 3, "c": 1}
    )
    assert overflow.backend.id == "c"
    assert overflow.preferred_backend_id == "a"
    assert overflow.load_overflow is True
    assert overflow.cache_verified is False


def test_trie_token_block_hashing_reduces_nodes_and_matches_blocks():
    trie = PrefixHashTrie(chunk_chars=8, token_block_size=16)
    tokens_a = list(range(1, 65))  # 64 tokens -> 4 blocks + 1 root = 5 nodes
    trie.insert_tokens(tokens_a, "a")
    assert trie.node_count == 5

    # First 2 blocks (32 tokens) match; 3rd block diverges.
    tokens_query = list(range(1, 33)) + [999] * 16
    match = trie.match_tokens(tokens_query, {"a"})
    assert match.length == 32
    assert match.backends == {"a"}

    # Exact match across all 64 tokens with cache verification.
    trie.record_cache_result_tokens(tokens_a, "a", 64)
    verified = trie.match_tokens(tokens_a, {"a"})
    assert verified.length == 64
    assert verified.verified == {"a"}


def test_extract_chat_includes_canonical_tools_and_tool_calls():
    tool_a = {
        "type": "function",
        "function": {"name": "search", "parameters": {"b": 2, "a": 1}},
    }
    tool_a_reordered = {
        "function": {"parameters": {"a": 1, "b": 2}, "name": "search"},
        "type": "function",
    }
    tool_b = {
        "type": "function",
        "function": {"name": "calculator", "parameters": {"expr": "str"}},
    }
    messages = [
        {"role": "system", "content": "you are an agent"},
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": "search", "arguments": '{"q":"kv"}'},
                }
            ],
        },
        {"role": "tool", "tool_call_id": "call_1", "content": "result text"},
    ]

    p1 = extract_prompt(
        "chat_completions",
        json.dumps({"tools": [tool_a], "messages": messages}).encode(),
    )
    p2 = extract_prompt(
        "chat_completions",
        json.dumps({"tools": [tool_a_reordered], "messages": messages}).encode(),
    )
    p3 = extract_prompt(
        "chat_completions",
        json.dumps({"tools": [tool_b], "messages": messages}).encode(),
    )

    assert p1 == p2
    assert p1 != p3
    assert "tools:" in p1
    assert "tool_calls=" in p1
    assert "tool[call_1]: result text" in p1


def test_extract_prompt_supports_string_lists_for_completions_and_embeddings():
    comp_body = json.dumps({"prompt": ["first prompt", "second prompt"]}).encode()
    assert extract_prompt("v1_completions", comp_body) == "first prompt\nsecond prompt"

    emb_body = json.dumps({"input": ["doc one", "doc two"]}).encode()
    assert extract_prompt("v1_embeddings", emb_body) == "doc one\ndoc two"

    # Non-string lists still return empty string safely.
    assert extract_prompt("v1_completions", json.dumps({"prompt": [1, 2]}).encode()) == ""
