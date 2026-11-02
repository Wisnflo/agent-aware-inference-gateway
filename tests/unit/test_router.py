import pytest

from gateway.backends import Backend
from gateway.router import (
    RoundRobinRouter,
    SessionAffinityRouter,
    make_router,
    session_fingerprint,
)


def make_backends(*ids):
    return [Backend(id=backend_id, base_url=f"http://127.0.0.1:{9000 + i}") for i, backend_id in enumerate(ids)]


def test_round_robin_cycles_in_configuration_order():
    router = RoundRobinRouter(make_backends("a", "b", "c"))
    assert [router.select(None).id for _ in range(7)] == ["a", "b", "c", "a", "b", "c", "a"]


def test_round_robin_ignores_session_id():
    router = RoundRobinRouter(make_backends("a", "b"))
    assert router.select("s1").id == "a"
    assert router.select("s1").id == "b"


def test_session_affinity_is_stable_per_session():
    router = SessionAffinityRouter(make_backends("a", "b"))
    for session in ("alpha", "beta", "gamma"):
        first = router.select(session).id
        assert all(router.select(session).id == first for _ in range(50))


def test_session_affinity_golden_mapping():
    # These values pin the hash contract across processes and Python versions.
    # They were computed once with the published formula and must not change.
    backends = make_backends("a", "b")
    router = SessionAffinityRouter(backends)
    expected = {
        "s0": "a",
        "s1": "b",
        "s2": "b",
        "s3": "a",
        "s4": "a",
        "s5": "a",
        "s6": "b",
        "s7": "a",
        "s8": "a",
        "s9": "b",
    }
    assert router.mapping_for(backends, list(expected)) == expected


def test_session_affinity_spreads_sessions():
    router = SessionAffinityRouter(make_backends("a", "b"))
    counts = {"a": 0, "b": 0}
    for index in range(200):
        counts[router.select(f"session-{index}").id] += 1
    assert min(counts.values()) > 70, counts


def test_missing_session_falls_back_to_round_robin():
    router = SessionAffinityRouter(make_backends("a", "b"))
    assert [router.select(None).id for _ in range(4)] == ["a", "b", "a", "b"]
    assert [router.select("").id for _ in range(2)] == ["a", "b"]


def test_rendezvous_minimal_migration_when_adding_backend():
    sessions = [f"session-{i}" for i in range(500)]
    before = SessionAffinityRouter(make_backends("a", "b", "c"))
    after = SessionAffinityRouter(make_backends("a", "b", "c", "d"))
    old = before.mapping_for(make_backends("a", "b", "c"), sessions)
    new = after.mapping_for(make_backends("a", "b", "c", "d"), sessions)
    moved_to_new = {sid for sid, backend in new.items() if backend == "d"}
    unexpected = {
        sid for sid, backend in new.items() if backend != "d" and backend != old[sid]
    }
    assert unexpected == set()
    assert 80 < len(moved_to_new) < 180  # ~1/4 of sessions


def test_removing_backend_only_moves_its_sessions():
    sessions = [f"session-{i}" for i in range(500)]
    before = SessionAffinityRouter(make_backends("a", "b", "c"))
    after = SessionAffinityRouter(make_backends("a", "c"))
    old = before.mapping_for(make_backends("a", "b", "c"), sessions)
    new = after.mapping_for(make_backends("a", "c"), sessions)
    for sid in sessions:
        if old[sid] != "b":
            assert new[sid] == old[sid]


def test_make_router_rejects_unknown_strategy():
    with pytest.raises(ValueError, match="unknown strategy"):
        make_router("magic", make_backends("a"))


def test_session_fingerprint_does_not_leak_id():
    assert session_fingerprint(None) == "none"
    fingerprint = session_fingerprint("secret-session")
    assert "secret" not in fingerprint
    assert len(fingerprint) == 8
