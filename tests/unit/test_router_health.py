"""Health-aware routing decisions (no HTTP)."""
import pytest

from gateway.backends import Backend
from gateway.router import NoHealthyBackend, RoundRobinRouter, SessionAffinityRouter


def make_backends(*ids):
    return [
        Backend(id=backend_id, base_url=f"http://127.0.0.1:{9000 + index}")
        for index, backend_id in enumerate(ids)
    ]


def test_session_affinity_keeps_preferred_backend_when_healthy():
    backends = make_backends("a", "b")
    router = SessionAffinityRouter(backends)
    preferred = router.select("s1").id
    decision = router.decide("s1", {preferred})
    assert decision.backend.id == preferred
    assert decision.preferred_backend_id == preferred
    assert decision.failover is False


def test_session_affinity_moves_to_healthy_backend_when_preferred_down():
    backends = make_backends("a", "b")
    router = SessionAffinityRouter(backends)
    preferred = router.select("s1").id
    other = "b" if preferred == "a" else "a"
    decision = router.decide("s1", {other})
    assert decision.backend.id == other
    assert decision.preferred_backend_id == preferred
    assert decision.failover is True


def test_session_affinity_immediate_failback_after_recovery():
    backends = make_backends("a", "b")
    router = SessionAffinityRouter(backends)
    preferred = router.select("s1").id
    other = "b" if preferred == "a" else "a"
    assert router.decide("s1", {other}).backend.id == other
    recovered = router.decide("s1", {"a", "b"})
    assert recovered.backend.id == preferred
    assert recovered.failover is False


def test_round_robin_skips_unhealthy_backend():
    router = RoundRobinRouter(make_backends("a", "b"))
    first = router.decide(None, {"b"})
    assert first.backend.id == "b"
    assert first.preferred_backend_id == "a"
    assert first.failover is True
    second = router.decide(None, {"b"})
    assert second.backend.id == "b"
    assert second.preferred_backend_id == "b"
    assert second.failover is False


def test_no_healthy_backend_raises():
    router = SessionAffinityRouter(make_backends("a", "b"))
    with pytest.raises(NoHealthyBackend):
        router.select("s1", set())
    rr = RoundRobinRouter(make_backends("a", "b"))
    with pytest.raises(NoHealthyBackend):
        rr.select(None, set())


def test_session_affinity_without_session_uses_health_aware_round_robin():
    router = SessionAffinityRouter(make_backends("a", "b"))
    decision = router.decide(None, {"b"})
    assert decision.backend.id == "b"
    assert decision.failover is True
