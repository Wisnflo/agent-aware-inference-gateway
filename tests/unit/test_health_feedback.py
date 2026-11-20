"""Health state feedback from business traffic and probes."""

from gateway.backends import BackendPool
from gateway.config import (
    BackendConfig,
    GatewayConfig,
    HealthConfig,
)


def make_pool():
    config = GatewayConfig(
        backends=(BackendConfig("a", "http://127.0.0.1:1"),),
        health=HealthConfig(failure_threshold=2, success_threshold=2),
    )
    return BackendPool(config=config)


def test_business_success_can_restore_but_does_not_clear_probe_failures():
    pool = make_pool()
    backend = pool.get("a")
    backend.healthy = True
    backend.consecutive_failures = 1
    pool.mark_business_success("a")
    # A business success must not mask a probe-failure streak while healthy.
    assert backend.healthy is True
    assert backend.consecutive_failures == 1

    backend.healthy = False
    backend.consecutive_successes = 0
    pool.mark_business_success("a")
    assert backend.healthy is False
    pool.mark_business_success("a")
    assert backend.healthy is True


def test_passive_failure_uses_debounce_threshold():
    pool = make_pool()
    backend = pool.get("a")
    backend.healthy = True
    pool.mark_failure("a", "ConnectError")
    assert backend.healthy is True
    pool.mark_failure("a", "ConnectError")
    assert backend.healthy is False


async def test_probe_client_is_separate_from_inference_client():
    pool = make_pool()
    backend = pool.get("a")
    pool._attach_clients(backend)
    assert backend.client is not None
    assert backend.probe_client is not None
    assert backend.probe_client is not backend.client
    assert backend.probe_client.timeout.connect == 2.0
    await pool.close()
