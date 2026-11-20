import pytest

from gateway.config import (
    BackendConfig,
    ConfigError,
    GatewayConfig,
    config_from_mapping,
    load_config,
)


def test_valid_config_from_mapping():
    config = config_from_mapping(
        {
            "server": {"host": "0.0.0.0", "port": 1234, "strategy": "round_robin"},
            "backends": [
                {"id": "a", "url": "http://127.0.0.1:18090/"},
                {"id": "b", "url": "http://127.0.0.1:18091"},
            ],
        }
    )
    assert config.port == 1234
    assert config.strategy == "round_robin"
    assert [b.id for b in config.backends] == ["a", "b"]
    assert config.backends[0].base_url == "http://127.0.0.1:18090"
    assert config.backends[0].base_url + "/health" == "http://127.0.0.1:18090/health"


def test_missing_backends_rejected():
    with pytest.raises(ConfigError, match="at least one"):
        config_from_mapping({"server": {}})


def test_duplicate_backend_ids_rejected():
    with pytest.raises(ConfigError, match="duplicate backend id"):
        GatewayConfig(
            backends=(
                BackendConfig("a", "http://127.0.0.1:1"),
                BackendConfig("a", "http://127.0.0.1:2"),
            )
        )


def test_bad_url_rejected():
    with pytest.raises(ConfigError, match="http"):
        BackendConfig("a", "127.0.0.1:18090")


def test_unknown_strategy_rejected():
    with pytest.raises(ConfigError, match="unknown strategy"):
        GatewayConfig(
            backends=(BackendConfig("a", "http://127.0.0.1:1"),),
            strategy="random",
        )


def test_bad_port_rejected():
    with pytest.raises(ConfigError, match="port"):
        GatewayConfig(
            backends=(BackendConfig("a", "http://127.0.0.1:1"),), port=70000
        )


def test_missing_field_reports_position():
    with pytest.raises(ConfigError, match=r"backends\[1\] is missing 'url'"):
        config_from_mapping(
            {"backends": [{"id": "a", "url": "http://127.0.0.1:1"}, {"id": "b"}]}
        )


def test_non_numeric_port_rejected():
    with pytest.raises(ConfigError, match="port must be an integer"):
        config_from_mapping(
            {
                "server": {"port": "8080"},
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
            }
        )


def test_load_config_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.toml")


def test_load_config_invalid_toml(tmp_path):
    path = tmp_path / "bad.toml"
    path.write_text("this is not = [valid")
    with pytest.raises(ConfigError, match="invalid TOML"):
        load_config(path)


def test_example_config_is_valid():
    from pathlib import Path

    example = Path(__file__).resolve().parents[2] / "config.example.toml"
    config = load_config(example)
    assert [b.id for b in config.backends] == ["a", "b"]
    assert config.strategy == "session_affinity"
    assert config.admission.max_in_flight == 8
    assert config.admission.max_queue_size == 16
    assert config.admission.limit_for("a") == 4
    assert config.admission.limit_for("b") == 4


def _admission_mapping(**admission):
    return {
        "backends": [
            {"id": "a", "url": "http://127.0.0.1:1"},
            {"id": "b", "url": "http://127.0.0.1:2"},
        ],
        "admission": admission,
    }


def test_admission_defaults_when_section_missing():
    config = config_from_mapping(
        {"backends": [{"id": "a", "url": "http://127.0.0.1:1"}]}
    )
    assert config.admission.enabled is True
    assert config.admission.max_in_flight == 64
    assert config.admission.max_queue_size == 128
    assert config.admission.queue_timeout_ms == 2000


def test_admission_valid_values():
    config = config_from_mapping(
        _admission_mapping(
            enabled=True,
            max_in_flight=4,
            max_queue_size=0,
            queue_timeout_ms=0,
            per_backend={"a": 2, "b": 4},
        )
    )
    assert config.admission.max_queue_size == 0
    assert config.admission.limit_for("a") == 2
    assert config.admission.limit_for("b") == 4


def test_admission_disabled_is_valid():
    config = config_from_mapping(_admission_mapping(enabled=False))
    assert config.admission.enabled is False


def test_admission_negative_or_zero_capacity_rejected():
    with pytest.raises(ConfigError, match="max_in_flight"):
        config_from_mapping(_admission_mapping(max_in_flight=0))
    with pytest.raises(ConfigError, match="max_queue_size"):
        config_from_mapping(_admission_mapping(max_queue_size=-1))
    with pytest.raises(ConfigError, match="queue_timeout_ms"):
        config_from_mapping(_admission_mapping(queue_timeout_ms=-5))


def test_admission_bad_types_rejected():
    with pytest.raises(ConfigError, match="enabled must be a boolean"):
        config_from_mapping(_admission_mapping(enabled="yes"))
    with pytest.raises(ConfigError, match="max_in_flight must be an integer"):
        config_from_mapping(_admission_mapping(max_in_flight=4.5))
    with pytest.raises(ConfigError, match="max_queue_size must be an integer"):
        config_from_mapping(_admission_mapping(max_queue_size=True))


def test_admission_unknown_backend_rejected():
    with pytest.raises(ConfigError, match="unknown backend"):
        config_from_mapping(_admission_mapping(per_backend={"z": 1}))


def test_admission_per_backend_limit_above_global_rejected():
    with pytest.raises(ConfigError, match="exceeds"):
        config_from_mapping(
            _admission_mapping(max_in_flight=2, per_backend={"a": 3})
        )


def test_admission_per_backend_must_be_positive():
    with pytest.raises(ConfigError, match="at least 1"):
        config_from_mapping(_admission_mapping(per_backend={"a": 0}))


def test_admission_unknown_key_rejected():
    with pytest.raises(ConfigError, match="unknown admission keys"):
        config_from_mapping(_admission_mapping(max_inflight=4))


def test_reliability_defaults_when_sections_missing():
    config = config_from_mapping(
        {"backends": [{"id": "a", "url": "http://127.0.0.1:1"}]}
    )
    assert config.deadline.request_timeout_ms == 60000
    assert config.deadline.execution_timeout_ms == 30000
    assert config.retry.max_retries == 1
    assert config.health.failure_threshold == 2
    assert config.health.success_threshold == 2


def test_reliability_sections_parsed():
    config = config_from_mapping(
        {
            "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
            "deadline": {
                "request_timeout_ms": 1200,
                "execution_timeout_ms": 700,
            },
            "retry": {"max_retries": 2},
            "health": {"failure_threshold": 3, "success_threshold": 4},
        }
    )
    assert config.deadline.request_timeout_ms == 1200
    assert config.deadline.execution_timeout_ms == 700
    assert config.retry.max_retries == 2
    assert config.health.failure_threshold == 3
    assert config.health.success_threshold == 4


def test_deadline_invalid_values_rejected():
    with pytest.raises(ConfigError, match="request_timeout_ms"):
        config_from_mapping(
            {
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
                "deadline": {"request_timeout_ms": 0},
            }
        )
    with pytest.raises(ConfigError, match="execution_timeout_ms"):
        config_from_mapping(
            {
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
                "deadline": {"execution_timeout_ms": -1},
            }
        )


def test_retry_negative_rejected():
    with pytest.raises(ConfigError, match="max_retries"):
        config_from_mapping(
            {
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
                "retry": {"max_retries": -1},
            }
        )


def test_health_thresholds_must_be_positive():
    with pytest.raises(ConfigError, match="failure_threshold"):
        config_from_mapping(
            {
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
                "health": {"failure_threshold": 0},
            }
        )
    with pytest.raises(ConfigError, match="success_threshold"):
        config_from_mapping(
            {
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
                "health": {"success_threshold": 0},
            }
        )


def test_reliability_unknown_keys_rejected():
    with pytest.raises(ConfigError, match="unknown deadline keys"):
        config_from_mapping(
            {
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
                "deadline": {"timeout": 1},
            }
        )
    with pytest.raises(ConfigError, match="unknown retry keys"):
        config_from_mapping(
            {
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
                "retry": {"retries": 1},
            }
        )
    with pytest.raises(ConfigError, match="unknown health keys"):
        config_from_mapping(
            {
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
                "health": {"threshold": 1},
            }
        )


def test_server_port_must_be_integer():
    with pytest.raises(ConfigError, match="port must be an integer"):
        config_from_mapping(
            {
                "server": {"port": 18080.5},
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
            }
        )
    with pytest.raises(ConfigError, match="port must be an integer"):
        GatewayConfig(
            backends=(BackendConfig("a", "http://127.0.0.1:1"),), port=18080.5
        )


def test_connection_limits_must_be_integers():
    with pytest.raises(ConfigError, match="max_connections_per_backend"):
        config_from_mapping(
            {
                "server": {"max_connections_per_backend": 4.5},
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
            }
        )
    with pytest.raises(ConfigError, match="max_keepalive_connections"):
        config_from_mapping(
            {
                "server": {"max_keepalive_connections": 2.5},
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
            }
        )


def test_keepalive_limit_must_not_exceed_connection_limit():
    with pytest.raises(ConfigError, match="must not exceed"):
        GatewayConfig(
            backends=(BackendConfig("a", "http://127.0.0.1:1"),),
            max_connections_per_backend=4,
            max_keepalive_connections=8,
        )


def test_prefix_defaults_and_valid_values():
    config = config_from_mapping(
        {"backends": [{"id": "a", "url": "http://127.0.0.1:1"}]}
    )
    assert config.prefix.chunk_chars == 128
    assert config.prefix.min_match_chars == 128
    assert config.prefix.max_nodes == 100000

    config = config_from_mapping(
        {
            "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
            "server": {"strategy": "prefix_affinity"},
            "prefix": {
                "chunk_chars": 64,
                "min_match_chars": 32,
                "max_nodes": 1000,
            },
        }
    )
    assert config.strategy == "prefix_affinity"
    assert config.prefix.chunk_chars == 64
    assert config.prefix.min_match_chars == 32
    assert config.prefix.max_nodes == 1000


def test_prefix_invalid_values_rejected():
    with pytest.raises(ConfigError, match="chunk_chars"):
        config_from_mapping(
            {
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
                "prefix": {"chunk_chars": 0},
            }
        )
    with pytest.raises(ConfigError, match="min_match_chars"):
        config_from_mapping(
            {
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
                "prefix": {"min_match_chars": -1},
            }
        )
    with pytest.raises(ConfigError, match="max_nodes"):
        config_from_mapping(
            {
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
                "prefix": {"max_nodes": 1.5},
            }
        )
    with pytest.raises(ConfigError, match="unknown prefix keys"):
        config_from_mapping(
            {
                "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
                "prefix": {"chunk": 8},
            }
        )


def test_kv_cache_aware_strategy_accepted():
    config = config_from_mapping(
        {
            "server": {"strategy": "kv_cache_aware"},
            "backends": [{"id": "a", "url": "http://127.0.0.1:1"}],
        }
    )
    assert config.strategy == "kv_cache_aware"


def test_hardening_defaults():
    config = config_from_mapping(
        {"backends": [{"id": "a", "url": "http://127.0.0.1:18090"}]}
    )
    assert config.max_request_body_bytes == 1_048_576
    assert config.drain_timeout_s == 30.0
    assert config.shutdown_timeout_s == 10.0
    assert config.health.timeout_s == 2.0
    assert config.prefix.ttl_seconds == 900.0
    assert config.prefix.tokenizer_url is None
    assert config.prefix.min_match_tokens == 16
    assert config.admin.token is None


def test_hardening_values_from_mapping():
    config = config_from_mapping(
        {
            "server": {
                "max_request_body_bytes": 2048,
                "drain_timeout_s": 5,
                "shutdown_timeout_s": 2.5,
            },
            "health": {"timeout_s": 0.5},
            "prefix": {
                "ttl_seconds": 0,
                "tokenizer_url": "http://127.0.0.1:18090",
                "min_match_tokens": 8,
                "tokenizer_timeout_s": 0.25,
                "tokenizer_cache_entries": 32,
            },
            "admin": {"token": "secret"},
            "backends": [{"id": "a", "url": "http://127.0.0.1:18090"}],
        }
    )
    assert config.max_request_body_bytes == 2048
    assert config.drain_timeout_s == 5.0
    assert config.shutdown_timeout_s == 2.5
    assert config.health.timeout_s == 0.5
    assert config.prefix.ttl_seconds == 0.0
    assert config.prefix.tokenizer_url == "http://127.0.0.1:18090"
    assert config.prefix.min_match_tokens == 8
    assert config.prefix.tokenizer_timeout_s == 0.25
    assert config.prefix.tokenizer_cache_entries == 32
    assert config.admin.token == "secret"
    assert "secret" not in repr(config.admin)


def test_hardening_invalid_values_rejected():
    base = {"backends": [{"id": "a", "url": "http://127.0.0.1:18090"}]}
    with pytest.raises(ConfigError, match="max_request_body_bytes"):
        config_from_mapping({**base, "server": {"max_request_body_bytes": 0}})
    with pytest.raises(ConfigError, match="ttl_seconds"):
        config_from_mapping({**base, "prefix": {"ttl_seconds": -1}})
    with pytest.raises(ConfigError, match="tokenizer_url"):
        config_from_mapping({**base, "prefix": {"tokenizer_url": "file:///x"}})
    with pytest.raises(ConfigError, match="tokenizer_timeout_s"):
        config_from_mapping(
            {
                **base,
                "prefix": {
                    "tokenizer_url": "http://127.0.0.1:1",
                    "tokenizer_timeout_s": 0,
                },
            }
        )
    config = config_from_mapping({**base, "admin": {"token": ""}})
    assert config.admin.token is None


def test_prefix_load_overflow_threshold_config():
    base = {"backends": [{"id": "a", "url": "http://127.0.0.1:18090"}]}
    default_cfg = config_from_mapping(base)
    assert default_cfg.prefix.load_overflow_threshold == 0

    custom_cfg = config_from_mapping(
        {**base, "prefix": {"load_overflow_threshold": 4}}
    )
    assert custom_cfg.prefix.load_overflow_threshold == 4

    with pytest.raises(ConfigError, match="load_overflow_threshold"):
        config_from_mapping({**base, "prefix": {"load_overflow_threshold": -1}})
    with pytest.raises(ConfigError, match="load_overflow_threshold"):
        config_from_mapping({**base, "prefix": {"load_overflow_threshold": True}})


def test_prefix_token_block_size_config():
    base = {"backends": [{"id": "a", "url": "http://127.0.0.1:18090"}]}
    default_cfg = config_from_mapping(base)
    assert default_cfg.prefix.token_block_size == 16

    custom_cfg = config_from_mapping({**base, "prefix": {"token_block_size": 32}})
    assert custom_cfg.prefix.token_block_size == 32

    with pytest.raises(ConfigError, match="token_block_size"):
        config_from_mapping({**base, "prefix": {"token_block_size": 0}})
    with pytest.raises(ConfigError, match="token_block_size"):
        config_from_mapping({**base, "prefix": {"token_block_size": False}})
