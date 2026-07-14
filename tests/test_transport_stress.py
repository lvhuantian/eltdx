from __future__ import annotations

from scripts.stress_actor_transport import (
    run_close_samples,
    run_generation_stress,
    run_heartbeat_impact,
    run_idle_cpu_sample,
    run_mixed_stress,
)


def test_one_thousand_generation_changes_keep_one_actor_and_no_resources() -> None:
    result = run_generation_stress(1000)

    assert result["generation_counter"] >= 1000
    assert result["server_accepts"] >= 1000
    assert result["stale_events"] == 0
    assert result["actor_threads_after"] == result["actor_threads_before"]
    if result["resource_before"] is not None and result["resource_after"] is not None:
        assert result["resource_after"] <= result["resource_before"] + 24


def test_bounded_mixed_stress_has_exact_pool_concurrency_and_clean_close() -> None:
    result = run_mixed_stress(
        2000,
        pool_size=4,
        concurrency=20,
        push_every=37,
        close_every=500,
        response_delay=0.001,
    )

    assert result["server_requests"] >= 2000
    assert result["server_max_active"] == 4
    assert result["stale_events"] == 0
    assert result["broker_waiters"] == 0
    assert result["broker_leases"] == 0
    assert result["actor_threads_after"] == 0
    if result["resource_before"] is not None and result["resource_after"] is not None:
        assert result["resource_after"] <= result["resource_before"] + 24


def test_close_latency_is_bounded_idle_and_under_load() -> None:
    idle = run_close_samples(10, pool_size=2)
    loaded = run_close_samples(10, pool_size=2, loaded=True)

    assert idle["p99_ms"] < 100
    assert loaded["p99_ms"] < 250


def test_idle_actor_blocks_and_heartbeat_defers_under_continuous_work() -> None:
    idle = run_idle_cpu_sample(0.2)
    impact = run_heartbeat_impact(5000)

    assert idle["cpu_ratio"] < 0.1
    assert impact["trials"] == 3
    assert impact["with_heartbeat"]["heartbeat_requests"] < impact["requests"] // 100
    assert impact["throughput_ratio"] >= 0.95
