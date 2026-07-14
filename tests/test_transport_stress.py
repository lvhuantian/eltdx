from __future__ import annotations

from scripts.stress_actor_transport import (
    run_close_samples,
    run_generation_stress,
    run_heartbeat_impact,
    run_idle_cpu_sample,
    run_mixed_stress,
    run_warmed_resource_stress,
)


def test_one_thousand_generation_changes_keep_one_actor_and_no_resources() -> None:
    result = run_generation_stress(1000)

    assert result["generation_counter"] >= 1000
    assert sum(result["server_accepts"]) >= 1000
    assert all(count > 0 for count in result["server_requests"])
    assert result["servers_used"] == 2
    assert result["unique_responses"] == 1000
    assert result["duplicate_responses"] == 0
    assert result["missing_responses"] == 0
    assert result["unexpected_responses"] == 0
    assert result["cross_request_completions"] == 0
    assert result["cross_generation_completions"] == 0
    assert result["stale_events"] == 0
    assert result["cleanup"]["all_owned_resources_closed"] is True
    assert result["actor_threads_after"] == result["actor_threads_before"]


def test_bounded_mixed_stress_has_exact_pool_concurrency_and_clean_close() -> None:
    result = run_mixed_stress(
        2000,
        pool_size=4,
        concurrency=20,
        push_every=37,
        close_every=500,
        response_delay=0.001,
    )

    assert sum(result["server_requests"]) >= 2000
    assert all(count > 0 for count in result["server_requests"])
    assert result["servers_used"] == 2
    assert result["server_max_active"] == 4
    assert result["unique_responses"] == 2000
    assert result["duplicate_responses"] == 0
    assert result["missing_responses"] == 0
    assert result["unexpected_responses"] == 0
    assert result["cross_request_completions"] == 0
    assert result["cross_generation_completions"] == 0
    assert result["stale_events"] == 0
    assert result["broker_after_close"] == {
        "idle_slots": 0,
        "waiters": 0,
        "pin_waiters": 0,
        "leases": 0,
        "closed": True,
    }
    assert result["push_after_close"] == {"frames": 0, "bytes": 0, "closed": True}
    assert all(item["all_owned_resources_closed"] for item in result["cleanup"])
    assert result["actor_threads_after"] == 0


def test_warmed_resource_rounds_reach_an_exact_plateau() -> None:
    result = run_warmed_resource_stress(warmup_rounds=2, measured_rounds=5, generations_per_round=20)

    assert result["resource_counter_supported"] is True
    assert result["exact_plateau"] is True
    assert result["monotonic_growth"] is False
    assert result["all_actor_threads_closed"] is True
    assert result["all_owned_resources_closed"] is True
    assert result["cross_request_completions"] == 0
    assert result["cross_generation_completions"] == 0


def test_close_latency_is_bounded_idle_and_under_load() -> None:
    idle = run_close_samples(10, pool_size=2)
    loaded = run_close_samples(10, pool_size=2, loaded=True)

    assert idle["p99_ms"] < 100
    assert loaded["p99_ms"] < 250


def test_idle_actor_blocks_and_heartbeat_defers_under_continuous_work() -> None:
    idle = run_idle_cpu_sample(0.2)
    impact = run_heartbeat_impact(1000)

    assert idle["cpu_ratio"] < 0.1
    assert impact["blocks"] == 4
    assert impact["phases"] == 32
    assert impact["idle_probe_heartbeats"] >= 4
    assert impact["idle_probe_connections"] == 4
    assert impact["paced_heartbeat_requests"] >= 32
    assert impact["paced_business_requests"] == 32
    assert impact["heartbeat_during_business"] == 0
    assert impact["unique_responses"] == impact["business_requests"]
    assert impact["duplicate_responses"] == 0
    assert impact["missing_responses"] == 0
    assert impact["unexpected_responses"] == 0
    assert impact["cross_request_completions"] == 0
    assert impact["cross_generation_completions"] == 0
    assert impact["with_heartbeat"]["heartbeat_requests_total"] == 0
    assert impact["throughput_ratio"] > 0.99
