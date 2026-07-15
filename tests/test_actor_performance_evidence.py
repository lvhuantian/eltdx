from __future__ import annotations

import copy
import hashlib
import json
import platform
import socket
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import replace
from fractions import Fraction
from pathlib import Path

import pytest

from eltdx.models import FileContentChunk
from scripts import benchmark_actor_transport as benchmark
from scripts import verify_actor_performance as verifier


CURRENT_SHA = "1" * 40


def test_performance_protocol_requires_unique_file_content_responses() -> None:
    config = benchmark.fifo_v2_config()

    assert benchmark.FIFO_PROTOCOL == "fifo-v2"
    assert config["response_contract"] == {
        "command": "file_content",
        "path": "eltdx-performance.bin",
        "content_fields": [
            "request_token",
            "measurement_epoch",
            "connection_id",
            "attempt_sequence",
        ],
        "completion_record_fields": [
            "requested_token",
            "snapshot_token",
            "echoed_token",
            "response_epoch",
            "response_connection_id",
            "response_attempt_sequence",
            "expected_epoch",
            "expected_connection_id",
            "expected_attempt_sequence",
        ],
    }


def test_saturated_real_loopback_has_unique_completion_evidence() -> None:
    result = benchmark.run_case(2, 4, 8, 0, include_latencies=True)

    assert result["request_token_start"] == 0
    assert result["request_token_end"] == 7
    assert result["server_connections"] == 2
    assert result["server_attempts"] == 8
    assert result["unique_responses"] == 8
    assert result["duplicate_responses"] == 0
    assert result["missing_responses"] == 0
    assert result["unexpected_responses"] == 0
    assert result["cross_request_completions"] == 0
    assert result["cross_generation_completions"] == 0
    assert len(result["completion_sha256"]) == 64


def _small_protocol() -> verifier.CampaignProtocol:
    config = {
        "delay_ns": 5_000_000,
        "cases": {
            "sequential": {
                "mode": "saturated",
                "pool_size": 1,
                "concurrency": 1,
                "requests": 4,
                "warmup_requests": 1,
                "latency_metric": "call_latency_ns",
            },
            "saturated": {
                "mode": "saturated",
                "pool_size": 2,
                "concurrency": 4,
                "requests": 4,
                "warmup_requests": 1,
                "latency_metric": "call_latency_ns",
            },
            "cohort": {
                "mode": "fixed_cohort",
                "pool_size": 2,
                "cohort_size": 2,
                "measured_cohorts": 2,
                "warmup_cohorts": 1,
                "requests": 4,
                "latency_metric": "call_latency_ns",
            },
        },
    }
    return verifier.CampaignProtocol(
        name="test-fifo-v1",
        schedule=("baseline", "current", "current", "baseline"),
        config=config,
        baseline_sha=verifier.BASELINE_SHA,
        throughput_cases=("sequential", "saturated"),
        latency_cases=("sequential", "cohort"),
        expected_max_active={"sequential": 1, "saturated": 2, "cohort": 2},
        required_system=platform.system(),
        validate_declared_roots=False,
    )


def _reported_median(values: list[int]) -> int | float:
    value = verifier._median(values)
    return int(value) if value.denominator == 1 else float(value)


def _benchmark_chunk(payload: dict) -> FileContentChunk:
    token = payload["offset"]
    content = benchmark._BENCHMARK_VALUE.pack(token, 1, 1, token + 1)
    raw_payload = len(content).to_bytes(4, "little") + content
    return FileContentChunk(
        path=payload["path"],
        offset=token,
        request_size=payload["size"],
        chunk_len=len(content),
        content=content,
        raw_payload=raw_payload,
    )


def _case(
    expected: dict,
    role: str,
    max_active: int,
    *,
    latency_ns: list[int] | None = None,
    elapsed_ns: int | None = None,
) -> dict:
    latencies = latency_ns or ([1_000_000] * expected["requests"])
    elapsed = elapsed_ns if elapsed_ns is not None else (95_000_000 if role == "baseline" else 100_000_000)
    wave_makespans = None
    if expected["mode"] == "fixed_cohort":
        width = expected["cohort_size"]
        wave_makespans = [
            max(latencies[start : start + width])
            for start in range(0, len(latencies), width)
        ]
        elapsed = sum(wave_makespans)
    completion_records = []
    completion_digest = hashlib.sha256()
    for token in range(expected["requests"]):
        connection_id = token % expected["pool_size"] + 1
        record = [
            token,
            token,
            token,
            1,
            connection_id,
            token + 1,
            1,
            connection_id,
            token + 1,
        ]
        completion_records.append(record)
        completion_digest.update(benchmark._COMPLETION_RECORD.pack(*record))
    result = {
        **expected,
        "successes": expected["requests"],
        "errors": [],
        "worker_threads": expected.get("concurrency", expected.get("cohort_size")),
        "server_max_active": max_active,
        "server_requests": expected["requests"],
        "request_token_start": 0,
        "request_token_end": expected["requests"] - 1,
        "request_token_sum": expected["requests"] * (expected["requests"] - 1) // 2,
        "server_connections": expected["pool_size"],
        "server_attempts": expected["requests"],
        "unique_responses": expected["requests"],
        "duplicate_responses": 0,
        "missing_responses": 0,
        "unexpected_responses": 0,
        "cross_request_completions": 0,
        "cross_generation_completions": 0,
        "completion_sha256": completion_digest.hexdigest(),
        "completion_records": completion_records,
        "elapsed_ns": elapsed,
        "latency_ns": list(latencies),
        "latency_p50_ns": _reported_median(latencies),
        "latency_p99_ns": int(verifier._p99(latencies)),
        "seconds": round(elapsed / 1_000_000_000, 6),
        "throughput_rps": round(expected["requests"] * 1_000_000_000 / elapsed, 3),
        "latency_p50_ms": round(float(verifier._median(latencies)) / 1_000_000, 4),
        "latency_p99_ms": round(float(verifier._p99(latencies)) / 1_000_000, 4),
    }
    if expected["mode"] == "fixed_cohort":
        result.update(
            {
                "call_latency_ns": list(latencies),
                "cohort_latency_ns": list(latencies),
                "wave_makespan_ns": wave_makespans,
                "wall_elapsed_ns": elapsed + 1_000,
                "boundary_checks_supported": role == "current",
                "boundary_checks": (
                    expected["warmup_cohorts"] + expected["measured_cohorts"]
                    if role == "current"
                    else 0
                ),
                "boundary_checks_all_clean": True,
            }
        )
    return result


def _bundle(protocol: verifier.CampaignProtocol | None = None) -> dict:
    protocol = protocol or _small_protocol()
    config = copy.deepcopy(protocol.config)
    declaration = {
        "protocol": protocol.name,
        "campaign_id": "test-campaign",
        "declared_at_utc": "2026-07-15T00:00:00Z",
        "schedule": list(protocol.schedule),
        "expected_trials": len(protocol.schedule),
        "baseline_sha": protocol.baseline_sha,
        "current_sha": CURRENT_SHA,
        "baseline_source_root": "C:/baseline",
        "current_source_root": str(verifier.TOOL_ROOT),
        "workload_sha256": hashlib.sha256(verifier.BENCHMARK_PATH.read_bytes()).hexdigest(),
        "verifier_sha256": hashlib.sha256(Path(verifier.__file__).read_bytes()).hexdigest(),
        "system": platform.system(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "config": config,
        "config_sha256": verifier.canonical_sha256(config),
        "gates": verifier.frozen_gates(protocol),
        "stopping_rule": verifier.STOPPING_RULE,
    }
    declaration["declaration_sha256"] = verifier.canonical_sha256(declaration)
    trials = []
    for index, role in enumerate(protocol.schedule):
        trials.append(
            {
                "schema": 4,
                "kind": "actor-performance-trial",
                "protocol": protocol.name,
                "label": f"test-campaign/{index:03d}",
                "campaign_id": declaration["campaign_id"],
                "trial_id": f"test-campaign/{index:03d}",
                "trial_index": index,
                "attempt": 1,
                "role": role,
                "declaration_sha256": declaration["declaration_sha256"],
                "workload_sha256": declaration["workload_sha256"],
                "source_root": "C:/baseline" if role == "baseline" else str(verifier.TOOL_ROOT),
                "system": declaration["system"],
                "platform": declaration["platform"],
                "python": declaration["python"],
                "started_at_utc": f"2026-07-15T00:00:0{index * 2 + 1}Z",
                "ended_at_utc": f"2026-07-15T00:00:0{index * 2 + 2}Z",
                "config": copy.deepcopy(config),
                "config_sha256": declaration["config_sha256"],
                "implementation_sha": protocol.baseline_sha if role == "baseline" else CURRENT_SHA,
                "implementation_dirty": False,
                "cases": {
                    name: _case(
                        dict(expected),
                        role,
                        protocol.expected_max_active[name],
                        latency_ns=[1_000_000 + index] * expected["requests"],
                        elapsed_ns=(95_000_000 if role == "baseline" else 100_000_000) + index,
                    )
                    for name, expected in config["cases"].items()
                },
            }
        )
    return {
        "schema": 4,
        "kind": "actor-performance-campaign",
        "declaration": declaration,
        "trials": trials,
    }


def _set_latency(bundle: dict, protocol: verifier.CampaignProtocol, role: str, name: str, value: int) -> None:
    expected = dict(protocol.config["cases"][name])
    for index, trial in enumerate(bundle["trials"]):
        if trial["role"] == role:
            trial["cases"][name] = _case(
                expected,
                role,
                protocol.expected_max_active[name],
                latency_ns=[value - index, value, value, value],
            )


def _set_elapsed(case: dict, requests: int, elapsed_ns: int) -> None:
    case["elapsed_ns"] = elapsed_ns
    case["seconds"] = round(elapsed_ns / 1_000_000_000, 6)
    case["throughput_rps"] = round(requests * 1_000_000_000 / elapsed_ns, 3)


def _rehash_declaration(bundle: dict) -> None:
    declaration = bundle["declaration"]
    payload = dict(declaration)
    payload.pop("declaration_sha256", None)
    declaration["declaration_sha256"] = verifier.canonical_sha256(payload)
    for trial in bundle["trials"]:
        trial["declaration_sha256"] = declaration["declaration_sha256"]


def _rehash_completion(case: dict) -> None:
    digest = hashlib.sha256()
    for record in case["completion_records"]:
        digest.update(benchmark._COMPLETION_RECORD.pack(*record))
    case["completion_sha256"] = digest.hexdigest()


def test_fifo_producer_and_verifier_freeze_the_same_config() -> None:
    assert benchmark.fifo_v2_config() == verifier.fifo_v2_config()


def test_complete_counterbalanced_bundle_passes() -> None:
    protocol = _small_protocol()
    report = verifier.verify_campaign(_bundle(protocol), protocol)

    assert report["passed"]
    assert not report["errors"]
    assert all(gate["passed"] for gate in report["gates"])


def test_fifo_v2_rejects_fifo_v1_schema_and_missing_completion_evidence() -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    bundle["schema"] = 3
    for trial in bundle["trials"]:
        trial["schema"] = 3

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert any("unexpected schema" in error for error in report["errors"])

    bundle = _bundle(protocol)
    bundle["trials"][0]["cases"]["sequential"].pop("cross_generation_completions")
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("case keys differ" in error for error in report["errors"])


def test_completion_summary_detects_cross_request_and_cross_generation_results() -> None:
    server = benchmark.BenchmarkServer(0)
    server.accept_count = 1
    server.attempt_sequence = 2
    server.expected_provenance = {0: (1, 1, 1), 1: (1, 1, 2)}
    records = [
        {
            "requested_token": 0,
            "snapshot_token": 0,
            "echoed_token": 1,
            "measurement_epoch": 1,
            "connection_id": 1,
            "attempt_sequence": 1,
        },
        {
            "requested_token": 1,
            "snapshot_token": 1,
            "echoed_token": 0,
            "measurement_epoch": 1,
            "connection_id": 1,
            "attempt_sequence": 99,
        },
    ]

    summary = benchmark._completion_summary(records, server)

    assert summary["unique_responses"] == 2
    assert summary["duplicate_responses"] == 0
    assert summary["missing_responses"] == 0
    assert summary["unexpected_responses"] == 0
    assert summary["cross_request_completions"] == 2
    assert summary["cross_generation_completions"] == 1
    assert len(summary["completion_sha256"]) == 64


def test_campaign_recomputes_completion_digest_and_identity_counters() -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    case = bundle["trials"][0]["cases"]["sequential"]
    case["completion_sha256"] = "f" * 64

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert any("completion_sha256" in error and "recomputed" in error for error in report["errors"])

    bundle = _bundle(protocol)
    case = bundle["trials"][0]["cases"]["sequential"]
    records = case["completion_records"]
    records[0][2], records[1][2] = records[1][2], records[0][2]
    records[1][5] = records[0][5]
    digest = hashlib.sha256()
    for record in records:
        digest.update(benchmark._COMPLETION_RECORD.pack(*record))
    case["completion_sha256"] = digest.hexdigest()

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert any("cross_request_completions" in error for error in report["errors"])
    assert any("cross_generation_completions" in error for error in report["errors"])


def test_campaign_rejects_stale_trial_or_current_source_root() -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    bundle["trials"][0]["source_root"] = "C:/stale-checkout/ca43972"

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert any("source_root mismatch" in error for error in report["errors"])

    bundle = _bundle(protocol)
    bundle["declaration"]["current_source_root"] = "C:/stale-checkout/ca43972"
    _rehash_declaration(bundle)
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("does not own the adjacent verifier" in error for error in report["errors"])


def test_campaign_physically_validates_declared_git_roots(tmp_path, monkeypatch) -> None:
    protocol = replace(_small_protocol(), validate_declared_roots=True)
    baseline_root = (tmp_path / "baseline").resolve()
    baseline_root.mkdir()
    bundle = _bundle(protocol)
    bundle["declaration"]["baseline_source_root"] = str(baseline_root)
    for trial in bundle["trials"]:
        if trial["role"] == "baseline":
            trial["source_root"] = str(baseline_root)
    _rehash_declaration(bundle)

    def clean_identity(root: Path) -> tuple[str, bool]:
        return (protocol.baseline_sha, False) if root == baseline_root else (CURRENT_SHA, False)

    monkeypatch.setattr(verifier, "_git_identity", clean_identity)
    assert verifier.verify_campaign(bundle, protocol)["passed"]

    stale_sha = "ca439727b44a02d9396b3d6dcd21b78d06addb8b"
    bundle["declaration"]["current_sha"] = stale_sha
    for trial in bundle["trials"]:
        if trial["role"] == "current":
            trial["implementation_sha"] = stale_sha
    _rehash_declaration(bundle)
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("current source HEAD" in error for error in report["errors"])

    bundle = _bundle(protocol)
    missing = tmp_path / "missing-baseline"
    bundle["declaration"]["baseline_source_root"] = str(missing)
    for trial in bundle["trials"]:
        if trial["role"] == "baseline":
            trial["source_root"] = str(missing)
    _rehash_declaration(bundle)
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("not an existing path" in error for error in report["errors"])

    bundle = _bundle(protocol)
    bundle["declaration"]["baseline_source_root"] = str(baseline_root)
    for trial in bundle["trials"]:
        if trial["role"] == "baseline":
            trial["source_root"] = str(baseline_root)
    _rehash_declaration(bundle)

    def dirty_current_identity(root: Path) -> tuple[str, bool]:
        return (protocol.baseline_sha, False) if root == baseline_root else (CURRENT_SHA, True)

    monkeypatch.setattr(verifier, "_git_identity", dirty_current_identity)
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("current source root is dirty" in error for error in report["errors"])


def test_campaign_rejects_self_consistent_impossible_completion_provenance() -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    sequential = bundle["trials"][0]["cases"]["sequential"]
    for record in sequential["completion_records"]:
        record[3] = record[6] = 2
    _rehash_completion(sequential)
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("measurement epochs" in error for error in report["errors"])

    bundle = _bundle(protocol)
    saturated = bundle["trials"][0]["cases"]["saturated"]
    for record in saturated["completion_records"]:
        record[4] = record[7] = 1
    _rehash_completion(saturated)
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("every server connection" in error for error in report["errors"])

    bundle = _bundle(protocol)
    cohort = bundle["trials"][0]["cases"]["cohort"]
    first = cohort["completion_records"][0]
    second_wave = cohort["completion_records"][2]
    first[3:9], second_wave[3:9] = second_wave[3:9], first[3:9]
    _rehash_completion(cohort)
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("wave boundary" in error for error in report["errors"])

    bundle = _bundle(protocol)
    sequential = bundle["trials"][0]["cases"]["sequential"]
    first, second = sequential["completion_records"][:2]
    first[5], second[5] = second[5], first[5]
    first[8], second[8] = second[8], first[8]
    _rehash_completion(sequential)
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("sequential expected attempt" in error for error in report["errors"])


@pytest.mark.parametrize(
    "mutate",
    [
        lambda bundle: bundle["trials"].pop(),
        lambda bundle: bundle["trials"].append(copy.deepcopy(bundle["trials"][-1])),
        lambda bundle: bundle["trials"][1].__setitem__("role", "baseline"),
        lambda bundle: bundle["trials"][1].__setitem__("attempt", 2),
        lambda bundle: bundle["trials"][1].__setitem__("implementation_dirty", True),
        lambda bundle: bundle["trials"][1].__setitem__("trial_id", bundle["trials"][0]["trial_id"]),
        lambda bundle: bundle["trials"][1].__setitem__("workload_sha256", "b" * 64),
    ],
)
def test_campaign_rejects_optional_stop_or_identity_mutation(mutate) -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    mutate(bundle)

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert report["errors"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("successes", 3),
        ("errors", ["failed"]),
        ("server_max_active", 3),
        ("worker_threads", 3),
        ("elapsed_ns", 0),
        ("server_attempts", 3),
        ("unique_responses", 3),
        ("cross_request_completions", 1),
        ("completion_sha256", "0" * 64),
    ],
)
def test_campaign_rejects_invalid_case_evidence(field: str, value) -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    bundle["trials"][0]["cases"]["saturated"][field] = value

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert any("trial[0].saturated" in error for error in report["errors"])


@pytest.mark.parametrize(
    ("supported", "checks"),
    [
        (False, 999),
        ("no", -1),
        (None, "many"),
    ],
)
def test_campaign_rejects_impossible_unsupported_boundary_counts(supported, checks) -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    cohort = bundle["trials"][0]["cases"]["cohort"]
    cohort["boundary_checks_supported"] = supported
    cohort["boundary_checks"] = checks

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert any("boundary_checks" in error for error in report["errors"])


def test_campaign_rejects_truncated_or_forged_latency_samples() -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    bundle["trials"][0]["cases"]["cohort"]["latency_ns"].pop()

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert any("latency count" in error for error in report["errors"])

    bundle = _bundle(protocol)
    bundle["trials"][0]["cases"]["sequential"]["latency_p50_ns"] += 1
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("latency_p50_ns" in error for error in report["errors"])


def test_campaign_rejects_uniformly_forged_workload_hash() -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    declaration = bundle["declaration"]
    declaration["workload_sha256"] = "f" * 64
    declaration_without_hash = dict(declaration)
    declaration_without_hash.pop("declaration_sha256")
    declaration["declaration_sha256"] = verifier.canonical_sha256(declaration_without_hash)
    for trial in bundle["trials"]:
        trial["workload_sha256"] = declaration["workload_sha256"]
        trial["declaration_sha256"] = declaration["declaration_sha256"]

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert any("adjacent producer" in error for error in report["errors"])


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("gates", {"throughput_minimum": "0.01"}),
        ("stopping_rule", "rerun until pass"),
    ],
)
def test_campaign_rejects_self_consistent_policy_rewrite(field: str, value) -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    bundle["declaration"][field] = value
    _rehash_declaration(bundle)

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert any(field in error for error in report["errors"])


def test_campaign_rejects_overlapping_or_naive_trial_time() -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    bundle["trials"][1]["started_at_utc"] = bundle["trials"][0]["started_at_utc"]
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("overlaps" in error for error in report["errors"])

    bundle = _bundle(protocol)
    bundle["trials"][1]["started_at_utc"] = "2026-07-15T00:00:03"
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("UTC offset" in error for error in report["errors"])


def test_campaign_rejects_unbound_label_impossible_duration_and_replayed_cell() -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    bundle["trials"][0]["label"] = "unrelated-campaign/999"
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("label=" in error for error in report["errors"])

    bundle = _bundle(protocol)
    bundle["trials"][0]["ended_at_utc"] = "2026-07-15T00:00:01.000001Z"
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("shorter than case measurement" in error for error in report["errors"])

    bundle = _bundle(protocol)
    bundle["trials"][2]["cases"] = copy.deepcopy(bundle["trials"][1]["cases"])
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("replays sequential case evidence" in error for error in report["errors"])

    bundle = _bundle(protocol)
    bundle["trials"][2]["cases"]["sequential"] = copy.deepcopy(
        bundle["trials"][1]["cases"]["sequential"]
    )
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("replays sequential case evidence" in error for error in report["errors"])

    bundle = _bundle(protocol)
    for name in protocol.throughput_cases:
        bundle["trials"][1]["cases"][name] = copy.deepcopy(bundle["trials"][0]["cases"][name])
    report = verifier.verify_campaign(bundle, protocol)
    assert not report["passed"]
    assert any("replays sequential case evidence" in error for error in report["errors"])
    assert any("replays saturated case evidence" in error for error in report["errors"])


@pytest.mark.parametrize(("field", "index"), [("trial_index", 1), ("attempt", 0)])
def test_campaign_rejects_bool_as_schema_integer(field: str, index: int) -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    bundle["trials"][index][field] = True

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert report["errors"]


def test_campaign_rejects_forged_derived_summary() -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    bundle["trials"][0]["cases"]["sequential"]["throughput_rps"] = 999_999

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert any("throughput_rps" in error for error in report["errors"])


@pytest.mark.parametrize(
    "location",
    ["bundle", "declaration", "trial", "case"],
)
def test_campaign_rejects_unknown_schema_fields(location: str) -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    if location == "bundle":
        bundle["discarded_trials"] = []
    elif location == "declaration":
        bundle["declaration"]["actual_stopping_rule"] = "rerun until pass"
        _rehash_declaration(bundle)
    elif location == "trial":
        bundle["trials"][0]["prior_attempts"] = []
    else:
        bundle["trials"][0]["cases"]["sequential"]["discarded_samples"] = []

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert report["errors"]


def test_campaign_rejects_physically_impossible_elapsed_interval() -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    bundle["trials"][0]["cases"]["sequential"]["elapsed_ns"] = 1

    report = verifier.verify_campaign(bundle, protocol)

    assert not report["passed"]
    assert any("measured interval" in error or "concurrency-time" in error for error in report["errors"])


def test_throughput_gate_accepts_exact_95_percent_and_rejects_one_ns_slower() -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    for trial in bundle["trials"]:
        for name in protocol.throughput_cases:
            _set_elapsed(
                trial["cases"][name],
                protocol.config["cases"][name]["requests"],
                95_000_000 if trial["role"] == "baseline" else 100_000_000,
            )
    report = verifier.verify_campaign(bundle, protocol)
    assert report["passed"]

    current = next(trial for trial in bundle["trials"] if trial["role"] == "current")
    _set_elapsed(
        current["cases"]["saturated"],
        protocol.config["cases"]["saturated"]["requests"],
        current["cases"]["saturated"]["elapsed_ns"] + 1,
    )
    report = verifier.verify_campaign(bundle, protocol)
    gate = next(gate for gate in report["gates"] if gate["gate"] == "saturated.throughput")
    assert not gate["passed"]
    assert not report["passed"]


def test_latency_gate_accepts_floor_boundary_and_rejects_one_ns_more() -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    _set_latency(bundle, protocol, "baseline", "cohort", 1_000_000)
    _set_latency(bundle, protocol, "current", "cohort", 1_200_000)
    report = verifier.verify_campaign(bundle, protocol)
    gate = next(gate for gate in report["gates"] if gate["gate"] == "cohort.p50")
    assert gate["passed"]

    _set_latency(bundle, protocol, "current", "cohort", 1_200_001)
    report = verifier.verify_campaign(bundle, protocol)
    gate = next(gate for gate in report["gates"] if gate["gate"] == "cohort.p50")
    assert not gate["passed"]
    assert not report["passed"]


def test_saturated_raw_latency_is_reported_but_not_used_as_latency_gate() -> None:
    protocol = _small_protocol()
    bundle = _bundle(protocol)
    _set_latency(bundle, protocol, "baseline", "saturated", 1_000_000)
    _set_latency(bundle, protocol, "current", "saturated", 100_000_000)

    report = verifier.verify_campaign(bundle, protocol)

    raw_gate = next(gate for gate in report["gates"] if gate["gate"] == "saturated.raw_latency_report")
    assert raw_gate["passed"] and raw_gate["report_only"]
    assert report["passed"]


def test_percentile_definitions_are_exact() -> None:
    assert verifier._median([1, 2, 3, 4]) == Fraction(5, 2)
    assert verifier._p99(list(range(100))) == 98
    assert verifier._p99(list(range(101))) == 99


def test_fixed_cohort_real_loopback_has_exact_samples_and_clean_boundaries() -> None:
    result = benchmark.run_fixed_cohort_case(
        2,
        2,
        3,
        0.001,
        warmup_cohorts=1,
        latency_metric="call_latency_ns",
    )

    assert result["requests"] == result["successes"] == 6
    assert len(result["latency_ns"]) == 6
    assert len(result["wave_makespan_ns"]) == 3
    assert result["boundary_checks_supported"]
    assert result["boundary_checks"] == 4
    assert result["boundary_checks_all_clean"]
    assert result["server_max_active"] == 2
    assert result["server_requests"] == 6
    assert result["server_attempts"] == 6
    assert result["server_connections"] == 2
    assert result["unique_responses"] == 6
    assert result["duplicate_responses"] == 0
    assert result["missing_responses"] == 0
    assert result["unexpected_responses"] == 0
    assert result["cross_request_completions"] == 0
    assert result["cross_generation_completions"] == 0


def test_saturated_case_resets_server_activity_after_parallel_setup() -> None:
    result = benchmark.run_case(2, 1, 4, 0.001, warmup_requests=2)

    assert result["server_requests"] == 4
    assert result["server_max_active"] == 1


def test_campaign_runner_declares_every_cell_before_packing_bundle(tmp_path, monkeypatch) -> None:
    protocol = _small_protocol()
    baseline_root = tmp_path / "baseline"
    baseline_root.mkdir()
    current_root = verifier.TOOL_ROOT
    output_dir = tmp_path / "campaign"
    templates = _bundle(protocol)["trials"]

    def fake_identity(root: Path) -> tuple[str, bool]:
        return (protocol.baseline_sha, False) if root == baseline_root else (CURRENT_SHA, False)

    def fake_run(command, **kwargs):
        index = int(command[command.index("--trial-index") + 1])
        output = Path(command[command.index("--output") + 1])
        declaration = json.loads((output_dir / "declaration.json").read_text(encoding="utf-8"))
        role = protocol.schedule[index]
        expected_root = baseline_root if role == "baseline" else current_root
        assert Path(kwargs["cwd"]).resolve() == current_root.resolve()
        assert Path(kwargs["env"]["ELTDX_SOURCE_ROOT"]).resolve() == expected_root.resolve()
        trial = copy.deepcopy(templates[index])
        trial.update(
            {
                "protocol": protocol.name,
                "campaign_id": declaration["campaign_id"],
                "label": f"{declaration['campaign_id']}/{index:03d}",
                "trial_id": f"{declaration['campaign_id']}/{index:03d}",
                "trial_index": index,
                "role": role,
                "declaration_sha256": declaration["declaration_sha256"],
                "workload_sha256": declaration["workload_sha256"],
                "source_root": str(Path(kwargs["env"]["ELTDX_SOURCE_ROOT"]).resolve()),
                "system": declaration["system"],
                "platform": declaration["platform"],
                "python": declaration["python"],
                "config": copy.deepcopy(protocol.config),
                "config_sha256": declaration["config_sha256"],
                "implementation_sha": (
                    protocol.baseline_sha if role == "baseline" else CURRENT_SHA
                ),
                "implementation_dirty": False,
                "started_at_utc": f"2099-01-01T00:00:{index * 2:02d}Z",
                "ended_at_utc": f"2099-01-01T00:00:{index * 2 + 1:02d}Z",
            }
        )
        output.write_text(json.dumps(trial), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(verifier, "_git_identity", fake_identity)
    monkeypatch.setattr(verifier.subprocess, "run", fake_run)

    declaration = verifier.declare_campaign(
        baseline_root=baseline_root,
        current_root=current_root,
        output_dir=output_dir,
        campaign_id="runner-test",
        protocol=protocol,
    )
    report = verifier.run_campaign(
        baseline_root=baseline_root,
        current_root=current_root,
        declaration_path=output_dir / "declaration.json",
        expected_hash=declaration["declaration_sha256"],
        python_executable="python",
        protocol=protocol,
    )

    assert report["passed"]
    assert (output_dir / "declaration.json").is_file()
    assert (output_dir / "campaign_bundle.json").is_file()
    assert (output_dir / "verification_report.json").is_file()
    bundle = json.loads((output_dir / "campaign_bundle.json").read_text(encoding="utf-8"))
    assert [trial["role"] for trial in bundle["trials"]] == list(protocol.schedule)


def test_campaign_run_requires_the_externally_recorded_declaration_hash(tmp_path, monkeypatch) -> None:
    protocol = _small_protocol()
    baseline_root = tmp_path / "baseline"
    baseline_root.mkdir()
    output_dir = tmp_path / "campaign"

    monkeypatch.setattr(
        verifier,
        "_git_identity",
        lambda root: (protocol.baseline_sha, False) if root == baseline_root else (CURRENT_SHA, False),
    )
    verifier.declare_campaign(
        baseline_root=baseline_root,
        current_root=verifier.TOOL_ROOT,
        output_dir=output_dir,
        campaign_id="hash-test",
        protocol=protocol,
    )

    with pytest.raises(RuntimeError, match="externally recorded"):
        verifier.run_campaign(
            baseline_root=baseline_root,
            current_root=verifier.TOOL_ROOT,
            declaration_path=output_dir / "declaration.json",
            expected_hash="0" * 64,
            python_executable="python",
            protocol=protocol,
        )
    assert not list(output_dir.glob("trial_*.json"))


def test_campaign_runner_records_timeout_as_terminal_cell_failure(tmp_path, monkeypatch) -> None:
    protocol = _small_protocol()
    baseline_root = tmp_path / "baseline"
    baseline_root.mkdir()
    output_dir = tmp_path / "campaign"

    monkeypatch.setattr(
        verifier,
        "_git_identity",
        lambda root: (protocol.baseline_sha, False) if root == baseline_root else (CURRENT_SHA, False),
    )
    declaration = verifier.declare_campaign(
        baseline_root=baseline_root,
        current_root=verifier.TOOL_ROOT,
        output_dir=output_dir,
        campaign_id="timeout-test",
        protocol=protocol,
    )
    monkeypatch.setattr(
        verifier.subprocess,
        "run",
        lambda command, **_kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired(command, 1, stderr="timed out")
        ),
    )

    with pytest.raises(RuntimeError, match="output retained"):
        verifier.run_campaign(
            baseline_root=baseline_root,
            current_root=verifier.TOOL_ROOT,
            declaration_path=output_dir / "declaration.json",
            expected_hash=declaration["declaration_sha256"],
            python_executable="python",
            protocol=protocol,
        )
    failure = json.loads(next(output_dir.glob("failure_*.json")).read_text(encoding="utf-8"))
    assert failure["error_type"] == "TimeoutExpired"
    assert failure["stderr"] == "timed out"


def test_campaign_declaration_rejects_the_wrong_operating_system(tmp_path) -> None:
    protocol = replace(
        _small_protocol(),
        required_system="Linux" if platform.system() != "Linux" else "Windows",
    )
    baseline_root = tmp_path / "baseline"
    baseline_root.mkdir()

    with pytest.raises(RuntimeError, match="campaign requires"):
        verifier.declare_campaign(
            baseline_root=baseline_root,
            current_root=verifier.TOOL_ROOT,
            output_dir=tmp_path / "campaign",
            campaign_id="wrong-system",
            protocol=protocol,
        )


class _FailingSubmitExecutor:
    def __init__(self, workers: int, fail_at: int) -> None:
        self.inner = ThreadPoolExecutor(max_workers=workers)
        self.fail_at = fail_at
        self.calls = 0

    def submit(self, function, *args):
        self.calls += 1
        if self.calls == self.fail_at:
            raise RuntimeError("submit failed")
        return self.inner.submit(function, *args)


def test_partial_prestart_submit_aborts_already_registered_future() -> None:
    executor = _FailingSubmitExecutor(2, fail_at=2)
    try:
        with pytest.raises(RuntimeError, match="submit failed"):
            benchmark._prestart_executor(executor, 2)
    finally:
        executor.inner.shutdown(wait=True, cancel_futures=True)


def test_partial_cohort_submit_keeps_future_for_cleanup() -> None:
    executor = _FailingSubmitExecutor(2, fail_at=2)
    active = []

    class Transport:
        def execute(self, _command, payload):
            return _benchmark_chunk(payload)

    try:
        with pytest.raises(RuntimeError, match="submit failed"):
            benchmark._run_cohort_wave(executor, Transport(), range(2), active)
        assert len(active) == 1
        wait(active, timeout=1)
        assert active[0].done()
    finally:
        executor.inner.shutdown(wait=True, cancel_futures=True)


def test_cohort_timeout_returns_without_a_second_full_wait(monkeypatch) -> None:
    release = threading.Event()
    active = []

    class Transport:
        def execute(self, _command, payload):
            release.wait(timeout=1)
            return _benchmark_chunk(payload)

    executor = ThreadPoolExecutor(max_workers=2)
    monkeypatch.setattr(benchmark, "_COHORT_TIMEOUT", 0.1)
    started = time.perf_counter()
    try:
        with pytest.raises(TimeoutError):
            benchmark._run_cohort_wave(executor, Transport(), range(2), active)
        elapsed = time.perf_counter() - started
        assert elapsed < 0.17
    finally:
        release.set()
        wait(active, timeout=1)
        executor.shutdown(wait=True, cancel_futures=True)


def test_benchmark_server_exit_closes_accept_and_connection_workers() -> None:
    with benchmark.BenchmarkServer(0) as server:
        client = socket.create_connection(tuple(server._listener.getsockname()), timeout=1)
        deadline = time.monotonic() + 1
        while not server._workers and time.monotonic() < deadline:
            time.sleep(0.001)
        assert server._workers
    client.close()

    assert server._thread is not None and not server._thread.is_alive()
    assert all(not worker.is_alive() for worker in server._workers)
