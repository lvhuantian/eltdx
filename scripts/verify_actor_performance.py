"""Run and verify the prospective strict-FIFO performance campaign."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path
from typing import Any, Mapping, Sequence


TOOL_ROOT = Path(__file__).resolve().parents[1]
BENCHMARK_PATH = Path(__file__).with_name("benchmark_actor_transport.py").resolve()
BASELINE_SHA = "71089c0a2867a75dc79aa2c340213f4e3845b6e3"
FIFO_PROTOCOL = "fifo-v1"
FIFO_SCHEDULE = (
    "baseline",
    "current",
    "current",
    "baseline",
    "current",
    "baseline",
    "baseline",
    "current",
)
THROUGHPUT_CASES = ("sequential", "saturated")
LATENCY_CASES = ("sequential", "no_backlog")
EXPECTED_MAX_ACTIVE = {
    "sequential": 1,
    "saturated": 4,
    "no_backlog": 4,
    "contended_wave": 4,
}
BUNDLE_KEYS = {"schema", "kind", "declaration", "trials"}
DECLARATION_KEYS = {
    "protocol",
    "campaign_id",
    "declared_at_utc",
    "schedule",
    "expected_trials",
    "baseline_sha",
    "current_sha",
    "workload_sha256",
    "verifier_sha256",
    "system",
    "platform",
    "python",
    "config",
    "config_sha256",
    "gates",
    "stopping_rule",
    "declaration_sha256",
}
TRIAL_KEYS = {
    "schema",
    "kind",
    "protocol",
    "label",
    "campaign_id",
    "trial_id",
    "trial_index",
    "attempt",
    "role",
    "declaration_sha256",
    "workload_sha256",
    "source_root",
    "system",
    "platform",
    "python",
    "started_at_utc",
    "ended_at_utc",
    "config",
    "config_sha256",
    "implementation_sha",
    "implementation_dirty",
    "cases",
}
CASE_EVIDENCE_KEYS = {
    "successes",
    "errors",
    "worker_threads",
    "server_max_active",
    "server_requests",
    "elapsed_ns",
    "seconds",
    "throughput_rps",
    "latency_p50_ns",
    "latency_p99_ns",
    "latency_p50_ms",
    "latency_p99_ms",
    "latency_ns",
}
COHORT_EVIDENCE_KEYS = {
    "boundary_checks_supported",
    "boundary_checks",
    "boundary_checks_all_clean",
    "call_latency_ns",
    "cohort_latency_ns",
    "wave_makespan_ns",
    "wall_elapsed_ns",
}


def fifo_v1_config() -> dict[str, Any]:
    return {
        "delay_ns": 5_000_000,
        "cases": {
            "sequential": {
                "mode": "saturated",
                "pool_size": 1,
                "concurrency": 1,
                "requests": 10_000,
                "warmup_requests": 1_000,
                "latency_metric": "call_latency_ns",
            },
            "saturated": {
                "mode": "saturated",
                "pool_size": 4,
                "concurrency": 100,
                "requests": 100_000,
                "warmup_requests": 1_000,
                "latency_metric": "call_latency_ns",
            },
            "no_backlog": {
                "mode": "fixed_cohort",
                "pool_size": 4,
                "cohort_size": 4,
                "measured_cohorts": 2_500,
                "warmup_cohorts": 100,
                "requests": 10_000,
                "latency_metric": "call_latency_ns",
            },
            "contended_wave": {
                "mode": "fixed_cohort",
                "pool_size": 4,
                "cohort_size": 100,
                "measured_cohorts": 50,
                "warmup_cohorts": 10,
                "requests": 5_000,
                "latency_metric": "cohort_latency_ns",
            },
        },
    }


@dataclass(frozen=True)
class CampaignProtocol:
    name: str
    schedule: tuple[str, ...]
    config: Mapping[str, Any]
    baseline_sha: str
    throughput_cases: tuple[str, ...]
    latency_cases: tuple[str, ...]
    expected_max_active: Mapping[str, int]
    required_system: str = "Windows"
    throughput_minimum: Fraction = Fraction(95, 100)
    latency_fraction: Fraction = Fraction(1, 10)
    latency_floor_ns: int = 200_000


FIFO_V1 = CampaignProtocol(
    name=FIFO_PROTOCOL,
    schedule=FIFO_SCHEDULE,
    config=fifo_v1_config(),
    baseline_sha=BASELINE_SHA,
    throughput_cases=THROUGHPUT_CASES,
    latency_cases=LATENCY_CASES,
    expected_max_active=EXPECTED_MAX_ACTIVE,
)

STOPPING_RULE = (
    "run all declared trials once; any missing, extra, reordered, overlapping, or retried cell fails"
)


def frozen_gates(protocol: CampaignProtocol) -> dict[str, Any]:
    return {
        "throughput_minimum": {
            "numerator": protocol.throughput_minimum.numerator,
            "denominator": protocol.throughput_minimum.denominator,
        },
        "latency_delta": {
            "fraction_numerator": protocol.latency_fraction.numerator,
            "fraction_denominator": protocol.latency_fraction.denominator,
            "floor_ns": protocol.latency_floor_ns,
        },
        "throughput_cases": list(protocol.throughput_cases),
        "latency_cases": list(protocol.latency_cases),
        "other_raw_latency": "report-only",
    }


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp is not a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must use an explicit UTC offset")
    return parsed


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _number_fraction(value: Any) -> Fraction | None:
    if _is_int(value):
        return Fraction(value, 1)
    if isinstance(value, float):
        return Fraction(str(value))
    return None


def _median(values: Sequence[int]) -> Fraction:
    if not values:
        raise ValueError("latency sample is empty")
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return Fraction(ordered[middle], 1)
    return Fraction(ordered[middle - 1] + ordered[middle], 2)


def _p99(values: Sequence[int]) -> Fraction:
    if not values:
        raise ValueError("latency sample is empty")
    ordered = sorted(values)
    return Fraction(ordered[((len(ordered) - 1) * 99) // 100], 1)


def _fraction_json(value: Fraction, *, scale: int = 1) -> float:
    return round(float(value / scale), 6)


def _summary(values: Sequence[int]) -> dict[str, float]:
    return {
        "p50_ms": _fraction_json(_median(values), scale=1_000_000),
        "p99_ms": _fraction_json(_p99(values), scale=1_000_000),
    }


def _error(errors: list[str], location: str, message: str) -> None:
    errors.append(f"{location}: {message}")


def _validate_case(
    case: Any,
    expected: Mapping[str, Any],
    *,
    role: str,
    name: str,
    location: str,
    expected_max_active: int,
    errors: list[str],
) -> list[int] | None:
    if not isinstance(case, Mapping):
        _error(errors, location, "case is not an object")
        return None
    allowed_keys = set(expected) | CASE_EVIDENCE_KEYS
    if expected["mode"] == "fixed_cohort":
        allowed_keys |= COHORT_EVIDENCE_KEYS
    if set(case) != allowed_keys:
        _error(
            errors,
            location,
            f"case keys differ: extra={sorted(set(case) - allowed_keys)!r}, missing={sorted(allowed_keys - set(case))!r}",
        )
    for field, expected_value in expected.items():
        if case.get(field) != expected_value:
            _error(errors, location, f"{field}={case.get(field)!r}, expected {expected_value!r}")
    requests = expected["requests"]
    if not _is_int(case.get("successes")) or case.get("successes") != requests:
        _error(errors, location, f"successes={case.get('successes')!r}, expected {requests}")
    if case.get("errors") != []:
        _error(errors, location, f"request errors are not empty: {case.get('errors')!r}")
    elapsed_ns = case.get("elapsed_ns")
    if not _is_int(elapsed_ns) or elapsed_ns <= 0:
        _error(errors, location, f"invalid elapsed_ns: {elapsed_ns!r}")
    if not _is_int(case.get("server_max_active")) or case.get("server_max_active") != expected_max_active:
        _error(
            errors,
            location,
            f"server_max_active={case.get('server_max_active')!r}, expected {expected_max_active}",
        )
    if not _is_int(case.get("server_requests")) or case.get("server_requests") != requests:
        _error(errors, location, f"server_requests={case.get('server_requests')!r}, expected {requests}")
    expected_workers = expected.get("concurrency", expected.get("cohort_size"))
    if not _is_int(case.get("worker_threads")) or case.get("worker_threads") != expected_workers:
        _error(errors, location, f"worker_threads={case.get('worker_threads')!r}, expected {expected_workers}")
    latencies = case.get("latency_ns")
    if not isinstance(latencies, list):
        _error(errors, location, "latency_ns is not an array")
        return None
    if len(latencies) != requests:
        _error(errors, location, f"latency count={len(latencies)}, expected {requests}")
        return None
    if any(not _is_int(value) or value <= 0 for value in latencies):
        _error(errors, location, "latency_ns contains a non-positive or non-integer value")
        return None
    if _is_int(elapsed_ns) and elapsed_ns > 0:
        if max(latencies) > elapsed_ns:
            _error(errors, location, "a request latency exceeds the complete measured interval")
        if sum(latencies) > expected_workers * elapsed_ns:
            _error(errors, location, "latency integral exceeds the concurrency-time bound")
    expected_p50 = _median(latencies)
    reported_p50 = case.get("latency_p50_ns")
    if _number_fraction(reported_p50) != expected_p50:
        _error(errors, location, f"latency_p50_ns={reported_p50!r}, expected {expected_p50}")
    expected_p99 = int(_p99(latencies))
    if not _is_int(case.get("latency_p99_ns")) or case.get("latency_p99_ns") != expected_p99:
        _error(
            errors,
            location,
            f"latency_p99_ns={case.get('latency_p99_ns')!r}, expected {expected_p99}",
        )
    expected_seconds = round(elapsed_ns / 1_000_000_000, 6) if _is_int(elapsed_ns) else None
    expected_throughput = (
        round(requests * 1_000_000_000 / elapsed_ns, 3)
        if _is_int(elapsed_ns) and elapsed_ns > 0
        else None
    )
    expected_p50_ms = round(float(expected_p50) / 1_000_000, 4)
    expected_p99_ms = round(expected_p99 / 1_000_000, 4)
    for field, expected_value in (
        ("seconds", expected_seconds),
        ("throughput_rps", expected_throughput),
        ("latency_p50_ms", expected_p50_ms),
        ("latency_p99_ms", expected_p99_ms),
    ):
        reported = case.get(field)
        if isinstance(reported, bool) or reported != expected_value:
            _error(errors, location, f"{field}={reported!r}, expected {expected_value!r}")
    if expected["mode"] == "fixed_cohort":
        for raw_field in ("call_latency_ns", "cohort_latency_ns"):
            raw = case.get(raw_field)
            if not isinstance(raw, list) or len(raw) != requests:
                _error(errors, location, f"{raw_field} does not contain {requests} samples")
            elif any(not _is_int(value) or value <= 0 for value in raw):
                _error(errors, location, f"{raw_field} contains an invalid sample")
        selected = case.get(expected["latency_metric"])
        if selected != latencies:
            _error(errors, location, "latency_ns does not match the declared latency metric")
        wave_makespans = case.get("wave_makespan_ns")
        if not isinstance(wave_makespans, list) or len(wave_makespans) != expected["measured_cohorts"]:
            _error(errors, location, "wave_makespan_ns count does not match measured cohorts")
        elif any(not _is_int(value) or value <= 0 for value in wave_makespans):
            _error(errors, location, "wave_makespan_ns contains an invalid sample")
        else:
            if _is_int(elapsed_ns) and elapsed_ns != sum(wave_makespans):
                _error(errors, location, "elapsed_ns does not equal the sum of wave makespans")
            cohort_values = case.get("cohort_latency_ns")
            call_values = case.get("call_latency_ns")
            if isinstance(cohort_values, list) and isinstance(call_values, list):
                if len(cohort_values) == len(call_values) == requests:
                    if any(call > cohort for call, cohort in zip(call_values, cohort_values)):
                        _error(errors, location, "call latency exceeds its cohort completion latency")
                    width = expected["cohort_size"]
                    for wave_index, makespan in enumerate(wave_makespans):
                        start = wave_index * width
                        wave = cohort_values[start : start + width]
                        if len(wave) != width or max(wave) != makespan:
                            _error(errors, location, f"wave {wave_index} makespan mismatch")
                            break
        wall_elapsed_ns = case.get("wall_elapsed_ns")
        if not _is_int(wall_elapsed_ns) or not _is_int(elapsed_ns) or wall_elapsed_ns < elapsed_ns:
            _error(errors, location, "wall_elapsed_ns is shorter than active cohort elapsed_ns")
        if case.get("boundary_checks_all_clean") is not True:
            _error(errors, location, "cohort boundary cleanup is not true")
        supported = case.get("boundary_checks_supported")
        if role == "current" and supported is not True:
            _error(errors, location, "current implementation did not expose Broker boundary checks")
        if supported is True:
            expected_checks = expected["warmup_cohorts"] + expected["measured_cohorts"]
            if not _is_int(case.get("boundary_checks")) or case.get("boundary_checks") != expected_checks:
                _error(errors, location, f"boundary_checks must equal {expected_checks}")
    return latencies


def verify_campaign(
    bundle: Mapping[str, Any],
    protocol: CampaignProtocol = FIFO_V1,
) -> dict[str, Any]:
    errors: list[str] = []
    gates: list[dict[str, Any]] = []
    trial_summaries: list[dict[str, Any]] = []
    if not isinstance(bundle, Mapping):
        return {"schema": 1, "protocol": protocol.name, "passed": False, "errors": ["bundle is not an object"], "gates": []}
    if set(bundle) != BUNDLE_KEYS:
        _error(errors, "bundle", "bundle keys differ from the frozen schema")
    if bundle.get("schema") != 3 or bundle.get("kind") != "actor-performance-campaign":
        _error(errors, "bundle", "unexpected schema or kind")
    declaration = bundle.get("declaration")
    if not isinstance(declaration, Mapping):
        _error(errors, "declaration", "missing declaration object")
        return {"schema": 1, "protocol": protocol.name, "passed": False, "errors": errors, "gates": []}
    if set(declaration) != DECLARATION_KEYS:
        _error(errors, "declaration", "declaration keys differ from the frozen schema")
    declaration_without_hash = dict(declaration)
    declared_hash = declaration_without_hash.pop("declaration_sha256", None)
    calculated_hash = canonical_sha256(declaration_without_hash)
    if declared_hash != calculated_hash:
        _error(errors, "declaration", "declaration_sha256 mismatch")
    expected_config = dict(protocol.config)
    if declaration.get("protocol") != protocol.name:
        _error(errors, "declaration", f"protocol must be {protocol.name}")
    if declaration.get("schedule") != list(protocol.schedule):
        _error(errors, "declaration", "schedule differs from the frozen protocol")
    if declaration.get("expected_trials") != len(protocol.schedule):
        _error(errors, "declaration", "expected_trials differs from the frozen protocol")
    if declaration.get("baseline_sha") != protocol.baseline_sha:
        _error(errors, "declaration", "baseline SHA mismatch")
    current_sha = declaration.get("current_sha")
    if (
        not isinstance(current_sha, str)
        or len(current_sha) != 40
        or any(character not in "0123456789abcdef" for character in current_sha)
        or current_sha == protocol.baseline_sha
    ):
        _error(errors, "declaration", "invalid current SHA")
    if declaration.get("config") != expected_config:
        _error(errors, "declaration", "config differs from the frozen protocol")
    expected_config_hash = canonical_sha256(expected_config)
    if declaration.get("config_sha256") != expected_config_hash:
        _error(errors, "declaration", "config_sha256 mismatch")
    if declaration.get("gates") != frozen_gates(protocol):
        _error(errors, "declaration", "gates differ from the frozen protocol")
    if declaration.get("stopping_rule") != STOPPING_RULE:
        _error(errors, "declaration", "stopping_rule differs from the frozen protocol")
    for field in ("campaign_id", "workload_sha256", "platform", "python", "declared_at_utc"):
        if not isinstance(declaration.get(field), str) or not declaration[field]:
            _error(errors, "declaration", f"missing {field}")
    if declaration.get("system") != protocol.required_system:
        _error(errors, "declaration", f"system must be {protocol.required_system}")
    if declaration.get("system") != platform.system():
        _error(errors, "declaration", "system does not match the verifier host")
    if declaration.get("platform") != platform.platform():
        _error(errors, "declaration", "platform does not match the verifier host")
    if declaration.get("python") != platform.python_version():
        _error(errors, "declaration", "Python does not match the verifier process")
    expected_verifier_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if declaration.get("verifier_sha256") != expected_verifier_hash:
        _error(errors, "declaration", "verifier_sha256 mismatch")
    expected_workload_hash = hashlib.sha256(BENCHMARK_PATH.read_bytes()).hexdigest()
    if declaration.get("workload_sha256") != expected_workload_hash:
        _error(errors, "declaration", "workload_sha256 does not match the adjacent producer")
    try:
        declared_at = _parse_utc(declaration.get("declared_at_utc"))
    except ValueError as exc:
        _error(errors, "declaration", str(exc))
        declared_at = None

    trials = bundle.get("trials")
    if not isinstance(trials, list):
        _error(errors, "bundle", "trials is not an array")
        trials = []
    if len(trials) != len(protocol.schedule):
        _error(errors, "bundle", f"trial count={len(trials)}, expected {len(protocol.schedule)}")
    seen_ids: set[str] = set()
    seen_indexes: set[int] = set()
    raw_by_role: dict[str, dict[str, list[int]]] = {
        role: {name: [] for name in expected_config["cases"]}
        for role in ("baseline", "current")
    }
    elapsed_by_role: dict[str, dict[str, int]] = {
        role: {name: 0 for name in expected_config["cases"]}
        for role in ("baseline", "current")
    }
    requests_by_role: dict[str, dict[str, int]] = {
        role: {name: 0 for name in expected_config["cases"]}
        for role in ("baseline", "current")
    }

    previous_ended = declared_at
    for expected_index, role in enumerate(protocol.schedule):
        if expected_index >= len(trials):
            continue
        trial = trials[expected_index]
        location = f"trial[{expected_index}]"
        if not isinstance(trial, Mapping):
            _error(errors, location, "trial is not an object")
            continue
        if set(trial) != TRIAL_KEYS:
            _error(errors, location, "trial keys differ from the frozen schema")
        if trial.get("schema") != 3 or trial.get("kind") != "actor-performance-trial":
            _error(errors, location, "unexpected trial schema or kind")
        index = trial.get("trial_index")
        trial_id = trial.get("trial_id")
        expected_id = f"{declaration.get('campaign_id')}/{expected_index:03d}"
        if not _is_int(index) or index != expected_index:
            _error(errors, location, f"trial_index={index!r}, expected {expected_index}")
        if index in seen_indexes:
            _error(errors, location, "duplicate trial_index")
        if _is_int(index):
            seen_indexes.add(index)
        if trial_id != expected_id:
            _error(errors, location, f"trial_id={trial_id!r}, expected {expected_id!r}")
        if trial_id in seen_ids:
            _error(errors, location, "duplicate trial_id")
        if isinstance(trial_id, str):
            seen_ids.add(trial_id)
        if not _is_int(trial.get("attempt")) or trial.get("attempt") != 1:
            _error(errors, location, "attempt must be exactly 1")
        if trial.get("role") != role:
            _error(errors, location, f"role={trial.get('role')!r}, expected {role!r}")
        if trial.get("campaign_id") != declaration.get("campaign_id"):
            _error(errors, location, "campaign_id mismatch")
        if trial.get("declaration_sha256") != declared_hash:
            _error(errors, location, "declaration hash mismatch")
        if trial.get("protocol") != protocol.name:
            _error(errors, location, "protocol mismatch")
        if trial.get("implementation_dirty") is not False:
            _error(errors, location, "implementation_dirty must be false")
        expected_sha = declaration.get(f"{role}_sha")
        if trial.get("implementation_sha") != expected_sha:
            _error(errors, location, "implementation SHA mismatch")
        for field in ("workload_sha256", "system", "platform", "python"):
            if trial.get(field) != declaration.get(field):
                _error(errors, location, f"{field} mismatch")
        if trial.get("config") != expected_config or trial.get("config_sha256") != expected_config_hash:
            _error(errors, location, "trial config mismatch")
        try:
            started = _parse_utc(trial.get("started_at_utc"))
            ended = _parse_utc(trial.get("ended_at_utc"))
            if ended < started or (declared_at is not None and started < declared_at):
                _error(errors, location, "trial timestamps are out of order")
            if previous_ended is not None and started < previous_ended:
                _error(errors, location, "trial overlaps or predates the preceding cell")
            previous_ended = ended
        except ValueError as exc:
            _error(errors, location, str(exc))
        cases = trial.get("cases")
        if not isinstance(cases, Mapping) or set(cases) != set(expected_config["cases"]):
            _error(errors, location, "case names differ from the frozen protocol")
            continue
        summary_cases: dict[str, Any] = {}
        for name, case_protocol in expected_config["cases"].items():
            case_location = f"{location}.{name}"
            latencies = _validate_case(
                cases[name],
                case_protocol,
                role=role,
                name=name,
                location=case_location,
                expected_max_active=protocol.expected_max_active[name],
                errors=errors,
            )
            if latencies is None:
                continue
            raw_by_role[role][name].extend(latencies)
            elapsed = cases[name].get("elapsed_ns")
            if _is_int(elapsed) and elapsed > 0:
                elapsed_by_role[role][name] += elapsed
                requests_by_role[role][name] += case_protocol["requests"]
            summary_cases[name] = _summary(latencies)
        trial_summaries.append({"index": expected_index, "role": role, "cases": summary_cases})

    if not errors:
        for name in protocol.throughput_cases:
            baseline_requests = requests_by_role["baseline"][name]
            current_requests = requests_by_role["current"][name]
            baseline_elapsed = elapsed_by_role["baseline"][name]
            current_elapsed = elapsed_by_role["current"][name]
            ratio = Fraction(current_requests * baseline_elapsed, baseline_requests * current_elapsed)
            passed = ratio >= protocol.throughput_minimum
            gates.append(
                {
                    "gate": f"{name}.throughput",
                    "passed": passed,
                    "baseline_rps": round(baseline_requests * 1_000_000_000 / baseline_elapsed, 6),
                    "current_rps": round(current_requests * 1_000_000_000 / current_elapsed, 6),
                    "ratio": round(float(ratio), 6),
                    "minimum": float(protocol.throughput_minimum),
                }
            )
        for name in expected_config["cases"]:
            baseline_values = raw_by_role["baseline"][name]
            current_values = raw_by_role["current"][name]
            baseline_summary = _summary(baseline_values)
            current_summary = _summary(current_values)
            if name not in protocol.latency_cases:
                gates.append(
                    {
                        "gate": f"{name}.raw_latency_report",
                        "passed": True,
                        "report_only": True,
                        "baseline": baseline_summary,
                        "current": current_summary,
                    }
                )
                continue
            for quantile, statistic in (("p50", _median), ("p99", _p99)):
                baseline_value = statistic(baseline_values)
                current_value = statistic(current_values)
                delta = current_value - baseline_value
                allowance = max(protocol.latency_fraction * baseline_value, protocol.latency_floor_ns)
                passed = delta <= allowance
                gates.append(
                    {
                        "gate": f"{name}.{quantile}",
                        "passed": passed,
                        "baseline_ms": _fraction_json(baseline_value, scale=1_000_000),
                        "current_ms": _fraction_json(current_value, scale=1_000_000),
                        "delta_ms": _fraction_json(delta, scale=1_000_000),
                        "allowance_ms": _fraction_json(allowance, scale=1_000_000),
                    }
                )
    passed = not errors and all(gate["passed"] for gate in gates)
    return {
        "schema": 1,
        "protocol": protocol.name,
        "campaign_id": declaration.get("campaign_id"),
        "passed": passed,
        "errors": errors,
        "gates": gates,
        "trial_summaries": trial_summaries,
    }


def _git_identity(root: Path) -> tuple[str, bool]:
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    return sha, dirty


def _write_json(path: Path, value: Any, *, compact: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if compact:
        encoded = json.dumps(value, ensure_ascii=True, separators=(",", ":")) + "\n"
    else:
        encoded = json.dumps(value, ensure_ascii=True, indent=2) + "\n"
    path.write_text(encoded, encoding="utf-8")


def _validate_source_roots(
    *,
    baseline_root: Path,
    current_root: Path,
    protocol: CampaignProtocol = FIFO_V1,
) -> tuple[str, str]:
    if platform.system() != protocol.required_system:
        raise RuntimeError(
            f"campaign requires {protocol.required_system}, current system is {platform.system()}"
        )
    if current_root.resolve() != TOOL_ROOT:
        raise RuntimeError(
            f"current source root {current_root} does not own the campaign tools at {TOOL_ROOT}"
        )
    baseline_sha, baseline_dirty = _git_identity(baseline_root)
    current_sha, current_dirty = _git_identity(current_root)
    if baseline_sha != protocol.baseline_sha or baseline_dirty:
        raise RuntimeError(f"invalid baseline source: sha={baseline_sha}, dirty={baseline_dirty}")
    if current_dirty:
        raise RuntimeError(f"current source is dirty at {current_sha}")
    return baseline_sha, current_sha


def declare_campaign(
    *,
    baseline_root: Path,
    current_root: Path,
    output_dir: Path,
    campaign_id: str,
    protocol: CampaignProtocol = FIFO_V1,
) -> dict[str, Any]:
    if output_dir.exists():
        raise FileExistsError(f"campaign output already exists: {output_dir}")
    output_dir.mkdir(parents=True)
    baseline_sha, current_sha = _validate_source_roots(
        baseline_root=baseline_root,
        current_root=current_root,
        protocol=protocol,
    )
    benchmark = BENCHMARK_PATH
    workload_sha = hashlib.sha256(benchmark.read_bytes()).hexdigest()
    config = dict(protocol.config)
    declaration: dict[str, Any] = {
        "protocol": protocol.name,
        "campaign_id": campaign_id,
        "declared_at_utc": _utc_now(),
        "schedule": list(protocol.schedule),
        "expected_trials": len(protocol.schedule),
        "baseline_sha": baseline_sha,
        "current_sha": current_sha,
        "workload_sha256": workload_sha,
        "verifier_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "system": platform.system(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "config": config,
        "config_sha256": canonical_sha256(config),
        "gates": frozen_gates(protocol),
        "stopping_rule": STOPPING_RULE,
    }
    declaration["declaration_sha256"] = canonical_sha256(declaration)
    _write_json(output_dir / "declaration.json", declaration)
    print(
        f"declared {campaign_id} {declaration['declaration_sha256']} with {len(protocol.schedule)} trials",
        flush=True,
    )
    return declaration


def _validate_declaration_for_run(
    declaration: Mapping[str, Any],
    *,
    expected_hash: str,
    baseline_root: Path,
    current_root: Path,
    protocol: CampaignProtocol,
) -> None:
    declaration_without_hash = dict(declaration)
    declared_hash = declaration_without_hash.pop("declaration_sha256", None)
    if declared_hash != expected_hash or canonical_sha256(declaration_without_hash) != expected_hash:
        raise RuntimeError("declaration does not match the externally recorded expected hash")
    baseline_sha, current_sha = _validate_source_roots(
        baseline_root=baseline_root,
        current_root=current_root,
        protocol=protocol,
    )
    expected = {
        "protocol": protocol.name,
        "schedule": list(protocol.schedule),
        "expected_trials": len(protocol.schedule),
        "baseline_sha": baseline_sha,
        "current_sha": current_sha,
        "workload_sha256": hashlib.sha256(BENCHMARK_PATH.read_bytes()).hexdigest(),
        "verifier_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "system": platform.system(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "config": dict(protocol.config),
        "config_sha256": canonical_sha256(dict(protocol.config)),
        "gates": frozen_gates(protocol),
        "stopping_rule": STOPPING_RULE,
    }
    for field, value in expected.items():
        if declaration.get(field) != value:
            raise RuntimeError(f"declaration field {field} no longer matches the frozen environment")
    if not isinstance(declaration.get("campaign_id"), str) or not declaration["campaign_id"]:
        raise RuntimeError("declaration campaign_id is missing")
    _parse_utc(declaration.get("declared_at_utc"))


def run_campaign(
    *,
    declaration_path: Path,
    expected_hash: str,
    baseline_root: Path,
    current_root: Path,
    python_executable: str,
    protocol: CampaignProtocol = FIFO_V1,
) -> dict[str, Any]:
    declaration_path = declaration_path.resolve()
    output_dir = declaration_path.parent
    declaration = json.loads(declaration_path.read_text(encoding="utf-8"))
    _validate_declaration_for_run(
        declaration,
        expected_hash=expected_hash,
        baseline_root=baseline_root,
        current_root=current_root,
        protocol=protocol,
    )
    existing = list(output_dir.iterdir())
    if len(existing) != 1 or existing[0].resolve() != declaration_path:
        raise FileExistsError(
            f"campaign directory must contain only its declaration before sampling: {existing!r}"
        )
    campaign_id = declaration["campaign_id"]
    benchmark = BENCHMARK_PATH
    print(f"running declared campaign {campaign_id} hash={expected_hash}", flush=True)
    trials: list[Mapping[str, Any]] = []
    for index, role in enumerate(protocol.schedule):
        source_root = baseline_root if role == "baseline" else current_root
        trial_path = output_dir / f"trial_{index:03d}_{role}.json"
        command = [
            python_executable,
            str(benchmark),
            "--label",
            f"{campaign_id}/{index:03d}",
            "--fifo-trial",
            "--campaign-id",
            campaign_id,
            "--trial-index",
            str(index),
            "--role",
            role,
            "--declaration-sha256",
            declaration["declaration_sha256"],
            "--output",
            str(trial_path),
        ]
        env = os.environ.copy()
        env["ELTDX_SOURCE_ROOT"] = str(source_root)
        started = _utc_now()
        completed: subprocess.CompletedProcess[str] | None = None
        try:
            completed = subprocess.run(
                command,
                cwd=current_root,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                timeout=900,
            )
            if completed.returncode:
                raise RuntimeError(f"child exited with status {completed.returncode}")
            trial = json.loads(trial_path.read_text(encoding="utf-8"))
        except BaseException as exc:
            stderr = completed.stderr if completed is not None else getattr(exc, "stderr", None)
            if isinstance(stderr, bytes):
                stderr = stderr.decode(errors="replace")
            failure = {
                "campaign_id": campaign_id,
                "trial_index": index,
                "role": role,
                "started_at_utc": started,
                "failed_at_utc": _utc_now(),
                "error_type": type(exc).__name__,
                "error": str(exc),
                "returncode": completed.returncode if completed is not None else None,
                "stderr": stderr,
            }
            _write_json(output_dir / f"failure_{index:03d}_{role}.json", failure)
            raise RuntimeError(f"campaign trial {index} ({role}) failed; output retained") from exc
        trials.append(trial)
        print(f"completed trial {index + 1}/{len(protocol.schedule)} role={role}", flush=True)
    bundle = {
        "schema": 3,
        "kind": "actor-performance-campaign",
        "declaration": declaration,
        "trials": trials,
    }
    bundle_path = output_dir / "campaign_bundle.json"
    _write_json(bundle_path, bundle, compact=True)
    report = verify_campaign(bundle, protocol)
    report["bundle_sha256"] = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
    _write_json(output_dir / "verification_report.json", report)
    print(f"campaign passed={report['passed']} bundle_sha256={report['bundle_sha256']}", flush=True)
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    declare_parser = subparsers.add_parser("declare")
    declare_parser.add_argument("--baseline-root", type=Path, required=True)
    declare_parser.add_argument("--current-root", type=Path, required=True)
    declare_parser.add_argument("--output-dir", type=Path, required=True)
    declare_parser.add_argument("--campaign-id", required=True)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--baseline-root", type=Path, required=True)
    run_parser.add_argument("--current-root", type=Path, required=True)
    run_parser.add_argument("--declaration", type=Path, required=True)
    run_parser.add_argument("--expected-hash", required=True)
    run_parser.add_argument("--python", default=sys.executable)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--bundle", type=Path, required=True)
    verify_parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.command == "declare":
        declaration = declare_campaign(
            baseline_root=args.baseline_root.resolve(),
            current_root=args.current_root.resolve(),
            output_dir=args.output_dir.resolve(),
            campaign_id=args.campaign_id,
        )
        print(json.dumps(declaration, ensure_ascii=True, indent=2))
        return
    if args.command == "run":
        report = run_campaign(
            baseline_root=args.baseline_root.resolve(),
            current_root=args.current_root.resolve(),
            declaration_path=args.declaration.resolve(),
            expected_hash=args.expected_hash,
            python_executable=args.python,
        )
    else:
        bundle = json.loads(args.bundle.read_text(encoding="utf-8"))
        report = verify_campaign(bundle)
        if args.output:
            _write_json(args.output, report)
        print(json.dumps(report, ensure_ascii=True, indent=2))
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
