#!/usr/bin/env python3
"""Compare scheduler_benchmark JSONL output from baseline and scheduled runs."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path


def load(path: Path, expected_scheduling: bool) -> list[dict]:
    records = []
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("schema_version") != 2:
                raise ValueError(f"{path}:{line_number}: unsupported schema")
            if record.get("scheduling") is not expected_scheduling:
                raise ValueError(
                    f"{path}:{line_number}: unexpected scheduling mode"
                )
            records.append(record)
    if not records:
        raise ValueError(f"{path}: no records")
    return records


def median(records: list[dict], key: str) -> float:
    return statistics.median(float(record[key]) for record in records)


def class_medians(records: list[dict]) -> dict[str, dict[str, float]]:
    values: dict[str, dict[str, list[float]]] = {}
    for record in records:
        for traffic in record["classes"]:
            samples = values.setdefault(
                traffic["name"],
                {"avg_us": [], "p99_us": [], "throughput_gbps": []},
            )
            for metric in samples:
                samples[metric].append(float(traffic[metric]))
    return {
        name: {
            metric: statistics.median(samples)
            for metric, samples in metrics.items()
        }
        for name, metrics in values.items()
    }


def reduction(before: float, after: float, description: str) -> float:
    if before <= 0:
        raise ValueError(f"baseline {description} must be positive")
    return (before - after) / before * 100.0


def retention(before: float, after: float, description: str) -> float:
    if before <= 0:
        raise ValueError(f"baseline {description} must be positive")
    return after / before * 100.0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("baseline", type=Path)
    parser.add_argument("scheduled", type=Path)
    parser.add_argument("--min-p99-reduction", type=float, default=20.0)
    parser.add_argument("--min-throughput-retention", type=float, default=95.0)
    args = parser.parse_args()

    try:
        baseline = load(args.baseline, False)
        scheduled = load(args.scheduled, True)
        if len(baseline) != len(scheduled):
            raise ValueError("run counts differ")
        baseline_protocols = {record["protocol"] for record in baseline}
        scheduled_protocols = {record["protocol"] for record in scheduled}
        if (
            len(baseline_protocols) != 1
            or baseline_protocols != scheduled_protocols
        ):
            raise ValueError("baseline and scheduled protocols differ")
        baseline_classes = class_medians(baseline)
        scheduled_classes = class_medians(scheduled)
        if baseline_classes.keys() != scheduled_classes.keys():
            raise ValueError("traffic class sets differ")

        baseline_bw = median(baseline, "aggregate_throughput_gbps")
        scheduled_bw = median(scheduled, "aggregate_throughput_gbps")
        total_retention = retention(
            baseline_bw, scheduled_bw, "aggregate throughput"
        )
        passed = total_retention >= args.min_throughput_retention

        print(f"protocol: {next(iter(baseline_protocols))}")
        for name, baseline_metrics in baseline_classes.items():
            scheduled_metrics = scheduled_classes[name]
            avg_reduction = reduction(
                baseline_metrics["avg_us"],
                scheduled_metrics["avg_us"],
                f"Avg for {name}",
            )
            p99_reduction = reduction(
                baseline_metrics["p99_us"],
                scheduled_metrics["p99_us"],
                f"P99 for {name}",
            )
            bandwidth_retention = retention(
                baseline_metrics["throughput_gbps"],
                scheduled_metrics["throughput_gbps"],
                f"bandwidth for {name}",
            )
            print(name)
            print(
                f"  Avg:       {baseline_metrics['avg_us']:.2f} -> "
                f"{scheduled_metrics['avg_us']:.2f} us "
                f"({avg_reduction:.2f}% reduction)"
            )
            print(
                f"  P99:       {baseline_metrics['p99_us']:.2f} -> "
                f"{scheduled_metrics['p99_us']:.2f} us "
                f"({p99_reduction:.2f}% reduction)"
            )
            print(
                "  Bandwidth: "
                f"{baseline_metrics['throughput_gbps']:.6f} -> "
                f"{scheduled_metrics['throughput_gbps']:.6f} GB/s "
                f"({bandwidth_retention:.2f}% retained)"
            )
            if name.startswith("foreground-"):
                passed = passed and p99_reduction >= args.min_p99_reduction
        print(
            f"aggregate throughput: {baseline_bw:.6f} -> "
            f"{scheduled_bw:.6f} GB/s ({total_retention:.2f}% retained)"
        )
        print("result: " + ("PASS" if passed else "FAIL"))
        return 0 if passed else 1
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
