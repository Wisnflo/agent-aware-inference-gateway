#!/usr/bin/env python3
"""Render a Markdown table from a benchmark summary.json."""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def render(summary: dict, overhead: dict | None) -> str:
    lines = [
        "| Workload | Strategy | Requests | Errors | Cache hit | prompt p50 | first p50 | first p95 | req/s | Gateway overhead |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key, item in sorted(summary.items()):
        workload, strategy = key.split("/")
        lines.append(
            "| {w} | {s} | {r} | {e} | {c}% | {p50} ms | {f50} ms | {f95} ms | {tps} | {ov} ms |".format(
                w=workload,
                s=strategy,
                r=item["requests"],
                e=item["errors"],
                c=item["cache_hit_pct"],
                p50=item["prompt_ms_p50"],
                f50=item["first_content_ms_p50"],
                f95=item["first_content_ms_p95"],
                tps=item["requests_per_s"],
                ov=item["gateway_overhead_ms_mean"],
            )
        )
    if overhead:
        lines += [
            "",
            "Direct vs gateway overhead probe (non-stream, warm cache): "
            f"direct p50 {overhead['direct_ms_p50']:.3f} ms, "
            f"gateway p50 {overhead['gateway_ms_p50']:.3f} ms, "
            f"delta {overhead['delta_ms_p50']:+.3f} ms.",
        ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", help="bench/results/<run-id>")
    args = parser.parse_args()
    run_dir = Path(args.run_dir)
    summary = json.loads((run_dir / "summary.json").read_text())
    overhead_path = run_dir / "overhead.json"
    overhead = json.loads(overhead_path.read_text()) if overhead_path.exists() else None
    print(render(summary, overhead))


if __name__ == "__main__":
    main()
