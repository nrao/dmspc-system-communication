"""
Run this script after you are done with a set of test runs.
For example, I want to visualize the 5 load tests I just performed. The output got saved to the k6-results folder (gitignored). Running this script will aggregate ALL of the results in that folder unless you adjust the parameters toward the bottom of this script.
"""

import argparse
import json
from pathlib import Path
import matplotlib.pyplot as plt


def load_results(pattern):
    files = sorted(Path("./").glob(pattern))

    if not files:
        raise FileNotFoundError(f"No files found matching: {pattern}")

    results = []

    for file in files:
        with file.open("r", encoding="utf-8") as f:
            data = json.load(f)

        metrics = data.get("metrics", {})

        duration = metrics.get("http_req_duration", {})
        failed = metrics.get("http_req_failed", {})
        requests = metrics.get("http_reqs", {})
        checks = metrics.get("checks", {})

        results.append({
            "file": file.name,
            "run_id": data.get("runId", file.stem),
            "timestamp": data.get("timestamp", ""),
            "duration_min": duration.get("min", 0),
            "duration_avg": duration.get("avg", 0),
            "duration_med": duration.get("med", 0),
            "duration_p90": duration.get("p(90)", 0),
            "duration_p95": duration.get("p(95)", 0),
            "duration_max": duration.get("max", 0),
            "failure_rate": failed.get("rate", 0) * 100,
            "request_count": requests.get("count", 0),
            "request_rate": requests.get("rate", 0),
            "check_rate": checks.get("rate", 0) * 100,
            "check_passes": checks.get("passes", 0),
            "check_fails": checks.get("fails", 0),
        })

    return results


def create_plot(results, output_file):
    # Preserve file order, which is usually chronological when filenames contain timestamps.
    labels = [result["run_id"] for result in results]
    positions = list(range(len(results)))

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle("k6 Performance Test Results", fontsize=18, fontweight="bold")

    # 1. Latency percentiles
    ax = axes[0][0]

    latency_series = {
        "Average": "duration_avg",
        "Median": "duration_med",
        "p90": "duration_p90",
        "p95": "duration_p95",
        "Maximum": "duration_max",
    }

    for name, key in latency_series.items():
        ax.plot(
            positions,
            [result[key] for result in results],
            marker="o",
            linewidth=2,
            label=name,
        )

    ax.set_title("HTTP Request Duration")
    ax.set_ylabel("Milliseconds")
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.grid(True, alpha=0.3)
    ax.legend()

    # 2. Request throughput
    ax = axes[0][1]

    request_counts = [result["request_count"] for result in results]
    request_rates = [result["request_rate"] for result in results]

    bars = ax.bar(positions, request_rates, color="#4C78A8")

    ax.set_title("Request Throughput")
    ax.set_ylabel("Requests per second")
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.grid(True, axis="y", alpha=0.3)

    for bar, count in zip(bars, request_counts):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{count:.0f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    # 3. Failure and check rates
    ax = axes[1][0]

    failure_rates = [result["failure_rate"] for result in results]
    check_rates = [result["check_rate"] for result in results]

    width = 0.35

    ax.bar(
        [position - width / 2 for position in positions],
        failure_rates,
        width=width,
        label="HTTP failure rate",
        color="#E45756",
    )

    ax.bar(
        [position + width / 2 for position in positions],
        check_rates,
        width=width,
        label="Check success rate",
        color="#59A14F",
    )

    ax.set_title("Failure and Check Rates")
    ax.set_ylabel("Percentage")
    ax.set_ylim(0, 105)
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend()

    # 4. Check pass/fail counts
    ax = axes[1][1]

    check_passes = [result["check_passes"] for result in results]
    check_fails = [result["check_fails"] for result in results]

    ax.bar(
        positions,
        check_passes,
        label="Passed",
        color="#59A14F",
    )

    ax.bar(
        positions,
        check_fails,
        bottom=check_passes,
        label="Failed",
        color="#E45756",
    )

    ax.set_title("Check Results")
    ax.set_ylabel("Number of checks")
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend()

    plt.tight_layout()
    fig.savefig(output_file, dpi=150, bbox_inches="tight")

    print(f"Created: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Visualize k6 summary JSON results."
    )

    parser.add_argument(
        "pattern",
        nargs="?",
        default="k6-results/*.json", # Adjust this if you don't want to grab every file in the k6-results folder. Maybe you only want ones named stress*.json or load*.json
        help="File pattern for k6 result files.",
    )

    parser.add_argument(
        "-o",
        "--output",
        default="k6-results/k6-loadtest-results2.png", # change name of png file here
        help="Output image filename.",
    )

    args = parser.parse_args()

    results = load_results(args.pattern)
    create_plot(results, args.output)


if __name__ == "__main__":
    main()