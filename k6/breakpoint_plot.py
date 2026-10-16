'''
This script reads a raw k6 JSON output file and produces a summary CSV and a graph of the results.
Run this script after you have performed a breakoint test and have the raw output.
'''
import argparse
import csv
import json
import math
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def load_k6_json(path):
    records = []
    invalid_lines = 0

    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()

            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                # A crashed container may leave one incomplete final line.
                invalid_lines += 1
                continue

            if (
                record.get("type") == "Point"
                and "metric" in record
                and "data" in record
            ):
                data = record["data"]

                timestamp = data.get("time")
                value = data.get("value")

                if timestamp is None or value is None:
                    continue

                records.append(
                    {
                        "metric": record["metric"],
                        "time": pd.to_datetime(timestamp, utc=True),
                        "value": float(value),
                        "tags": data.get("tags") or {},
                    }
                )

    if invalid_lines:
        print(
            f"Warning: skipped {invalid_lines} invalid/incomplete JSON line(s). "
            "This is normal if k6 was killed while writing the file."
        )

    if not records:
        raise RuntimeError(f"No usable k6 metric points found in {path}")

    return pd.DataFrame(records)


def metric_frame(df, metric_name):
    result = df[df["metric"] == metric_name].copy()

    if result.empty:
        return result

    result = result.set_index("time").sort_index()

    # Aggregate into one-second buckets. This makes the graphs readable
    # even when k6 has many VUs producing samples.
    return result


def percentile(series, percentile_value):
    if series.empty:
        return float("nan")

    return series.quantile(percentile_value / 100.0)


def build_summary(df):
    rows = []

    for metric_name in sorted(df["metric"].unique()):
        values = df.loc[df["metric"] == metric_name, "value"]

        rows.append(
            {
                "metric": metric_name,
                "samples": len(values),
                "min": values.min(),
                "mean": values.mean(),
                "p95": percentile(values, 95),
                "p99": percentile(values, 99),
                "max": values.max(),
            }
        )

    return pd.DataFrame(rows)


def plot_results(df, output_path):
    fig, axes = plt.subplots(2, 3, figsize=(19, 10), constrained_layout=True)

    # ------------------------------------------------------------
    # 1. Latency over time
    # ------------------------------------------------------------
    duration = metric_frame(df, "http_req_duration")

    if not duration.empty:
        latency_by_time = duration["value"].resample("1s").agg(
            mean="mean",
            p95=lambda values: percentile(values, 95),
            p99=lambda values: percentile(values, 99),
        )

        axes[0, 0].plot(
            latency_by_time.index,
            latency_by_time["mean"],
            label="Mean",
        )
        axes[0, 0].plot(
            latency_by_time.index,
            latency_by_time["p95"],
            label="p95",
        )
        axes[0, 0].plot(
            latency_by_time.index,
            latency_by_time["p99"],
            label="p99",
        )

        axes[0, 0].set_title("Latency over time")
        axes[0, 0].set_ylabel("Milliseconds")
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)
    else:
        axes[0, 0].set_title("Latency over time: no data")

    # ------------------------------------------------------------
    # 2. Active VUs over time
    # ------------------------------------------------------------
    vus = metric_frame(df, "vus")

    if not vus.empty:
        vus_by_time = vus["value"].resample("1s").max()

        axes[0, 1].step(
            vus_by_time.index,
            vus_by_time,
            where="post",
        )
        axes[0, 1].set_title("Active virtual users")
        axes[0, 1].set_ylabel("VUs")
        axes[0, 1].set_ylim(bottom=0)
        axes[0, 1].grid(True, alpha=0.3)
    else:
        axes[0, 1].set_title("Active VUs: no data")

    # ------------------------------------------------------------
    # 3. Request failure rate
    # ------------------------------------------------------------
    failed = metric_frame(df, "http_req_failed")

    if not failed.empty:
        failure_rate = failed["value"].resample("1s").mean().mul(100)

        axes[0, 2].plot(
            failure_rate.index,
            failure_rate,
        )
        axes[0, 2].set_title("HTTP request failure rate")
        axes[0, 2].set_ylabel("Failure rate (%)")
        axes[0, 2].set_ylim(bottom=0)
        axes[0, 2].grid(True, alpha=0.3)
    else:
        axes[0, 2].set_title("Failure rate: no data")

    # ------------------------------------------------------------
    # 4. Requests per second
    # ------------------------------------------------------------
    requests = metric_frame(df, "http_reqs")

    if not requests.empty:
        requests_per_second = requests["value"].resample("1s").count()

        axes[1, 0].plot(
            requests_per_second.index,
            requests_per_second,
        )
        axes[1, 0].set_title("Requests per second")
        axes[1, 0].set_ylabel("Requests")
        axes[1, 0].set_ylim(bottom=0)
        axes[1, 0].grid(True, alpha=0.3)
    else:
        axes[1, 0].set_title("Requests per second: no data")

    # ------------------------------------------------------------
    # 5. Latency versus active VUs
    # ------------------------------------------------------------
    if not duration.empty and not vus.empty:
        latency = duration["value"].resample("1s").agg(
            mean="mean",
            p95=lambda values: percentile(values, 95),
            p99=lambda values: percentile(values, 99),
            samples="count",
        )

        active_vus = vus["value"].resample("1s").max().rename("vus")

        latency_vs_vus = latency.join(active_vus, how="inner")
        latency_vs_vus = latency_vs_vus[
            latency_vs_vus["samples"] > 0
        ]

        # Group by VU count so each point represents the latency
        # observed while that number of VUs was active.
        grouped = (
            latency_vs_vus
            .groupby("vus")
            .agg(
                mean_latency=("mean", "mean"),
                p95_latency=("p95", "mean"),
                p99_latency=("p99", "mean"),
                seconds=("samples", "count"),
            )
            .reset_index()
            .sort_values("vus")
        )

        axes[1, 1].plot(
            grouped["vus"],
            grouped["mean_latency"],
            marker="o",
            label="Mean latency",
        )
        axes[1, 1].plot(
            grouped["vus"],
            grouped["p95_latency"],
            marker="o",
            label="p95 latency",
        )
        axes[1, 1].plot(
            grouped["vus"],
            grouped["p99_latency"],
            marker="o",
            label="p99 latency",
        )

        axes[1, 1].set_title("Latency versus active VUs")
        axes[1, 1].set_xlabel("Active VUs")
        axes[1, 1].set_ylabel("Latency (milliseconds)")
        axes[1, 1].legend()
        axes[1, 1].grid(True, alpha=0.3)

        # Save the grouped data for further analysis.
        grouped.to_csv("latency-vs-vus.csv", index=False)
        print("Saved latency/VU data: latency-vs-vus.csv")
    else:
        axes[1, 1].set_title("Latency versus VUs: insufficient data")

    # ------------------------------------------------------------
    # 6. Scatter plot of individual latency samples versus VUs
    # ------------------------------------------------------------
    if not duration.empty and not vus.empty:
        latency_points = duration[["value"]].rename(
            columns={"value": "latency_ms"}
        )
        vus_points = vus[["value"]].rename(
            columns={"value": "vus"}
        )

        # Match each latency sample to the most recent VU measurement.
        latency_points = pd.merge_asof(
            latency_points.sort_index(),
            vus_points.sort_index(),
            left_index=True,
            right_index=True,
            direction="backward",
            tolerance=pd.Timedelta("2s"),
        ).dropna()

        if not latency_points.empty:
            # Limit visual impact of extreme outliers while retaining
            # all data in latency-vs-vus.csv.
            upper_limit = latency_points["latency_ms"].quantile(0.995)
            visible = latency_points[
                latency_points["latency_ms"] <= upper_limit
            ]

            axes[1, 2].scatter(
                visible["vus"],
                visible["latency_ms"],
                s=8,
                alpha=0.25,
            )

            axes[1, 2].set_title(
                "Individual latency samples versus VUs"
            )
            axes[1, 2].set_xlabel("Active VUs")
            axes[1, 2].set_ylabel("Latency (milliseconds)")
            axes[1, 2].grid(True, alpha=0.3)
        else:
            axes[1, 2].set_title("Latency/VU scatter: no matching data")
    else:
        axes[1, 2].set_title("Latency/VU scatter: insufficient data")

    # ------------------------------------------------------------
    # Formatting
    # ------------------------------------------------------------
    for axis in axes.flat:
        axis.tick_params(axis="x", rotation=30)

    fig.suptitle("k6 breakpoint test results", fontsize=16)

    fig.savefig(output_path, dpi=150)
    print(f"Saved graph: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Visualize a raw k6 JSON output file."
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Path to k6 JSON output, for example load-20261001.json",
    )
    parser.add_argument(
        "--plot",
        type=Path,
        default=Path("k6-results.png"),
        help="Output image path",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("k6-summary.csv"),
        help="Output CSV summary path",
    )

    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input file does not exist: {args.input}")

    df = load_k6_json(args.input)

    print(f"Loaded {len(df):,} metric points")
    print(f"Metrics found: {', '.join(sorted(df['metric'].unique()))}")
    print(
        f"Time range: {df['time'].min().isoformat()} "
        f"to {df['time'].max().isoformat()}"
    )

    summary = build_summary(df)
    summary.to_csv(args.summary, index=False)

    print(f"Saved summary: {args.summary}")
    print(summary.to_string(index=False))

    plot_results(df, args.plot)

if __name__ == "__main__":
    main()
