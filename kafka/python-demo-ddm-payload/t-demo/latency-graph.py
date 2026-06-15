import csv
from collections import defaultdict
import matplotlib.pyplot as plt


def load_stats(csv_file="ddm_stats.csv"):
    """Load latency and byte-size records from a CSV file."""
    records = []
    with open(csv_file, newline="") as fh:
        reader = csv.DictReader(fh) # read the CSV file as a dictionary
        for row in reader:
            try:
                num_bytes = int(row.get("num_bytes", ""))
                latency_ms = float(row.get("latency_ms", ""))
            except (ValueError, TypeError):
                continue
            records.append({"num_bytes": num_bytes, "latency_ms": latency_ms})
    return records


def avg_latency_by_byte(records):
    """Calculate average latency grouped by exact image byte size."""
    if not records:
        return []

    grouped = defaultdict(list) # 
    for x in records:
        grouped[x["num_bytes"]].append(x["latency_ms"]) # group latencies by exact byte size

    averages = []
    for num_bytes, latencies in grouped.items():
        average_latency = sum(latencies) / len(latencies)
        averages.append((num_bytes, average_latency)) # create a list of tuples (num_bytes, average_latency)

    averages.sort(key=lambda item: item[0]) # sort the averages by byte size for better plotting
    return averages


def plot(csv_file="ddm_stats.csv", output_file='latency_graph.png', show=True):
    """Plot average latency versus byte size from the CSV file."""
    records = load_stats(csv_file) # load the records from the CSV file
    averages = avg_latency_by_byte(records) # calculate average latency for each byte size

    if not averages:
        raise ValueError(f"No valid records found in {csv_file}")

    sizes, avg_latencies = zip(*averages) # unzip the list of tuples into two separate lists for plotting

    
    plt.figure()
    plt.plot(sizes, avg_latencies, marker="o", linestyle="-", color="tab:blue")
    plt.scatter(sizes, avg_latencies, color="tab:red", zorder=3)
    plt.title("Average Latency vs Byte Size")
    plt.xlabel("Image Size (bytes)")
    plt.ylabel("Average Latency (ms)")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()

    if output_file:
        plt.savefig(output_file)
    if show:
        plt.show()
    plt.close()


if __name__ == "__main__":
    plot()
