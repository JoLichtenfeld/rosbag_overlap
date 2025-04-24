import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from typing import List, Tuple


def plot_bag_time_ranges(bag_ranges: List[Tuple[datetime, datetime, str]]) -> None:

    # Sort by name and then by start time
    bag_ranges.sort(key=lambda x: (os.path.basename(x[2]), x[0]))

    fig_height = max(4, len(bag_ranges) * 1.2)
    fig, ax = plt.subplots(figsize=(12, fig_height))

    colors = plt.cm.tab10.colors

    # For x-axis margin computation
    start_nums = [mdates.date2num(start) for start, _, _ in bag_ranges]
    end_nums = [mdates.date2num(end) for _, end, _ in bag_ranges]
    xmin = min(start_nums)
    xmax = max(end_nums)
    xmargin = (xmax - xmin) * 0.05  # 5% margin

    for i, (start, end, label) in enumerate(bag_ranges):
        start_num = mdates.date2num(start)
        end_num = mdates.date2num(end)
        duration = end - start
        minutes, seconds = divmod(duration.total_seconds(), 60)
        duration_text = f"{int(minutes)}m {int(seconds)}s"

        ax.barh(
            y=i,
            width=end_num - start_num,
            left=start_num,
            height=0.3,
            color=colors[i % len(colors)],
            edgecolor="black",
        )

        middle = start_num + (end_num - start_num) / 2
        ax.text(
            middle,
            i,
            duration_text,
            va="center",
            ha="center",
            color="black",
            fontweight="bold",
            fontsize=10,
        )

    ax.set_xlim(xmin - xmargin, xmax + xmargin)
    ax.set_yticks(range(len(bag_ranges)))
    ax.set_yticklabels(
        [os.path.basename(label.rstrip("/")) for _, _, label in bag_ranges]
    )
    ax.invert_yaxis()
    ax.set_xlabel("Time")
    ax.set_title("ROS Bag File Time Ranges")

    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())

    ax.grid(True, axis="x", linestyle="--", alpha=0.4)

    # Manual margin instead of tight_layout
    plt.subplots_adjust(left=0.25, right=0.95, bottom=0.15)
    plt.show()
