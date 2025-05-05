from typing import Any, Optional, Union, List, Tuple, Literal
import argparse
from datetime import datetime
import os
import sqlite3
import shutil
import yaml
import rclpy
from rclpy.node import Node
import rclpy.exceptions
from rosbag2_py import (
    SequentialReader,
    StorageOptions,
    ConverterOptions,
    SequentialWriter,
)
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
from .plot import plot_bag_time_ranges


class Split(Node):
    def __init__(self):
        """Constructor"""
        super().__init__("split")

    def split(
        self, bag: str, output: str, start: float, end: float, exclude_topics: List[str]
    ):
        """
        Splits a bag file into two parts based on the start and end time.
        Args:
            bag (str): Path to the bag file.
            start (float): Start time in seconds.
            end (float): End time in seconds.
            exclude_topics (List[str]): List of topics to exclude.
        """

        # Feasibility check
        if start > end:
            raise ValueError("Start time must be less than end time. You prick.")

        # Prepare storage options
        storage_id = None
        if not os.path.isdir(bag):
            print(f"Bag file {bag} does not exist.")
            return

        # For directory-based bags, use the same storage format as input
        if any(f.endswith(".db3") for f in os.listdir(bag)):
            storage_id = "sqlite3"
        else:
            storage_id = "mcap"

        reader = SequentialReader()
        reader.open(
            StorageOptions(uri=bag, storage_id=storage_id),
            ConverterOptions("", ""),
        )
        if output:
            new_bag = output
        else:
            # Append '_split' to the bag directory name
            new_bag = f"{bag.rstrip('/')}_split"

        writer = SequentialWriter()
        writer.open(
            StorageOptions(uri=new_bag, storage_id=storage_id),
            ConverterOptions("", ""),
        )
        for topic in reader.get_all_topics_and_types():

            if exclude_topics and topic.name not in exclude_topics:
                writer.create_topic(topic)

        first_time = None
        count = 0
        while reader.has_next():
            (topic, data, t) = reader.read_next()

            if first_time is None:
                first_time = t
                print("Bag starts at: ", first_time)

            if exclude_topics and topic in exclude_topics:
                continue

            if (start * 1e9 + first_time) > t:
                continue
            
            if (end * 1e9 + first_time) < t:
                print(f"End time {end} reached. Stopping.")
                break

            writer.write(topic, data, t)
            count += 1
        print(f"Wrote {count} messages to {new_bag}")
        writer.close()


def main():
    parser = argparse.ArgumentParser(description="Split by time")
    parser.add_argument("--bag", required=True, help="Path to the bag file")
    parser.add_argument("--output", help="Path to the output bag file")
    parser.add_argument(
        "--start-offset",
        type=float,
        required=True,
        help="Start offset in seconds",
    )
    parser.add_argument(
        "--end-offset",
        type=float,
        required=True,
        help="End offset in seconds",
    )
    parser.add_argument(
        "--exclude-topics",
        type=str,
        nargs="*",
        help="Topics to exclude",
    )

    args = parser.parse_args()

    rclpy.init()
    node = Split()

    try:
        node.split(
            args.bag,
            args.output,
            args.start_offset,
            args.end_offset,
            args.exclude_topics,
        )
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
