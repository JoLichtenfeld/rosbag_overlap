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


class RosbagOverlap(Node):
    def __init__(self):
        """Constructor"""
        super().__init__("rosbag_overlap")

    def get_start_end_timestamps(self, bag_path):
        """Get the start and end timestamps of a bag file by parsing metadata.yaml."""
        
        # Determine if bag_path is a directory or file
        if os.path.isdir(bag_path):
            metadata_path = os.path.join(bag_path, "metadata.yaml")
            if not os.path.exists(metadata_path):
                raise ValueError(f"No metadata.yaml found in directory {bag_path}")
        else:
            # For single files, check if a metadata file exists in the parent directory
            parent_dir = os.path.dirname(bag_path)
            metadata_path = os.path.join(parent_dir, "metadata.yaml")
            if not os.path.exists(metadata_path):
                # Fall back to original method if no metadata file is found
                return self._get_timestamps_using_reader(bag_path)
        
        # Parse metadata.yaml
        try:
            with open(metadata_path, 'r') as f:
                metadata = yaml.safe_load(f)
            
            info = metadata.get('rosbag2_bagfile_information', {})
            
            # Get start time and duration in nanoseconds
            start_ns = info.get('starting_time', {}).get('nanoseconds_since_epoch', 0)
            duration_ns = info.get('duration', {}).get('nanoseconds', 0)
            end_ns = start_ns + duration_ns
            
            # Convert to datetime objects
            start_dt = datetime.fromtimestamp(start_ns / 1e9)
            end_dt = datetime.fromtimestamp(end_ns / 1e9)
            
            return start_dt, end_dt
            
        except (yaml.YAMLError, KeyError, TypeError) as e:
            self.get_logger().warning(f"Error parsing metadata.yaml: {e}. Falling back to reader method.")
            return self._get_timestamps_using_reader(bag_path)
    
    def _get_timestamps_using_reader(self, bag_path):
        """Legacy method to get timestamps using SequentialReader (fallback)."""
        storage_id = None
        file_paths = []
        
        # Determine storage type and file paths
        if os.path.isdir(bag_path):
            db3_files = [os.path.join(bag_path, f) for f in os.listdir(bag_path) if f.endswith(".db3")]
            mcap_files = [os.path.join(bag_path, f) for f in os.listdir(bag_path) if f.endswith(".mcap")]
            
            if db3_files:
                storage_id = "sqlite3"
                file_paths = [db3_files[0]]  # For SQLite, we only need one file
            elif mcap_files:
                storage_id = "mcap"
                file_paths = mcap_files  # For MCAP, we need to process all files
            else:
                raise ValueError(f"No valid bag files found in directory {bag_path}")
        else:
            storage_id = "sqlite3" if bag_path.endswith(".db3") else "mcap"
            file_paths = [bag_path]
        
        # Process file(s) to find start and end times
        start_ns = float("inf")
        end_ns = float("-inf")
        
        for file_path in file_paths:
            storage_options = StorageOptions(uri=file_path, storage_id=storage_id)
            converter_options = ConverterOptions(
                input_serialization_format="cdr",
                output_serialization_format="cdr",
            )
            
            reader = SequentialReader()
            reader.open(storage_options, converter_options)
            metadata = reader.get_metadata()
            
            file_start = metadata.starting_time.nanoseconds
            file_end = file_start + metadata.duration.nanoseconds
            
            start_ns = min(start_ns, file_start)
            end_ns = max(end_ns, file_end)
        
        # Convert nanoseconds to datetime
        start_dt = datetime.fromtimestamp(start_ns / 1e9)
        end_dt = datetime.fromtimestamp(end_ns / 1e9)
        
        return start_dt, end_dt

    def find_overlap(self, args) -> Tuple[datetime, datetime]:
        """Find the temporal overlap between multiple bag files."""

        # Get all bag files from the specified paths
        bag_files = self._get_all_bag_files(args.bags)
        
        if not bag_files:
            print("\033[91mError: No valid bag files found in the specified paths.\033[0m")
            return datetime.now(), datetime.now()

        time_ranges = []
        for bag_path in bag_files:
            start, end = self.get_start_end_timestamps(bag_path)
            time_ranges.append((start, end, bag_path))

        overlap_start = max(start for start, _, _ in time_ranges)
        overlap_end = min(end for _, end, _ in time_ranges)

        if overlap_start > overlap_end:
            print("\033[91mWarning: No temporal overlap found between the bag files.\033[0m")
            overlap_start = datetime.now()
            overlap_end = datetime.now()

        print("\nBag File Summary:")
        print("=" * 80)
        for start, end, bag_path in time_ranges:
            self._print_bag_summary(start, end, bag_path)

        print("\nTemporal Overlap Summary:")
        print("=" * 80)
        self._print_timespan(overlap_start, overlap_end)

        if args.plot:
            # Plot the time ranges of the bag files
            plot_bag_time_ranges(time_ranges)

        return overlap_start, overlap_end
    
    def _get_all_bag_files(self, paths: List[str]) -> List[str]:
        """Recursively process the input paths and return a list of all bag files/directories."""
        bag_files = []

        def is_bag_directory(dir_path):
            """Check if a directory is a valid bag directory."""
            if not os.path.isdir(dir_path):
                return False
            has_metadata = os.path.exists(os.path.join(dir_path, "metadata.yaml"))
            files = os.listdir(dir_path)
            has_db3 = any(f.endswith(".db3") for f in files)
            has_mcap = any(f.endswith(".mcap") for f in files)
            return has_metadata and (has_db3 or has_mcap)

        def find_bag_files_recursively(path, n):
            """Recursively find bag directories up to a specified depth."""
            if n < 0:
                return
            if is_bag_directory(path):
                bag_files.append(path)
            else:
                for item in os.listdir(path):
                    item_path = os.path.join(path, item)
                    find_bag_files_recursively(item_path, n - 1)

        for path in paths:
            if not os.path.exists(path):
                raise ValueError(f"Path {path} does not exist.")
            find_bag_files_recursively(path, n=1)

        print(f"Found {len(bag_files)} bag files/directories:")
        for bag in bag_files:
            print(f" - {bag}")

        return bag_files

    def _print_bag_summary(self, start: datetime, end: datetime, path: str) -> None:
        """Print the summary of a bag file."""
        print(f"\nBag: {path}")
        self._print_timespan(start, end)

    def _print_timespan(self, overlap_start: datetime, overlap_end: datetime) -> None:
        print(f" Start:    {overlap_start}")
        print(f" End:      {overlap_end}")
        print(f" Duration: {overlap_end - overlap_start}")

    def crop_bags(self, args) -> None:
        """Crop bag files to their overlap period and save to output directory."""
        # Get all bag files from the specified paths
        bag_files = self._get_all_bag_files(args.bags)
        
        if not bag_files:
            print("\033[91mError: No valid bag files found in the specified paths.\033[0m")
            return
            
        output_dir = args.output_dir
        overwrite = args.overwrite

        # Create a new args object with the processed bag files for find_overlap
        class Args:
            pass
        new_args = Args()
        new_args.bags = bag_files
        new_args.plot = args.plot

        overlap_start, overlap_end = self.find_overlap(new_args)

        # Check if there's a valid overlap
        if overlap_start >= overlap_end:
            print("\033[91mCannot crop: No valid temporal overlap found.\033[0m")
            return

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        for bag_path in bag_files:
            self.crop_bag(
                bag_path,
                overlap_start,
                overlap_end,
                output_dir,
                overwrite=overwrite,
            )

    def crop_bag(
        self,
        bag_path: str,
        overlap_start: datetime,
        overlap_end: datetime,
        output_dir: str,
        overwrite: bool = False,
    ) -> None:

        # Get the bag name for the output file
        bag_name = os.path.basename(bag_path.rstrip("/"))
        output_path = os.path.join(output_dir, f"{bag_name}_cropped")

        # Check if output directory exists and ask for overwrite
        if os.path.exists(output_path):
            if not overwrite:
                response = input(
                    f"Bag directory {output_path} already exists. Overwrite? [Y/n] "
                )
                if response.lower() not in ["", "y", "yes"]:
                    print(f"Skipping {bag_name}")
                    return
            # Remove existing directory
            shutil.rmtree(output_path)

        # Prepare storage options
        if os.path.isdir(bag_path):
            # For directory-based bags, use the same storage format as input
            if any(f.endswith(".db3") for f in os.listdir(bag_path)):
                storage_id = "sqlite3"
            else:
                storage_id = "mcap"
        else:
            storage_id = "sqlite3" if bag_path.endswith(".db3") else "mcap"

        # Check if the bag file is completely contained in the overlap period
        bag_start, bag_end = self.get_start_end_timestamps(bag_path)
        if (
            overlap_start.timestamp() * 1e9 <= bag_start.timestamp() * 1e9 + 10
            and bag_end.timestamp() * 1e9 - 10 <= overlap_end.timestamp() * 1e9
        ):
            # copy the entire bag
            print(f"Bag {bag_path} is completely contained in the overlap period.")
            print(f"Copying {bag_path} to {output_path}")
            shutil.copytree(bag_path, output_path)
            return

        # Create writer with max file size of 1GB
        writer = SequentialWriter()
        writer.open(
            StorageOptions(
                uri=output_path,
                storage_id=storage_id,
                max_bagfile_size=1024 * 1024 * 1024,  # 1GB in bytes
            ),
            ConverterOptions("", ""),
        )

        # Get total messages from metadata
        reader = SequentialReader()
        reader.open(
            StorageOptions(uri=bag_path, storage_id=storage_id),
            ConverterOptions("", ""),
        )

        metadata = reader.get_metadata()
        total_msgs = metadata.message_count
        print(f"Total messages: {total_msgs}")

        # Copy topic metadata
        for topic in reader.get_all_topics_and_types():
            writer.create_topic(topic)

        # Copy messages within overlap period
        msg_count = 0
        while reader.has_next():
            (topic, data, t) = reader.read_next()
            # t is already in nanoseconds
            if overlap_start.timestamp() * 1e9 <= t <= overlap_end.timestamp() * 1e9:
                writer.write(topic, data, t)
                msg_count += 1
                if msg_count % 1000 == 0:
                    print(
                        f"\rProgress: {msg_count}/{total_msgs} messages ({msg_count/total_msgs*100:.1f}%)",
                        end="",
                    )

        print(f"\nCropped bag saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Find temporal overlap between ROS bag files and optionally crop them"
    )
    parser.add_argument("--bags", nargs="+", required=True, 
                        help="Paths to bag files or directories containing bag files")
    parser.add_argument(
        "--crop", action="store_true", help="Crop bags to overlap period"
    )
    parser.add_argument(
        "--output-dir", default="cropped_bags", help="Output directory for cropped bags"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output directory",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Plot the time ranges of the bag files",
    )
    args = parser.parse_args()

    rclpy.init()
    node = RosbagOverlap()

    try:
        if args.crop:
            node.crop_bags(args)
        else:
            node.find_overlap(args)
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()