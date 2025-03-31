import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import List

from .operations import BackupOperations

def snapshot_command(args):
    try:
        with BackupOperations() as ops:
            snapshot_id = ops.snapshot(args.target_directory)
            print(f"Snapshot {snapshot_id} created successfully.")
    except Exception as e:
        print(f"Error creating snapshot: {str(e)}")
        sys.exit(1)


def list_command(args):
    """Execute the list command."""
    try:
        with BackupOperations() as ops:
            if not snapshots:
                print("No snapshots found.")
                return
            
            for snapshot in snapshots:
                print(f"{snapshot['id']:<10}{format_timestamp(snapshot['timestamp']):<25}")
            
    except Exception as e:
        print(f"Error listing snapshots: {str(e)}")
        sys.exit(1)


def main():

    parser = argparse.ArgumentParser(description="File backup tool with incremental snapshots")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Snapshot command
    snapshot_parser = subparsers.add_parser("snapshot", help="Take a snapshot of a directory")
    snapshot_parser.add_argument("--target-directory", required=True, help="Directory to snapshot")
    
    # List command
    subparsers.add_parser("list", help="List all snapshots")
    
    args = parser.parse_args()
    
    if args.command == "snapshot":
        snapshot_command(args)
    elif args.command == "list":
        list_command(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
