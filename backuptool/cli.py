import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import List

from .operations import BackupOperations


def format_timestamp(timestamp: str) -> str:
    """Convert ISO format timestamp to a more readable format."""
    dt = datetime.fromisoformat(timestamp)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def snapshot_command(args):
    """Execute the snapshot command."""
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
            snapshots, total_db_size = ops.list_snapshots()
            
            if not snapshots:
                print("No snapshots found.")
                return
            
            # Print the header
            print(f"{'SNAPSHOT':<10}{'TIMESTAMP':<25}{'SIZE':<6}{'DISTINCT_SIZE':<15}")
            
            # Print each snapshot
            for snapshot in snapshots:
                print(f"{snapshot['id']:<10}{format_timestamp(snapshot['timestamp']):<25}{snapshot['size']:<6}{snapshot['distinct_size']:<15}")
            
            # Print total size summary
            print(f"{'total':<35}{total_db_size:<6}")
    except Exception as e:
        print(f"Error listing snapshots: {str(e)}")
        sys.exit(1)


def restore_command(args):
    """Execute the restore command."""
    try:
        with BackupOperations() as ops:
            success = ops.restore(args.snapshot_number, args.output_directory)
            if success:
                print(f"Snapshot {args.snapshot_number} restored to {args.output_directory}")
    except Exception as e:
        print(f"Error restoring snapshot: {str(e)}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="File backup tool with incremental snapshots")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Snapshot command
    snapshot_parser = subparsers.add_parser("snapshot", help="Take a snapshot of a directory")
    snapshot_parser.add_argument("--target-directory", required=True, help="Directory to snapshot")
    
    # List command
    subparsers.add_parser("list", help="List all snapshots")
    
    # Restore command
    restore_parser = subparsers.add_parser("restore", help="Restore a snapshot to a directory")
    restore_parser.add_argument("--snapshot-number", type=int, required=True, help="Snapshot ID to restore")
    restore_parser.add_argument("--output-directory", required=True, help="Directory to restore to")
    
    args = parser.parse_args()
    
    if args.command == "snapshot":
        snapshot_command(args)
    elif args.command == "list":
        list_command(args)
    elif args.command == "restore":
        restore_command(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
