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
        with BackupOperations(db_path=args.db_path) as ops:
            snapshot_id = ops.snapshot(args.target_directory)
            print(f"Snapshot {snapshot_id} created successfully.")
    except Exception as e:
        print(f"Error creating snapshot: {str(e)}")
        sys.exit(1)


def list_command(args):
    """Execute the list command."""
    try:
        with BackupOperations(db_path=args.db_path) as ops:
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
        with BackupOperations(db_path=args.db_path) as ops:
            success = ops.restore(args.snapshot_number, args.output_directory)
            if success:
                print(f"Snapshot {args.snapshot_number} restored to {args.output_directory}")
    except Exception as e:
        print(f"Error restoring snapshot: {str(e)}")
        sys.exit(1)


def prune_command(args):
    """Execute the prune command."""
    try:
        with BackupOperations(db_path=args.db_path) as ops:
            success = ops.prune(args.snapshot)
            if success:
                print(f"Snapshot {args.snapshot} pruned successfully.")
            else:
                print(f"Failed to prune snapshot {args.snapshot}.")
                sys.exit(1)
    except Exception as e:
        print(f"Error pruning snapshot: {str(e)}")
        sys.exit(1)


def main():

    parser = argparse.ArgumentParser(description="File backup tool with incremental snapshots")
    # Add db-path argument to the main parser so it's available for all commands
    parser.add_argument("--db-path", default="backups.db", help="Path to the database file (default: backups.db)")
    
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
    
    # Prune command
    prune_parser = subparsers.add_parser("prune", help="Remove a snapshot")
    prune_parser.add_argument("--snapshot", type=int, required=True, help="Snapshot ID to prune")
    
    args = parser.parse_args()
    
    if args.command == "snapshot":
        snapshot_command(args)
    elif args.command == "list":
        list_command(args)
    elif args.command == "restore":
        restore_command(args)
    elif args.command == "prune":
        prune_command(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
