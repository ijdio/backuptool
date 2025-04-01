import argparse
import sys
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, NoReturn

from .operations import BackupOperations

# Configure logging to write to file only, not stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='backuptool.log',
    filemode='a'
)
logger = logging.getLogger('backuptool')


def format_timestamp(timestamp: str) -> str:
    """
    Convert ISO format timestamp to a more readable format.
    
    Args:
        timestamp (str): ISO format timestamp string
        
    Returns:
        str: Human-readable timestamp in format YYYY-MM-DD HH:MM:SS
    """
    dt = datetime.fromisoformat(timestamp)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def print_error_and_exit(error_message: str, exit_code: int = 1) -> NoReturn:
    """
    Print an error message and exit the program with the specified exit code.
    
    Args:
        error_message (str): The error message to display
        exit_code (int, optional): The exit code to use. Defaults to 1.
    """
    logger.error(error_message)
    print(f"Error: {error_message}", file=sys.stderr)
    sys.exit(exit_code)


def snapshot_command(args: argparse.Namespace) -> None:
    """
    Execute the snapshot command to create a backup snapshot of a directory.
    
    Args:
        args (argparse.Namespace): Command line arguments containing:
            - db_path: Path to the database file
            - target_directory: Directory to snapshot
    """
    try:
        logger.info("Starting snapshot creation")
        target_dir = Path(args.target_directory)
        if not target_dir.exists():
            print_error_and_exit(f"Target directory '{target_dir}' does not exist")
        if not target_dir.is_dir():
            print_error_and_exit(f"'{target_dir}' is not a directory")
            
        with BackupOperations(db_path=args.db_path) as ops:
            snapshot_id = ops.snapshot(str(target_dir))
            logger.info(f"Snapshot {snapshot_id} created successfully")
            print(f"Snapshot {snapshot_id} created successfully.")
    except PermissionError as e:
        print_error_and_exit(f"Permission denied: {str(e)}")
    except FileNotFoundError as e:
        print_error_and_exit(f"File not found: {str(e)}")
    except OSError as e:
        print_error_and_exit(f"OS error: {str(e)}")
    except Exception as e:
        print_error_and_exit(f"Error creating snapshot: {str(e)}")


def list_command(args: argparse.Namespace) -> None:
    """
    Execute the list command to display all snapshots in the database.
    
    Args:
        args (argparse.Namespace): Command line arguments containing:
            - db_path: Path to the database file
    """
    try:
        logger.info("Listing snapshots")
        with BackupOperations(db_path=args.db_path) as ops:
            snapshots = ops.list_snapshots()
            total_db_size = ops.db.get_database_size()
            
            if not snapshots:
                logger.info("No snapshots found")
                print("No snapshots found.")
                return
            
            # Print the header
            print(f"{'SNAPSHOT':<10}{'TIMESTAMP':<22}{'SIZE':<6}{'DISTINCT_SIZE':<14}")
            
            # Print each snapshot
            for snapshot in snapshots:
                print(f"{snapshot['id']:<10}{format_timestamp(snapshot['timestamp']):<22}{snapshot['size']:<6}{snapshot['distinct_size']:<14}")
            
            # Print total size summary
            print(f"{'total':<32}{total_db_size:<6}")
    except FileNotFoundError:
        print_error_and_exit(f"Database file '{args.db_path}' not found")
    except PermissionError:
        print_error_and_exit(f"Permission denied when accessing database '{args.db_path}'")
    except Exception as e:
        print_error_and_exit(f"Error listing snapshots: {str(e)}")


def restore_command(args: argparse.Namespace) -> None:
    """
    Execute the restore command to recover files from a snapshot.
    
    Args:
        args (argparse.Namespace): Command line arguments containing:
            - db_path: Path to the database file
            - snapshot_number: ID of the snapshot to restore
            - output_directory: Directory to restore files to
    """
    try:
        logger.info("Starting restore operation")
        output_dir = Path(args.output_directory)
        
        # Create output directory if it doesn't exist
        if not output_dir.exists():
            output_dir.mkdir(parents=True)
        elif not output_dir.is_dir():
            print_error_and_exit(f"'{output_dir}' exists but is not a directory")
            
        with BackupOperations(db_path=args.db_path) as ops:
            success = ops.restore(args.snapshot_number, str(output_dir))
            if success:
                logger.info(f"Snapshot {args.snapshot_number} restored to {output_dir}")
                print(f"Snapshot {args.snapshot_number} restored to {output_dir}")
            else:
                print_error_and_exit(f"Failed to restore snapshot {args.snapshot_number}")
    except FileNotFoundError as e:
        print_error_and_exit(f"File not found: {str(e)}")
    except PermissionError as e:
        print_error_and_exit(f"Permission denied: {str(e)}")
    except ValueError as e:
        print_error_and_exit(f"Invalid value: {str(e)}")
    except Exception as e:
        print_error_and_exit(f"Error restoring snapshot: {str(e)}")


def prune_command(args: argparse.Namespace) -> None:
    """
    Execute the prune command to remove a snapshot from the database.
    
    Args:
        args (argparse.Namespace): Command line arguments containing:
            - db_path: Path to the database file
            - snapshot: ID of the snapshot to prune
    """
    try:
        logger.info("Pruning snapshot")
        with BackupOperations(db_path=args.db_path) as ops:
            if ops.prune(args.snapshot):
                logger.info(f"Snapshot {args.snapshot} pruned successfully")
                print(f"Snapshot {args.snapshot} pruned successfully.")
            else:
                print_error_and_exit(f"Failed to prune snapshot {args.snapshot}")
    except ValueError as e:
        print_error_and_exit(f"Invalid value: {str(e)}")
    except FileNotFoundError as e:
        print_error_and_exit(f"File not found: {str(e)}")
    except Exception as e:
        print_error_and_exit(f"Error pruning snapshot: {str(e)}")


def check_command(args: argparse.Namespace) -> None:
    """
    Execute the check command to verify database integrity.
    
    Args:
        args (argparse.Namespace): Command line arguments containing:
            - db_path: Path to the database file
    """
    try:
        logger.info("Starting database integrity check")
        with BackupOperations(db_path=args.db_path) as ops:
            all_valid, corrupted_items = ops.check()
            
            if all_valid:
                logger.info("Database integrity check passed")
                print("Database integrity check passed. All file content is valid.")
            else:
                logger.warning(f"Database integrity check failed. Found {len(corrupted_items)} corrupted items.")
                print("\nDatabase integrity check FAILED. Corrupted content detected.\n")
                print(f"Found {len(corrupted_items)} corrupted items:")
                
                for i, item in enumerate(corrupted_items, 1):
                    print(f"\n{i}. Corrupted content:")
                    print(f"   Stored hash:     {item['stored_hash']}")
                    print(f"   Calculated hash: {item['calculated_hash']}")
                    
                    if item['affected_files']:
                        print(f"   Affected files ({len(item['affected_files'])}):")
                        for file in item['affected_files']:
                            print(f"     - Snapshot {file['snapshot_id']} ({format_timestamp(file['timestamp'])}): {file['path']}")
                
                print("\nRecommendation: Restore affected files from an alternative backup if available.")
                sys.exit(1)
    except FileNotFoundError:
        print_error_and_exit(f"Database file '{args.db_path}' not found")
    except PermissionError:
        print_error_and_exit(f"Permission denied when accessing database '{args.db_path}'")
    except Exception as e:
        print_error_and_exit(f"Error checking database integrity: {str(e)}")


def main() -> None:
    """
    Main entry point for the backup tool command line interface.
    Parses arguments and dispatches to appropriate command handlers.
    """
    parser = argparse.ArgumentParser(
        description="File backup tool with incremental snapshots",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    # Add db-path argument to the main parser so it's available for all commands
    parser.add_argument(
        "--db-path", 
        default="backups.db", 
        help="Path to the database file"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Snapshot command
    snapshot_parser = subparsers.add_parser(
        "snapshot", 
        help="Create a new snapshot of a directory"
    )
    snapshot_parser.add_argument(
        "--target-directory", 
        required=True, 
        help="Directory to take a snapshot of"
    )
    
    # List command
    subparsers.add_parser(
        "list", 
        help="List all snapshots with their sizes"
    )
    
    # Restore command
    restore_parser = subparsers.add_parser(
        "restore", 
        help="Restore a snapshot to a directory"
    )
    restore_parser.add_argument(
        "--snapshot-number", 
        required=True, 
        type=int, 
        help="Snapshot ID to restore"
    )
    restore_parser.add_argument(
        "--output-directory", 
        required=True, 
        help="Directory to restore to (will be created if it doesn't exist)"
    )
    
    # Prune command
    prune_parser = subparsers.add_parser(
        "prune", 
        help="Remove a snapshot"
    )
    prune_parser.add_argument(
        "--snapshot", 
        type=int, 
        required=True, 
        help="Snapshot ID to prune"
    )
    
    # Check command
    subparsers.add_parser(
        "check", 
        help="Check database integrity"
    )
    
    args = parser.parse_args()
    
    # Command dispatch
    command_handlers = {
        "snapshot": snapshot_command,
        "list": list_command,
        "restore": restore_command,
        "prune": prune_command,
        "check": check_command,
    }
    
    if args.command in command_handlers:
        command_handlers[args.command](args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
