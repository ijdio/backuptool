import os
import shutil
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any, Union
import hashlib

from .database import BackupDatabase, hash_file_content


# Get logger instance (configuration is handled in cli.py)
logger = logging.getLogger('backuptool')


class BackupOperations:
    """Handles core backup operations like snapshot, restore, list, and prune."""

    def __init__(self, db_path: str = "backups.db"):
        """
        Initialize BackupOperations with a database path.
        
        Args:
            db_path (str, optional): Path to the SQLite database file. Defaults to "backups.db".
            
        Raises:
            sqlite3.Error: If there's an error connecting to the database
            PermissionError: If there's no permission to access the database file
        """
        self.db = BackupDatabase(db_path)
        logger.debug(f"Initialized BackupOperations with database at {db_path}")

    def snapshot(self, target_directory: str) -> int:
        """
        Take a snapshot of the specified directory.
        
        This method walks through the target directory, hashes each file's content,
        and stores the file metadata and content in the database. It uses content
        hashing to avoid storing duplicate content.
        
        Args:
            target_directory (str): Path to the directory to snapshot
            
        Returns:
            int: The snapshot ID
            
        Raises:
            ValueError: If the target directory doesn't exist or is not a directory
            PermissionError: If there's a permission error accessing files
            FileNotFoundError: If a file is not found during processing
            RuntimeError: If there's an error during the snapshot process
        """
        target_path = Path(target_directory).resolve()
        
        if not target_path.exists():
            raise ValueError(f"Target directory '{target_directory}' does not exist")
        if not target_path.is_dir():
            raise ValueError(f"'{target_directory}' is not a directory")
        if not os.access(target_path, os.R_OK):
            raise PermissionError(f"No permission to read directory '{target_directory}'")
        
        try:
            # Create a new snapshot
            snapshot_id = self.db.add_snapshot()
            logger.info(f"Created new snapshot with ID {snapshot_id} for directory '{target_directory}'")
            
            # Collect all files to process
            all_files = []
            for root, _, files in os.walk(target_path):
                for file in files:
                    file_path = Path(root) / file
                    relative_path = str(file_path.relative_to(target_path))
                    all_files.append((file_path, relative_path))
            
            logger.info(f"Found {len(all_files)} files to process in '{target_directory}'")
            
            # Process files sequentially to avoid SQLite threading issues
            processed_count = 0
            skipped_count = 0
            total_size = 0
            
            for file_path, relative_path in all_files:
                try:
                    # Skip files we can't read
                    if not os.access(file_path, os.R_OK):
                        logger.warning(f"Skipping file due to permission denied: '{file_path}'")
                        skipped_count += 1
                        continue
                    
                    # Skip files that are too large (> 1GB) or empty
                    file_size = file_path.stat().st_size
                    if file_size > 1_073_741_824:  # 1GB
                        logger.warning(f"Skipping file larger than 1GB: '{file_path}' ({file_size / 1_048_576:.2f} MB)")
                        skipped_count += 1
                        continue
                    
                    if file_size == 0:
                        logger.debug(f"Processing empty file: '{file_path}'")
                    
                    # Hash the file content
                    content_hash = hash_file_content(str(file_path))
                    
                    # Add the file to the database
                    self.db.add_file(snapshot_id, relative_path, content_hash)
                    
                    # Store the file content if it doesn't exist already
                    if not self.db.content_exists(content_hash):
                        with open(file_path, 'rb') as f:
                            file_content = f.read()
                        self.db.add_content(content_hash, file_content)
                        total_size += file_size
                    
                    processed_count += 1
                    if processed_count % 100 == 0:
                        logger.info(f"Processed {processed_count}/{len(all_files)} files")
                        
                except PermissionError as e:
                    logger.warning(f"Permission denied for file '{file_path}': {str(e)}")
                    skipped_count += 1
                except FileNotFoundError as e:
                    logger.warning(f"File not found: '{file_path}': {str(e)}")
                    skipped_count += 1
                except (IOError, OSError) as e:
                    logger.warning(f"Could not process file '{file_path}': {str(e)}")
                    skipped_count += 1
            
            if skipped_count > 0:
                logger.warning(f"Skipped {skipped_count} files due to errors")
            
            # Update the snapshot size in the database
            self.db.update_snapshot_size(snapshot_id, total_size)
                
            logger.info(f"Snapshot {snapshot_id} completed successfully: {processed_count} files processed, {total_size / 1_048_576:.2f} MB of new content added")
            return snapshot_id
        except Exception as e:
            logger.error(f"Error creating snapshot: {str(e)}")
            raise RuntimeError(f"Failed to create snapshot: {str(e)}") from e

    def list_snapshots(self) -> List[Dict[str, Any]]:
        """
        List all snapshots with disk usage metrics.
        
        Returns:
            List[Dict[str, Any]]: List of snapshots with information including:
                - id: Snapshot ID
                - timestamp: Creation timestamp
                - size: Total size of files in kilobytes
                - distinct_size: Size of unique content in kilobytes
            
        Raises:
            RuntimeError: If there's an error retrieving snapshot information
        """
        try:
            snapshots = self.db.get_snapshots()
            
            # Enhance snapshots with disk usage metrics
            for snapshot in snapshots:
                snapshot_id = snapshot['id']
                snapshot['size'] = self.db.get_snapshot_size(snapshot_id)
                snapshot['distinct_size'] = self.db.get_snapshot_distinct_size(snapshot_id)
            
            logger.debug(f"Retrieved information for {len(snapshots)} snapshots")
            return snapshots
        except Exception as e:
            logger.error(f"Error listing snapshots: {str(e)}")
            raise RuntimeError(f"Failed to list snapshots: {str(e)}") from e

    def restore(self, snapshot_id: int, output_directory: str) -> bool:
        """
        Restore a snapshot to the specified directory.
        
        This method recreates the entire directory structure and file contents
        from the specified snapshot, ensuring bit-for-bit identical restoration.
        
        Args:
            snapshot_id (int): ID of the snapshot to restore
            output_directory (str): Directory to restore the snapshot to
            
        Returns:
            bool: True if the restore was successful
            
        Raises:
            ValueError: If the snapshot doesn't exist or parameters are invalid
            PermissionError: If there's no permission to write to the output directory
            RuntimeError: If there's an error during the restore process
        """
        if not isinstance(snapshot_id, int) or snapshot_id <= 0:
            raise ValueError(f"Invalid snapshot ID: {snapshot_id}")
        
        if not output_directory:
            raise ValueError("Output directory cannot be empty")
            
        try:
            # Check if the snapshot exists
            snapshot = self.db.get_snapshot(snapshot_id)
            if not snapshot:
                raise ValueError(f"Snapshot {snapshot_id} does not exist")
            
            logger.info(f"Restoring snapshot {snapshot_id} from {snapshot['timestamp']} to {output_directory}")
            
            # Create the output directory if it doesn't exist
            output_path = Path(output_directory).resolve()
            try:
                os.makedirs(output_path, exist_ok=True)
            except PermissionError as e:
                raise PermissionError(f"No permission to create directory '{output_directory}': {str(e)}")
            except OSError as e:
                raise RuntimeError(f"Failed to create output directory '{output_directory}': {str(e)}")
                
            # Check if we can write to the output directory
            if not os.access(output_path, os.W_OK):
                raise PermissionError(f"No permission to write to directory '{output_directory}'")
            
            # Get all files in the snapshot
            files = self.db.get_snapshot_files(snapshot_id)
            logger.info(f"Found {len(files)} files to restore")
            
            # Create all necessary directories first
            directories = {str(Path(file_info['path']).parent) for file_info in files}
            dir_count = 0
            for directory in directories:
                if directory and directory != '.':
                    dir_path = output_path / directory
                    try:
                        os.makedirs(dir_path, exist_ok=True)
                        dir_count += 1
                    except PermissionError as e:
                        raise PermissionError(f"No permission to create directory '{dir_path}': {str(e)}")
                    except OSError as e:
                        raise RuntimeError(f"Failed to create directory '{dir_path}': {str(e)}")
            
            logger.info(f"Created {dir_count} directories in '{output_directory}'")
            
            # Restore each file
            restored_count = 0
            skipped_count = 0
            total_size = 0
            
            for file_info in files:
                file_path = output_path / file_info['path']
                
                try:
                    # Get the file content and write it to the output path
                    content = self.db.get_file_content(file_info['content_hash'])
                    if content is None:
                        logger.error(f"File content not found for hash {file_info['content_hash']}")
                        raise RuntimeError(f"File content not found for hash {file_info['content_hash']}")
                    
                    # Write the content to the file
                    with open(file_path, 'wb') as f:
                        f.write(content)
                    
                    total_size += len(content)
                    restored_count += 1
                    
                    if restored_count % 100 == 0:
                        logger.info(f"Restored {restored_count}/{len(files)} files")
                        
                except PermissionError as e:
                    logger.warning(f"Permission denied when writing file '{file_path}': {str(e)}")
                    skipped_count += 1
                except OSError as e:
                    logger.warning(f"Failed to write file '{file_path}': {str(e)}")
                    skipped_count += 1
            
            if skipped_count > 0:
                logger.warning(f"Skipped {skipped_count} files due to errors")
                
            logger.info(f"Successfully restored {restored_count}/{len(files)} files from snapshot {snapshot_id} to '{output_directory}' ({total_size / 1_048_576:.2f} MB)")
            return True
        except (ValueError, PermissionError) as e:
            # Re-raise these specific exceptions without wrapping
            logger.error(f"Error restoring snapshot: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error restoring snapshot: {str(e)}")
            raise RuntimeError(f"Failed to restore snapshot: {str(e)}") from e

    def prune(self, snapshot_id: int) -> bool:
        """
        Prune a snapshot and any unreferenced data.
        
        This method removes a snapshot and any content that is unique to it,
        ensuring that no data is lost from remaining snapshots.
        
        Args:
            snapshot_id (int): ID of the snapshot to prune
            
        Returns:
            bool: True if the prune was successful
            
        Raises:
            ValueError: If the snapshot doesn't exist or ID is invalid
            RuntimeError: If there's an error during the prune process
        """
        if not isinstance(snapshot_id, int) or snapshot_id <= 0:
            raise ValueError(f"Invalid snapshot ID: {snapshot_id}")
            
        try:
            logger.info(f"Pruning snapshot {snapshot_id}")
            
            # Get snapshot distinct size before pruning for logging
            try:
                distinct_size = self.db.get_snapshot_distinct_size(snapshot_id)
                logger.info(f"Snapshot {snapshot_id} has {distinct_size} KB of unique content")
            except Exception as e:
                logger.warning(f"Could not determine unique content size: {str(e)}")
                distinct_size = "unknown"
            
            self.db.prune_snapshot(snapshot_id)
            logger.info(f"Successfully pruned snapshot {snapshot_id}, freed approximately {distinct_size} KB")
            return True
        except ValueError as e:
            # Re-raise ValueError without wrapping
            logger.error(f"Error pruning snapshot: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error pruning snapshot: {str(e)}")
            raise RuntimeError(f"Failed to prune snapshot: {str(e)}") from e

    def check(self) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Check the database for corrupted file content.
        
        This method verifies that all file content stored in the database has the correct hash value. 
        It recalculates the hash for each stored blob and compares it with the key hash.
        
        Returns:
            Tuple[bool, List[Dict[str, Any]]]: A tuple containing:
                - A boolean indicating if all content matches the expected hash.
                - A list of dictionaries with information about corrupted content
            
        Raises:
            RuntimeError: If there's an error during the check process
        """
        try:
            logger.info("Starting database integrity check")
            
            # Use the database class's integrity check method
            all_valid, corrupted_items = self.db.check_integrity()
            
            if all_valid:
                logger.info("Database integrity check passed. All content is valid.")
            else:
                logger.warning(f"Database integrity check failed. Found {len(corrupted_items)} corrupted items.")
                
                # Log details about corrupted items
                for i, item in enumerate(corrupted_items, 1):
                    logger.debug(f"Corrupted item {i}:")
                    logger.debug(f"  Stored hash: {item['stored_hash']}")
                    logger.debug(f"  Calculated hash: {item['calculated_hash']}")
                    logger.debug(f"  Affected files: {len(item['affected_files'])}")
            
            return all_valid, corrupted_items
        except Exception as e:
            logger.error(f"Error checking database integrity: {str(e)}")
            raise RuntimeError(f"Failed to check database integrity: {str(e)}") from e

    def close(self) -> None:
        """
        Close the database connection.
        
        This method should be called when the operations object is no longer needed
        to ensure all database connections are properly closed.
        """
        if hasattr(self, 'db') and self.db:
            logger.debug("Closing database connection")
            self.db.close()

    def __enter__(self) -> 'BackupOperations':
        """
        Support for context manager protocol.
        
        Returns:
            BackupOperations: Self reference for context manager usage
        """
        logger.debug("Entering context manager for BackupOperations")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Support for context manager protocol. Ensures resources are cleaned up.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        logger.debug("Exiting context manager for BackupOperations")
        self.close()
