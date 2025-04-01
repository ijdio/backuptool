import os
import shutil
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import hashlib

from .database import BackupDatabase, hash_file_content


class BackupOperations:
    """Handles core backup operations like snapshot, restore, list, and prune."""

    def __init__(self, db_path="backups.db"):
        """Initialize BackupOperations with a database path."""
        self.db = BackupDatabase(db_path)

    def snapshot(self, target_directory: str) -> int:
        """
        Take a snapshot of the specified directory.
        
        Args:
            target_directory: Path to the directory to snapshot
            
        Returns:
            The snapshot ID
        """
        target_path = Path(target_directory).resolve()
        
        if not target_path.exists() or not target_path.is_dir():
            raise ValueError(f"Target directory {target_directory} does not exist or is not a directory")
        
        # Create a new snapshot
        snapshot_id = self.db.add_snapshot()
        
        # Walk through the directory and process each file
        for root, _, files in os.walk(target_path):
            for file in files:
                file_path = Path(root) / file
                relative_path = file_path.relative_to(target_path)
                
                # Hash the file content
                content_hash = hash_file_content(str(file_path))
                
                # Add the file to the database
                self.db.add_file(snapshot_id, str(relative_path), content_hash)
                
                # Store the file content if it doesn't exist already
                if not self.db.content_exists(content_hash):
                    with open(file_path, 'rb') as f:
                        file_content = f.read()
                    self.db.add_content(content_hash, file_content)
        
        return snapshot_id

    def list_snapshots(self) -> Tuple[List[Dict], int]:
        """
        List all snapshots with disk usage metrics.
        
        Returns:
            A tuple containing:
            - A list of dictionaries with snapshot information including size metrics
            - The total database size in kilobytes
        """
        snapshots = self.db.get_snapshots()
        
        # Enhance snapshots with disk usage metrics
        for snapshot in snapshots:
            snapshot_id = snapshot['id']
            snapshot['size'] = self.db.get_snapshot_size(snapshot_id)
            snapshot['distinct_size'] = self.db.get_snapshot_distinct_size(snapshot_id)
        
        # Get total database size
        total_size = self.db.get_database_size()
        
        return snapshots, total_size

    def restore(self, snapshot_id: int, output_directory: str) -> bool:
        """
        Restore a snapshot to the specified directory.
        
        Args:
            snapshot_id: ID of the snapshot to restore
            output_directory: Directory to restore the snapshot to
            
        Returns:
            True if the restore was successful, False otherwise
        """
        # Check if the snapshot exists
        snapshot = self.db.get_snapshot(snapshot_id)
        if not snapshot:
            raise ValueError(f"Snapshot {snapshot_id} does not exist")
        
        # Create the output directory if it doesn't exist
        output_path = Path(output_directory).resolve()
        os.makedirs(output_path, exist_ok=True)
        
        # Get all files in the snapshot
        files = self.db.get_snapshot_files(snapshot_id)
        
        # Restore each file
        for file_info in files:
            file_path = output_path / file_info['path']
            
            # Create parent directories if they don't exist
            os.makedirs(file_path.parent, exist_ok=True)
            
            # Get the file content and write it to the output path
            content = self.db.get_file_content(file_info['content_hash'])
            if content is None:
                raise RuntimeError(f"File content not found for hash {file_info['content_hash']}")
            
            with open(file_path, 'wb') as f:
                f.write(content)
        
        return True

    def prune(self, snapshot_id: int) -> bool:
        """
        Prune a snapshot and any unreferenced data.
        
        Args:
            snapshot_id: ID of the snapshot to prune
            
        Returns:
            True if the prune was successful, False otherwise
        """
        try:
            self.db.prune_snapshot(snapshot_id)
            return True
        except ValueError as e:
            print(f"Error: {str(e)}")
            return False

    def check(self) -> Tuple[bool, List[Dict]]:
        """
        Check the database for corrupted file content.
        
        This method verifies that all file content stored in the database has the correct hash value. 
        It recalculates the hash for each stored blob and compares it with the key hash.
        
        Returns:
            A tuple containing:
            - A boolean indicating if all content matches the expected hash.
            - A list of dictionaries with information about corrupted content
        """
        cursor = self.db.conn.cursor()
        
        # Get all content hashes and data from the database
        cursor.execute("SELECT hash, data FROM contents")
        contents = cursor.fetchall()
        
        corrupted_items = []
        all_valid = True
        
        for content in contents:
            stored_hash = content[0]
            data = content[1]
            
            # Recalculate the hash from the stored data
            calculated_hash = hashlib.sha256(data).hexdigest()
            
            # Check if the calculated hash matches the stored hash
            if calculated_hash != stored_hash:
                all_valid = False
                
                # Find which files use this corrupted content
                cursor.execute("""
                    SELECT f.path, s.id as snapshot_id, s.timestamp
                    FROM files f
                    JOIN snapshots s ON f.snapshot_id = s.id
                    WHERE f.content_hash = ?
                """, (stored_hash,))
                
                affected_files = cursor.fetchall()
                
                corrupted_item = {
                    'stored_hash': stored_hash,
                    'calculated_hash': calculated_hash,
                    'affected_files': [
                        {
                            'path': file[0],
                            'snapshot_id': file[1],
                            'timestamp': file[2]
                        } for file in affected_files
                    ]
                }
                
                corrupted_items.append(corrupted_item)
        
        return all_valid, corrupted_items

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
