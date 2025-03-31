import os
import shutil
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from .database import BackupDatabase, hash_file_content


class BackupOperations:
    """Handles core backup operations like snapshot, restore, list, and prune."""

    def __init__(self, db_path="backups.db"):
        """Initialize BackupOperations with a database path."""
        self.db = BackupDatabase(db_path)

    def snapshot(self, target_directory: str) -> int:
        target_path = Path(target_directory).resolve()
        
        if not target_path.exists() or not target_path.is_dir():
            raise ValueError(f"Target directory {target_directory} does not exist or is not a directory")
        
        # Create a new snapshot
        snapshot_id = self.db.add_snapshot()
        
        return snapshot_id

    def list_snapshots(self) -> Tuple[List[Dict], int]:
        snapshots = self.db.get_snapshots()
        return snapshots, total_size

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
