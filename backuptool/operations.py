import os
from pathlib import Path
from typing import List, Dict, Tuple

from .database import BackupDatabase, hash_file_content


class BackupOperations:
    """Handles core backup operations: snapshot, restore, and list."""

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

        snapshot_id = self.db.add_snapshot()

        for root, _, files in os.walk(target_path):
            for file in files:
                file_path = Path(root) / file
                relative_path = file_path.relative_to(target_path)

                content_hash = hash_file_content(str(file_path))

                self.db.add_file(snapshot_id, str(relative_path), content_hash)

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

        for snapshot in snapshots:
            snapshot_id = snapshot['id']
            snapshot['size'] = self.db.get_snapshot_size(snapshot_id)
            snapshot['distinct_size'] = self.db.get_snapshot_distinct_size(snapshot_id)

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
        snapshot = self.db.get_snapshot(snapshot_id)
        if not snapshot:
            raise ValueError(f"Snapshot {snapshot_id} does not exist")

        output_path = Path(output_directory).resolve()
        os.makedirs(output_path, exist_ok=True)

        files = self.db.get_snapshot_files(snapshot_id)

        for file_info in files:
            file_path = output_path / file_info['path']
            os.makedirs(file_path.parent, exist_ok=True)

            content = self.db.get_file_content(file_info['content_hash'])
            if content is None:
                raise RuntimeError(f"File content not found for hash {file_info['content_hash']}")

            with open(file_path, 'wb') as f:
                f.write(content)

        return True

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
