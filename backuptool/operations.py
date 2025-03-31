import os
from pathlib import Path
from typing import List, Dict, Tuple

from .database import BackupDatabase, hash_file_content


class BackupOperations:
    def __init__(self, db_path="backups.db"):
        self.db = BackupDatabase(db_path)

    def snapshot(self, target_directory: str) -> int:
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
        snapshots = self.db.get_snapshots()

        for snapshot in snapshots:
            snapshot_id = snapshot['id']
            snapshot['size'] = self.db.get_snapshot_size(snapshot_id)
            snapshot['distinct_size'] = self.db.get_snapshot_distinct_size(snapshot_id)

        total_size = self.db.get_database_size()

        return snapshots, total_size

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
