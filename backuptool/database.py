import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class BackupDatabase:

    def __init__(self, db_path="backups.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        cursor = self.conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            path TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            FOREIGN KEY (snapshot_id) REFERENCES snapshots(id),
            UNIQUE (snapshot_id, path)
        )
        ''')
        
    def add_snapshot(self) -> int:
        cursor = self.conn.cursor()
        timestamp = datetime.now().isoformat()
        cursor.execute("INSERT INTO snapshots (timestamp) VALUES (?)", (timestamp,))
        self.conn.commit()
        return cursor.lastrowid

    def add_file(self, snapshot_id: int, file_path: str, content_hash: str):
        """Add a file entry to the database."""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO files (snapshot_id, path, content_hash) VALUES (?, ?, ?)",
            (snapshot_id, file_path, content_hash)
        )
        self.conn.commit()

    def content_exists(self, content_hash: str) -> bool:
        """Check if content with the given hash exists in the database."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM contents WHERE hash = ?", (content_hash,))
        return cursor.fetchone() is not None

    def get_snapshots(self) -> List[Dict]:
        """Get all snapshots."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, timestamp FROM snapshots ORDER BY id")
        return [dict(row) for row in cursor.fetchall()]

    def get_snapshot(self, snapshot_id: int) -> Optional[Dict]:
        """Get a specific snapshot by ID."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, timestamp FROM snapshots WHERE id = ?", (snapshot_id,))
        result = cursor.fetchone()
        return dict(result) if result else None

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
