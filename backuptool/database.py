import os
import sqlite3
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


class BackupDatabase:
    """Handles database operations required for snapshot and list commands."""

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

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS contents (
            hash TEXT PRIMARY KEY,
            data BLOB NOT NULL
        )
        ''')

        self.conn.commit()

    def add_snapshot(self) -> int:
        cursor = self.conn.cursor()
        timestamp = datetime.now().isoformat()
        cursor.execute("INSERT INTO snapshots (timestamp) VALUES (?)", (timestamp,))
        self.conn.commit()
        return cursor.lastrowid

    def add_file(self, snapshot_id: int, file_path: str, content_hash: str):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO files (snapshot_id, path, content_hash) VALUES (?, ?, ?)",
            (snapshot_id, file_path, content_hash)
        )
        self.conn.commit()

    def add_content(self, content_hash: str, data: bytes):
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM contents WHERE hash = ?", (content_hash,))
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO contents (hash, data) VALUES (?, ?)",
                (content_hash, data)
            )
            self.conn.commit()

    def content_exists(self, content_hash: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM contents WHERE hash = ?", (content_hash,))
        return cursor.fetchone() is not None

    def get_snapshots(self) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, timestamp FROM snapshots ORDER BY id")
        return [dict(row) for row in cursor.fetchall()]

    def get_snapshot_size(self, snapshot_id: int) -> int:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT SUM(LENGTH(c.data)) as total_size
            FROM files f
            JOIN contents c ON f.content_hash = c.hash
            WHERE f.snapshot_id = ?
        """, (snapshot_id,))
        result = cursor.fetchone()
        return int(result['total_size'] / 1024) if result and result['total_size'] else 0

    def get_snapshot_distinct_size(self, snapshot_id: int) -> int:
        cursor = self.conn.cursor()

        cursor.execute("""
            SELECT DISTINCT content_hash
            FROM files
            WHERE snapshot_id = ?
        """, (snapshot_id,))
        snapshot_hashes = {row['content_hash'] for row in cursor.fetchall()}

        cursor.execute("""
            SELECT DISTINCT content_hash
            FROM files
            WHERE snapshot_id != ?
        """, (snapshot_id,))
        other_hashes = {row['content_hash'] for row in cursor.fetchall()}

        unique_hashes = snapshot_hashes - other_hashes

        if not unique_hashes:
            return 0

        placeholders = ','.join(['?'] * len(unique_hashes))
        cursor.execute(f"""
            SELECT SUM(LENGTH(data)) as unique_size
            FROM contents
            WHERE hash IN ({placeholders})
        """, tuple(unique_hashes))
        result = cursor.fetchone()
        return int(result['unique_size'] / 1024) if result and result['unique_size'] else 0

    def get_database_size(self) -> int:
        cursor = self.conn.cursor()
        cursor.execute("SELECT SUM(LENGTH(data)) as total_size FROM contents")
        result = cursor.fetchone()
        return int(result['total_size'] / 1024) if result and result['total_size'] else 0

    def close(self):
        if self.conn:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def hash_file_content(file_path: str) -> str:
    """Generate a SHA-256 hash for a file's content."""
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            sha256.update(chunk)
    return sha256.hexdigest()
