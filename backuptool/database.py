import os
import sqlite3
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class BackupDatabase:
    """Handles all database operations for the backup tool."""

    def __init__(self, db_path="backups.db"):
        """Initialize the database connection and create tables if they don't exist."""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        """Create the necessary tables if they don't exist."""
        cursor = self.conn.cursor()
        
        # Create snapshots table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL
        )
        ''')
        
        # Create files table (stores file metadata)
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
        
        # Create contents table (stores actual file contents)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS contents (
            hash TEXT PRIMARY KEY,
            data BLOB NOT NULL
        )
        ''')
        
        self.conn.commit()

    def add_snapshot(self) -> int:
        """Add a new snapshot and return its ID."""
        cursor = self.conn.cursor()
        timestamp = datetime.now().isoformat()
        cursor.execute("INSERT INTO snapshots (timestamp) VALUES (?)", (timestamp,))
        self.conn.commit()
        return cursor.lastrowid

    def get_file_hash(self, snapshot_id: int, file_path: str) -> Optional[str]:
        """Get the content hash for a file in a specific snapshot."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT content_hash FROM files WHERE snapshot_id = ? AND path = ?",
            (snapshot_id, file_path)
        )
        result = cursor.fetchone()
        return result['content_hash'] if result else None

    def add_file(self, snapshot_id: int, file_path: str, content_hash: str):
        """Add a file entry to the database."""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO files (snapshot_id, path, content_hash) VALUES (?, ?, ?)",
            (snapshot_id, file_path, content_hash)
        )
        self.conn.commit()

    def add_content(self, content_hash: str, data: bytes):
        """Add file content to the database if it doesn't already exist."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM contents WHERE hash = ?", (content_hash,))
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO contents (hash, data) VALUES (?, ?)",
                (content_hash, data)
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

    def get_snapshot_files(self, snapshot_id: int) -> List[Dict]:
        """Get all files for a specific snapshot."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id, path, content_hash FROM files WHERE snapshot_id = ?",
            (snapshot_id,)
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_file_content(self, content_hash: str) -> Optional[bytes]:
        """Get file content by hash."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT data FROM contents WHERE hash = ?", (content_hash,))
        result = cursor.fetchone()
        return result['data'] if result else None

    def get_snapshot_size(self, snapshot_id: int) -> int:
        """
        Get the total size of all files in a snapshot.
        This represents how much disk space the directory consumed at snapshot time.
        
        Args:
            snapshot_id: ID of the snapshot
            
        Returns:
            Total size in kilobytes
        """
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
        """
        Get the size of content that is unique to this snapshot.
        This represents how much space would be freed by pruning the snapshot.
        
        Args:
            snapshot_id: ID of the snapshot
            
        Returns:
            Size of unique content in kilobytes
        """
        cursor = self.conn.cursor()
        
        # Get all content hashes used in this snapshot
        cursor.execute("""
            SELECT DISTINCT content_hash
            FROM files
            WHERE snapshot_id = ?
        """, (snapshot_id,))
        snapshot_hashes = {row['content_hash'] for row in cursor.fetchall()}
        
        # Get content hashes used in other snapshots
        cursor.execute("""
            SELECT DISTINCT content_hash
            FROM files
            WHERE snapshot_id != ?
        """, (snapshot_id,))
        other_hashes = {row['content_hash'] for row in cursor.fetchall()}
        
        # Find hashes unique to this snapshot
        unique_hashes = snapshot_hashes - other_hashes
        
        if not unique_hashes:
            return 0
        
        # Get the total size of content with unique hashes
        placeholders = ','.join(['?'] * len(unique_hashes))
        cursor.execute(f"""
            SELECT SUM(LENGTH(data)) as unique_size
            FROM contents
            WHERE hash IN ({placeholders})
        """, tuple(unique_hashes))
        result = cursor.fetchone()
        return int(result['unique_size'] / 1024) if result and result['unique_size'] else 0

    def get_database_size(self) -> int:
        """
        Get the total size of all content stored in the database.
        
        Returns:
            Total database size in kilobytes
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT SUM(LENGTH(data)) as total_size FROM contents")
        result = cursor.fetchone()
        return int(result['total_size'] / 1024) if result and result['total_size'] else 0

    def prune_snapshot(self, snapshot_id: int):
        """Remove a snapshot and clean up any unreferenced content."""
        cursor = self.conn.cursor()
        
        # Check if the snapshot exists
        cursor.execute("SELECT 1 FROM snapshots WHERE id = ?", (snapshot_id,))
        if not cursor.fetchone():
            raise ValueError(f"Snapshot {snapshot_id} does not exist")
        
        # Get all content hashes for the snapshot to be pruned
        cursor.execute(
            "SELECT content_hash FROM files WHERE snapshot_id = ?",
            (snapshot_id,)
        )
        pruned_hashes = {row['content_hash'] for row in cursor.fetchall()}
        
        # Delete the files related to the snapshot
        cursor.execute("DELETE FROM files WHERE snapshot_id = ?", (snapshot_id,))
        
        # Delete the snapshot itself
        cursor.execute("DELETE FROM snapshots WHERE id = ?", (snapshot_id,))
        
        # Find hashes that are still in use in other snapshots
        cursor.execute("SELECT DISTINCT content_hash FROM files")
        active_hashes = {row['content_hash'] for row in cursor.fetchall()}
        
        # Delete contents that are no longer referenced
        for hash_to_check in pruned_hashes:
            if hash_to_check not in active_hashes:
                cursor.execute("DELETE FROM contents WHERE hash = ?", (hash_to_check,))
        
        self.conn.commit()

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

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
