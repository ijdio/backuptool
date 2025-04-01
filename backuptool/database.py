import os
import sqlite3
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set, Any, Generator, Union, Iterator


# Get logger instance (configuration is handled in cli.py)
logger = logging.getLogger('backuptool')


class BackupDatabase:
    """
    Handles all database operations for the backup tool.
    
    This class manages SQLite database connections and provides methods
    for storing and retrieving backup snapshots, file metadata, and content.
    """

    def __init__(self, db_path: str = "backups.db"):
        """
        Initialize the database connection and create tables if they don't exist.
        
        Args:
            db_path (str, optional): Path to the SQLite database file. Defaults to "backups.db".
            
        Raises:
            sqlite3.Error: If there's an error connecting to the database
            PermissionError: If there's no permission to access the database file
        """
        self.db_path = db_path
        try:
            self.conn = sqlite3.connect(db_path)
            self.conn.row_factory = sqlite3.Row
            self._create_tables()
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise sqlite3.Error(f"Failed to connect to database: {str(e)}")
        except PermissionError as e:
            logger.error(f"Permission denied when accessing database at '{db_path}': {str(e)}")
            raise PermissionError(f"Permission denied when accessing database at '{db_path}': {str(e)}")

    def _create_tables(self) -> None:
        """
        Create the necessary tables if they don't exist.
        
        Raises:
            sqlite3.Error: If there's an error executing SQL
        """
        try:
            cursor = self.conn.cursor()
            
            # Create snapshots table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                size INTEGER
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
        except sqlite3.Error as e:
            logger.error(f"Failed to create database tables: {str(e)}")
            self.conn.rollback()
            raise sqlite3.Error(f"Failed to create database tables: {str(e)}")

    def add_snapshot(self) -> int:
        """
        Add a new snapshot and return its ID.
        
        Returns:
            int: ID of the newly created snapshot
            
        Raises:
            sqlite3.Error: If there's an error adding the snapshot
        """
        try:
            cursor = self.conn.cursor()
            timestamp = datetime.now().isoformat()
            cursor.execute("INSERT INTO snapshots (timestamp, size) VALUES (?, 0)", (timestamp,))
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Failed to add snapshot: {str(e)}")
            self.conn.rollback()
            raise sqlite3.Error(f"Failed to add snapshot: {str(e)}")

    def get_file_hash(self, snapshot_id: int, file_path: str) -> Optional[str]:
        """
        Get the content hash for a file in a specific snapshot.
        
        Args:
            snapshot_id (int): ID of the snapshot
            file_path (str): Path of the file relative to the snapshot root
            
        Returns:
            Optional[str]: Content hash of the file, or None if not found
            
        Raises:
            sqlite3.Error: If there's an error querying the database
            ValueError: If snapshot_id is invalid
        """
        if not isinstance(snapshot_id, int) or snapshot_id <= 0:
            raise ValueError(f"Invalid snapshot ID: {snapshot_id}")
            
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT content_hash FROM files WHERE snapshot_id = ? AND path = ?",
                (snapshot_id, file_path)
            )
            result = cursor.fetchone()
            return result['content_hash'] if result else None
        except sqlite3.Error as e:
            logger.error(f"Failed to get file hash: {str(e)}")
            raise sqlite3.Error(f"Failed to get file hash: {str(e)}")

    def add_file(self, snapshot_id: int, file_path: str, content_hash: str) -> None:
        """
        Add a file entry to the database.
        
        Args:
            snapshot_id (int): ID of the snapshot
            file_path (str): Path of the file relative to the snapshot root
            content_hash (str): Hash of the file content
            
        Raises:
            sqlite3.Error: If there's an error adding the file
            ValueError: If any parameter is invalid
        """
        if not isinstance(snapshot_id, int) or snapshot_id <= 0:
            raise ValueError(f"Invalid snapshot ID: {snapshot_id}")
        if not file_path:
            raise ValueError("File path cannot be empty")
        if not content_hash:
            raise ValueError("Content hash cannot be empty")
            
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT INTO files (snapshot_id, path, content_hash) VALUES (?, ?, ?)",
                (snapshot_id, file_path, content_hash)
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            self.conn.rollback()
            # Replace the existing file entry (helpful for retries or updates)
            cursor.execute(
                "UPDATE files SET content_hash = ? WHERE snapshot_id = ? AND path = ?",
                (content_hash, snapshot_id, file_path)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Failed to add file: {str(e)}")
            self.conn.rollback()
            raise sqlite3.Error(f"Failed to add file: {str(e)}")

    def add_content(self, content_hash: str, data: bytes) -> None:
        """
        Add file content to the database if it doesn't already exist.
        
        Args:
            content_hash (str): Hash of the content
            data (bytes): Binary content data
            
        Raises:
            sqlite3.Error: If there's an error adding the content
            ValueError: If content_hash is invalid
        """
        if not content_hash:
            raise ValueError("Content hash cannot be empty")
        if not isinstance(data, bytes):
            raise ValueError("Content data must be bytes")
            
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM contents WHERE hash = ?", (content_hash,))
            if not cursor.fetchone():
                cursor.execute(
                    "INSERT INTO contents (hash, data) VALUES (?, ?)",
                    (content_hash, data)
                )
                self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Failed to add content: {str(e)}")
            self.conn.rollback()
            raise sqlite3.Error(f"Failed to add content: {str(e)}")

    def content_exists(self, content_hash: str) -> bool:
        """
        Check if content with the given hash exists in the database.
        
        Args:
            content_hash (str): Hash to check
            
        Returns:
            bool: True if content exists, False otherwise
            
        Raises:
            sqlite3.Error: If there's an error querying the database
        """
        if not content_hash:
            raise ValueError("Content hash cannot be empty")
            
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM contents WHERE hash = ?", (content_hash,))
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Failed to check content existence: {str(e)}")
            raise sqlite3.Error(f"Failed to check content existence: {str(e)}")

    def get_snapshots(self) -> List[Dict[str, Any]]:
        """
        Get all snapshots ordered by ID.
        
        Returns:
            List[Dict[str, Any]]: List of snapshots with 'id' and 'timestamp' keys
            
        Raises:
            sqlite3.Error: If there's an error querying the database
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id, timestamp FROM snapshots ORDER BY id")
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Failed to get snapshots: {str(e)}")
            raise sqlite3.Error(f"Failed to get snapshots: {str(e)}")

    def get_snapshot(self, snapshot_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a specific snapshot by ID.
        
        Args:
            snapshot_id (int): ID of the snapshot to retrieve
            
        Returns:
            Optional[Dict[str, Any]]: Snapshot with 'id' and 'timestamp' keys, or None if not found
            
        Raises:
            sqlite3.Error: If there's an error querying the database
            ValueError: If snapshot_id is invalid
        """
        if not isinstance(snapshot_id, int) or snapshot_id <= 0:
            raise ValueError(f"Invalid snapshot ID: {snapshot_id}")
            
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id, timestamp FROM snapshots WHERE id = ?", (snapshot_id,))
            result = cursor.fetchone()
            return dict(result) if result else None
        except sqlite3.Error as e:
            logger.error(f"Failed to get snapshot: {str(e)}")
            raise sqlite3.Error(f"Failed to get snapshot: {str(e)}")

    def get_snapshot_files(self, snapshot_id: int) -> List[Dict[str, Any]]:
        """
        Get all files for a specific snapshot.
        
        Args:
            snapshot_id (int): ID of the snapshot
            
        Returns:
            List[Dict[str, Any]]: List of files with 'id', 'path', and 'content_hash' keys
            
        Raises:
            sqlite3.Error: If there's an error querying the database
            ValueError: If snapshot_id is invalid
        """
        if not isinstance(snapshot_id, int) or snapshot_id <= 0:
            raise ValueError(f"Invalid snapshot ID: {snapshot_id}")
            
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT id, path, content_hash FROM files WHERE snapshot_id = ?",
                (snapshot_id,)
            )
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Failed to get snapshot files: {str(e)}")
            raise sqlite3.Error(f"Failed to get snapshot files: {str(e)}")

    def get_file_content(self, content_hash: str) -> Optional[bytes]:
        """
        Get file content by hash.
        
        Args:
            content_hash (str): Hash of the content to retrieve
            
        Returns:
            Optional[bytes]: Binary content data, or None if not found
            
        Raises:
            sqlite3.Error: If there's an error querying the database
            ValueError: If content_hash is invalid
        """
        if not content_hash:
            raise ValueError("Content hash cannot be empty")
            
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT data FROM contents WHERE hash = ?", (content_hash,))
            result = cursor.fetchone()
            return result['data'] if result else None
        except sqlite3.Error as e:
            logger.error(f"Failed to get file content: {str(e)}")
            raise sqlite3.Error(f"Failed to get file content: {str(e)}")

    def get_snapshot_size(self, snapshot_id: int) -> int:
        """
        Get the total size of all files in a snapshot.
        This represents how much disk space the directory consumed at snapshot time.
        
        Args:
            snapshot_id (int): ID of the snapshot
            
        Returns:
            int: Total size in kilobytes
            
        Raises:
            sqlite3.Error: If there's an error querying the database
            ValueError: If snapshot_id is invalid
        """
        if not isinstance(snapshot_id, int) or snapshot_id <= 0:
            raise ValueError(f"Invalid snapshot ID: {snapshot_id}")
            
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT SUM(LENGTH(c.data)) as total_size
                FROM files f
                JOIN contents c ON f.content_hash = c.hash
                WHERE f.snapshot_id = ?
            """, (snapshot_id,))
            result = cursor.fetchone()
            return int(result['total_size'] / 1024) if result and result['total_size'] else 0
        except sqlite3.Error as e:
            logger.error(f"Failed to get snapshot size: {str(e)}")
            raise sqlite3.Error(f"Failed to get snapshot size: {str(e)}")

    def get_snapshot_distinct_size(self, snapshot_id: int) -> int:
        """
        Get the size of content that is unique to this snapshot.
        This represents how much space would be freed by pruning the snapshot.
        
        Args:
            snapshot_id (int): ID of the snapshot
            
        Returns:
            int: Size of unique content in kilobytes
            
        Raises:
            sqlite3.Error: If there's an error querying the database
            ValueError: If snapshot_id is invalid
        """
        if not isinstance(snapshot_id, int) or snapshot_id <= 0:
            raise ValueError(f"Invalid snapshot ID: {snapshot_id}")
            
        try:
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
        except sqlite3.Error as e:
            logger.error(f"Failed to get snapshot distinct size: {str(e)}")
            raise sqlite3.Error(f"Failed to get snapshot distinct size: {str(e)}")

    def get_database_size(self) -> int:
        """
        Get the total size of all content stored in the database.
        
        Returns:
            int: Total database size in kilobytes
            
        Raises:
            sqlite3.Error: If there's an error querying the database
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT SUM(LENGTH(data)) as total_size FROM contents")
            result = cursor.fetchone()
            return int(result['total_size'] / 1024) if result and result['total_size'] else 0
        except sqlite3.Error as e:
            logger.error(f"Failed to get database size: {str(e)}")
            raise sqlite3.Error(f"Failed to get database size: {str(e)}")

    def get_total_size(self) -> int:
        """
        Calculate the total size of all snapshots in the database.
        
        Returns:
            int: Total size of all snapshots in kilobytes
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT SUM(size) AS total_size FROM snapshots")
            result = cursor.fetchone()
            return int(result['total_size'] / 1024) if result and result['total_size'] else 0
        except sqlite3.Error as e:
            logger.error(f"Failed to get total size: {str(e)}")
            raise RuntimeError(f"Failed to get total size: {str(e)}") from e

    def update_snapshot_size(self, snapshot_id: int, size: int) -> None:
        """
        Update the size of a snapshot in the database.
        
        Args:
            snapshot_id (int): ID of the snapshot to update
            size (int): Size in bytes
            
        Raises:
            sqlite3.Error: If there's an error updating the database
            ValueError: If snapshot_id is invalid
        """
        if not isinstance(snapshot_id, int) or snapshot_id <= 0:
            raise ValueError(f"Invalid snapshot ID: {snapshot_id}")
        if not isinstance(size, int) or size < 0:
            raise ValueError(f"Invalid size: {size}")
            
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE snapshots SET size = ? WHERE id = ?",
                (size, snapshot_id)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Failed to update snapshot size: {str(e)}")
            self.conn.rollback()
            raise sqlite3.Error(f"Failed to update snapshot size: {str(e)}")

    def prune_snapshot(self, snapshot_id: int) -> None:
        """
        Remove a snapshot and clean up any unreferenced content.
        
        Args:
            snapshot_id (int): ID of the snapshot to prune
            
        Raises:
            sqlite3.Error: If there's an error with database operations
            ValueError: If snapshot_id is invalid or doesn't exist
        """
        if not isinstance(snapshot_id, int) or snapshot_id <= 0:
            raise ValueError(f"Invalid snapshot ID: {snapshot_id}")
            
        try:
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
        except sqlite3.Error as e:
            logger.error(f"Failed to prune snapshot: {str(e)}")
            self.conn.rollback()
            raise sqlite3.Error(f"Failed to prune snapshot: {str(e)}")

    def check_integrity(self) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Check database integrity by verifying all stored content hashes.
        
        Returns:
            Tuple[bool, List[Dict[str, Any]]]: A tuple containing:
                - bool: True if all content is valid, False otherwise
                - List: List of corrupted items with details
                
        Raises:
            sqlite3.Error: If there's an error querying the database
        """
        try:
            cursor = self.conn.cursor()
            
            # Get all content hashes and data
            cursor.execute("SELECT hash, data FROM contents")
            contents = cursor.fetchall()
            
            corrupted_items = []
            
            for content in contents:
                stored_hash = content['hash']
                data = content['data']
                
                # Calculate hash from stored data
                calculated_hash = hashlib.sha256(data).hexdigest()
                
                # Check if hash matches
                if calculated_hash != stored_hash:
                    # Find all files affected by this corrupted content
                    cursor.execute("""
                        SELECT f.path, f.snapshot_id, s.timestamp
                        FROM files f
                        JOIN snapshots s ON f.snapshot_id = s.id
                        WHERE f.content_hash = ?
                    """, (stored_hash,))
                    
                    affected_files = [
                        {
                            'path': row['path'],
                            'snapshot_id': row['snapshot_id'],
                            'timestamp': row['timestamp']
                        }
                        for row in cursor.fetchall()
                    ]
                    
                    corrupted_items.append({
                        'stored_hash': stored_hash,
                        'calculated_hash': calculated_hash,
                        'affected_files': affected_files
                    })
            
            return len(corrupted_items) == 0, corrupted_items
        except sqlite3.Error as e:
            logger.error(f"Failed to check integrity: {str(e)}")
            raise sqlite3.Error(f"Failed to check integrity: {str(e)}")

    def close(self) -> None:
        """
        Close the database connection.
        """
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
            except sqlite3.Error:
                pass  # Ignore errors when closing
            self.conn = None

    def __enter__(self) -> 'BackupDatabase':
        """
        Context manager entry point.
        
        Returns:
            BackupDatabase: Self reference for context manager usage
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Context manager exit point that ensures the database connection is closed.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        self.close()


def hash_file_content(file_path: str) -> str:
    """
    Generate a SHA-256 hash for a file's content.
    
    Args:
        file_path (str): Path to the file to hash
        
    Returns:
        str: Hexadecimal digest of the SHA-256 hash
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        PermissionError: If there's no permission to read the file
        IOError: If there's an error reading the file
    """
    try:
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")
    except PermissionError:
        raise PermissionError(f"Permission denied when reading file: {file_path}")
    except IOError as e:
        raise IOError(f"Error reading file {file_path}: {str(e)}")
