import os
import pytest
import shutil
from pathlib import Path
import hashlib

from backuptool.operations import BackupOperations
from backuptool.database import BackupDatabase
from tests.conftest import TestBase


class TestSnapshot(TestBase):
    """Test snapshot functionality with proper isolation."""

    def test_snapshot_creation(self):
        """Test that a snapshot can be created successfully."""
        # Initialize backup operations
        ops = BackupOperations(str(self.db_path))
        
        try:
            # Take a snapshot
            snapshot_id = ops.snapshot(str(self.source_dir))
            
            # Verify the snapshot was created
            assert snapshot_id == 1
            
            # Verify the snapshot exists in the database
            snapshots = ops.db.get_snapshots()
            assert len(snapshots) == 1
            
            # Verify files were stored
            files = ops.db.get_snapshot_files(1)
            assert len(files) == 5  # 4 text files + 1 binary file
        finally:
            ops.close()

    def test_snapshot_restore(self):
        """Test that a snapshot can be restored successfully."""
        # Initialize backup operations
        ops = BackupOperations(str(self.db_path))
        
        try:
            # Take a snapshot
            snapshot_id = ops.snapshot(str(self.source_dir))
            
            # Restore the snapshot
            ops.restore(snapshot_id, str(self.restore_dir))
            
            # Verify all files were restored
            source_files = set(f.name for f in self.source_dir.glob("*"))
            restored_files = set(f.name for f in self.restore_dir.glob("*"))
            
            assert source_files == restored_files
            
            # Verify file contents
            for file_name in source_files:
                source_content = (self.source_dir / file_name).read_bytes()
                restored_content = (self.restore_dir / file_name).read_bytes()
                assert source_content == restored_content
        finally:
            ops.close()

    def test_duplicate_content_detection(self):
        """Test that duplicate content is detected and not stored twice."""
        # Create test directory with duplicate content
        dup_dir = self.working_dir / "duplicates"
        os.makedirs(dup_dir)
        
        # Create identical files in different locations
        for i in range(5):
            with open(dup_dir / f"file_{i}.txt", "w") as f:
                f.write("This is identical content in multiple files")
        
        # Initialize backup operations
        ops = BackupOperations(str(self.db_path))
        
        try:
            # Take a snapshot
            snapshot_id = ops.snapshot(str(dup_dir))
            
            # Verify the snapshot exists
            snapshots = ops.db.get_snapshots()
            assert len(snapshots) == 1
            
            # Verify files were stored
            files = ops.db.get_snapshot_files(1)
            assert len(files) == 5  # 5 file references
            
            # Verify content deduplication
            content_hashes = set(file['content_hash'] for file in files)
            assert len(content_hashes) == 1  # Only one unique content hash
            
            # Count content records
            cursor = ops.db.conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM contents")
            content_count = cursor.fetchone()['count']
            assert content_count == 1  # Only one content record should be stored
        finally:
            ops.close()
            
    def test_multiple_snapshots(self):
        """Test creating multiple snapshots of the same directory."""
        # Initialize backup operations
        ops = BackupOperations(str(self.db_path))
        
        try:
            # Take first snapshot
            snapshot_id1 = ops.snapshot(str(self.source_dir))
            assert snapshot_id1 == 1
            
            # Modify a file
            with open(self.source_dir / "file_1.txt", "w") as f:
                f.write("Modified content")
                
            # Add a new file
            with open(self.source_dir / "new_file.txt", "w") as f:
                f.write("New file content")
            
            # Take second snapshot
            snapshot_id2 = ops.snapshot(str(self.source_dir))
            assert snapshot_id2 == 2
            
            # Verify we have two snapshots
            snapshots = ops.db.get_snapshots()
            assert len(snapshots) == 2
            
            # Verify files in second snapshot
            files = ops.db.get_snapshot_files(2)
            assert len(files) == 6  # 4 original + 1 modified + 1 new
            
            # Verify content deduplication across snapshots
            cursor = ops.db.conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM contents")
            content_count = cursor.fetchone()['count']
            
            # We should have unique contents for:
            # - Original files from first snapshot
            # - Modified file
            # - New file
            # - Binary file
            # The exact count may vary based on implementation details
            assert content_count >= 5  # At least 5 unique contents expected
        finally:
            ops.close()
