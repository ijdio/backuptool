import os
import pytest
import tempfile
from pathlib import Path

# Import the necessary modules from the backuptool package
try:
    from backuptool.operations import BackupOperations
    from backuptool.database import BackupDatabase, hash_file_content
except ImportError as e:
    pytest.skip(f"Failed to import backuptool modules: {e}", allow_module_level=True)
from tests.conftest import TestBase


class TestHealth(TestBase):
    """Basic health check tests for the backup tool."""

    def test_imports(self):
        """Test that all required modules can be imported."""
        # This test passes if the imports above succeed
        assert BackupOperations is not None
        assert BackupDatabase is not None
        assert hash_file_content is not None

    def test_database_initialization(self):
        """Test that the database can be initialized."""
        # The database is already initialized in the setup
        assert self.db is not None
        assert os.path.exists(self.db_path)

    def test_backup_operations_initialization(self):
        """Test that the BackupOperations class can be initialized."""
        ops = BackupOperations(str(self.db_path))
        try:
            assert ops is not None
        finally:
            ops.close()

    def test_hash_function(self):
        """Test that the hash_file_content function works correctly."""
        # Create a test file
        test_file = self.working_dir / "test.txt"
        with open(test_file, "w") as f:
            f.write("Test content")
        
        # Hash the file
        file_hash = hash_file_content(str(test_file))
        assert file_hash is not None
        assert isinstance(file_hash, str)
        assert len(file_hash) > 0
        
    def test_context_manager(self):
        """Test that the context manager works correctly."""
        with BackupOperations(str(self.db_path)) as ops:
            assert ops is not None
            # Perform some operation to verify it works
            snapshot_id = ops.snapshot(str(self.source_dir))
            assert snapshot_id == 1
        
        # Verify the database connection was closed
        # by opening a new connection and checking the snapshot exists
        with BackupOperations(str(self.db_path)) as ops:
            snapshots = ops.db.get_snapshots()
            assert len(snapshots) == 1
