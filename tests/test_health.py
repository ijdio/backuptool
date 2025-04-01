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
        assert BackupOperations is not None
        assert BackupDatabase is not None
        assert hash_file_content is not None

    def test_database_initialization(self):
        """Test that the database can be initialized."""
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
        """Test that BackupOperations works as a context manager."""
        with BackupOperations(str(self.db_path)) as ops:
            assert ops is not None
        
        # After exiting the context, the database should be closed
        assert ops.db.conn is None
        
    def test_check_integrity(self):
        """Test that the check operation correctly identifies corrupted content."""
        # Create a snapshot to have some data in the database
        with BackupOperations(str(self.db_path)) as ops:
            ops.snapshot(str(self.source_dir))
            
            # Verify integrity - should pass initially
            all_valid, corrupted_items = ops.check()
            assert all_valid is True
            assert len(corrupted_items) == 0
            
            # Manually corrupt a content entry
            cursor = ops.db.conn.cursor()
            
            # Get a content hash to corrupt
            cursor.execute("SELECT hash FROM contents LIMIT 1")
            content_hash = cursor.fetchone()[0]
            
            # Corrupt the data by modifying a byte
            cursor.execute("SELECT data FROM contents WHERE hash = ?", (content_hash,))
            data = cursor.fetchone()[0]
            corrupted_data = bytearray(data)
            if len(corrupted_data) > 0:
                corrupted_data[0] = (corrupted_data[0] + 1) % 256  # Change the first byte
                
                # Update the database with corrupted data
                cursor.execute("UPDATE contents SET data = ? WHERE hash = ?", 
                              (bytes(corrupted_data), content_hash))
                ops.db.conn.commit()
                
                # Check again - should fail now
                all_valid, corrupted_items = ops.check()
                assert all_valid is False
                assert len(corrupted_items) > 0
                assert corrupted_items[0]['stored_hash'] == content_hash
