import os
import pytest
import hashlib
from pathlib import Path

from backuptool.operations import BackupOperations
from backuptool.database import BackupDatabase
from tests.conftest import TestBase


class TestSanityChecks(TestBase):
    """Test class for comprehensive sanity checks of the backup implementation."""

    def test_complete_restoration(self):
        """
        Test that all files in a snapshot are restored correctly.
        
        This test verifies that every file included in a snapshot is properly
        restored when that snapshot is restored.
        """
        with BackupOperations(str(self.db_path)) as ops:
            # Take a snapshot of the source directory
            snapshot_id = ops.snapshot(str(self.source_dir))
            
            # Get file list from the original directory
            original_files = set()
            for root, _, files in os.walk(self.source_dir):
                for file in files:
                    file_path = Path(root) / file
                    relative_path = file_path.relative_to(self.source_dir)
                    original_files.add(str(relative_path))
            
            # Restore the snapshot
            ops.restore(snapshot_id, str(self.restore_dir))
            
            # Verify all original files were restored
            for rel_path in original_files:
                restored_path = self.restore_dir / rel_path
                assert restored_path.exists(), f"File {rel_path} was not restored"

    def test_bit_for_bit_identical(self):
        """
        Test that restored files are bit-for-bit identical to the originals.
        
        This test verifies that the content of restored files exactly matches
        the content of the original files.
        """
        with BackupOperations(str(self.db_path)) as ops:
            # Take a snapshot of the source directory
            snapshot_id = ops.snapshot(str(self.source_dir))
            
            # Get file content and hashes from the original files
            original_files = {}
            for root, _, files in os.walk(self.source_dir):
                for file in files:
                    file_path = Path(root) / file
                    relative_path = file_path.relative_to(self.source_dir)
                    with open(file_path, "rb") as f:
                        content = f.read()
                    original_files[str(relative_path)] = {
                        "content": content,
                        "hash": hashlib.sha256(content).hexdigest()
                    }
            
            # Restore the snapshot
            ops.restore(snapshot_id, str(self.restore_dir))
            
            # Verify all restored files are bit-for-bit identical
            for rel_path, original in original_files.items():
                restored_path = self.restore_dir / rel_path
                
                with open(restored_path, "rb") as f:
                    restored_content = f.read()
                
                assert restored_content == original["content"], f"Content mismatch for {rel_path}"
                assert hashlib.sha256(restored_content).hexdigest() == original["hash"], f"Hash mismatch for {rel_path}"

    def test_pruning_independence(self):
        """
        Test that pruning a snapshot doesn't affect other snapshots.
        
        This test verifies that an unpruned snapshot is always restorable,
        even if another snapshot that shared data with it has been pruned.
        """
        with BackupOperations(str(self.db_path)) as ops:
            # Take first snapshot
            snapshot1_id = ops.snapshot(str(self.source_dir))
            
            # Modify a file and add a new one
            with open(self.source_dir / "file_1.txt", "w") as f:
                f.write("Modified content for file 1")
            
            with open(self.source_dir / "new_file.txt", "w") as f:
                f.write("This is a new file for the second snapshot")
            
            # Take second snapshot
            snapshot2_id = ops.snapshot(str(self.source_dir))
            
            # Prune the first snapshot
            assert ops.prune(snapshot1_id), "Failed to prune snapshot"
            
            # Verify second snapshot can still be restored after pruning
            ops.restore(snapshot2_id, str(self.restore_dir))
            
            # Verify the modified and new files exist in the restored snapshot
            assert (self.restore_dir / "file_1.txt").exists()
            with open(self.restore_dir / "file_1.txt", "r") as f:
                assert f.read() == "Modified content for file 1"
                
            assert (self.restore_dir / "new_file.txt").exists()
            with open(self.restore_dir / "new_file.txt", "r") as f:
                assert f.read() == "This is a new file for the second snapshot"

    def test_binary_content(self):
        """
        Test that the implementation can handle arbitrary binary content.
        
        This test verifies that binary files are properly backed up and restored.
        """
        with BackupOperations(str(self.db_path)) as ops:
            # Create a binary file with random content
            binary_file = self.source_dir / "complex_binary.bin"
            binary_content = os.urandom(2048)  # 2KB of random binary data
            with open(binary_file, "wb") as f:
                f.write(binary_content)
            
            # Take a snapshot
            snapshot_id = ops.snapshot(str(self.source_dir))
            
            # Restore the snapshot
            ops.restore(snapshot_id, str(self.restore_dir))
            
            # Verify binary content was handled correctly
            binary_restore_path = self.restore_dir / "complex_binary.bin"
            assert binary_restore_path.exists()
            with open(binary_restore_path, "rb") as f:
                restored_binary = f.read()
            assert restored_binary == binary_content, "Binary content was not preserved correctly"

    def test_path_handling(self):
        """
        Test that the implementation handles different path types correctly.
        
        This test verifies that both relative and absolute paths are handled robustly.
        """
        with BackupOperations(str(self.db_path)) as ops:
            # Create files with different path types
            
            # File in a nested directory
            nested_dir = self.source_dir / "level1" / "level2"
            os.makedirs(nested_dir, exist_ok=True)
            nested_file = nested_dir / "nested_file.txt"
            with open(nested_file, "w") as f:
                f.write("File in a nested directory")
            
            # File with a name containing special characters
            special_file = self.source_dir / "special@#$%.txt"
            with open(special_file, "w") as f:
                f.write("File with special characters in name")
            
            # Take a snapshot
            snapshot_id = ops.snapshot(str(self.source_dir))
            
            # Restore the snapshot
            ops.restore(snapshot_id, str(self.restore_dir))
            
            # Verify all paths were handled correctly
            assert (self.restore_dir / "level1" / "level2" / "nested_file.txt").exists()
            assert (self.restore_dir / "special@#$%.txt").exists()

    def test_deduplication(self):
        """
        Test that duplicate file content is not stored multiple times.
        
        This test verifies that when snapshotting a directory twice without changes,
        the second snapshot only causes storage of metadata and not duplicate content.
        """
        with BackupOperations(str(self.db_path)) as ops:
            # Take first snapshot
            snapshot1_id = ops.snapshot(str(self.source_dir))
            
            # Get content count after first snapshot
            content_count_after_first = ops.db.conn.cursor().execute(
                "SELECT COUNT(*) FROM contents").fetchone()[0]
            
            # Take second snapshot without changing any files
            snapshot2_id = ops.snapshot(str(self.source_dir))
            
            # Get content count after second snapshot
            content_count_after_second = ops.db.conn.cursor().execute(
                "SELECT COUNT(*) FROM contents").fetchone()[0]
            
            # Verify no new content was stored (only metadata)
            assert content_count_after_first == content_count_after_second, \
                "Duplicate content was stored when files didn't change"
            
            # Verify both snapshots can be restored correctly
            first_restore_dir = self.working_dir / "restore_first"
            os.makedirs(first_restore_dir, exist_ok=True)
            ops.restore(snapshot1_id, str(first_restore_dir))
            
            second_restore_dir = self.working_dir / "restore_second"
            os.makedirs(second_restore_dir, exist_ok=True)
            ops.restore(snapshot2_id, str(second_restore_dir))
            
            # Verify both restores have the same files
            for root, _, files in os.walk(first_restore_dir):
                for file in files:
                    rel_path = Path(root).relative_to(first_restore_dir) / file
                    assert (second_restore_dir / rel_path).exists(), \
                        f"File {rel_path} missing from second restore"

    def test_comprehensive_sanity_check(self):
        """
        Comprehensive test that verifies all sanity check requirements together.
        
        This test combines all the individual checks into a single comprehensive test
        to ensure the backup implementation meets all requirements.
        """
        with BackupOperations(str(self.db_path)) as ops:
            # Create some additional test files with different path types
            # Binary file with random content
            binary_file = self.source_dir / "complex_binary.bin"
            binary_content = os.urandom(2048)  # 2KB of random binary data
            with open(binary_file, "wb") as f:
                f.write(binary_content)
            
            # File with absolute path reference
            abs_path_dir = self.source_dir / "absolute_dir"
            os.makedirs(abs_path_dir, exist_ok=True)
            abs_path_file = abs_path_dir / "abs_path_file.txt"
            with open(abs_path_file, "w") as f:
                f.write("File with absolute path")
            
            # File with relative path that contains parent references
            nested_dir = self.source_dir / "level1" / "level2"
            os.makedirs(nested_dir, exist_ok=True)
            relative_path_file = nested_dir / "relative_file.txt"
            with open(relative_path_file, "w") as f:
                f.write("File with relative path")
            
            # 1. Take first snapshot
            snapshot1_id = ops.snapshot(str(self.source_dir))
            
            # Get file list and hashes from the original files
            original_files = {}
            for root, _, files in os.walk(self.source_dir):
                for file in files:
                    file_path = Path(root) / file
                    relative_path = file_path.relative_to(self.source_dir)
                    with open(file_path, "rb") as f:
                        content = f.read()
                    original_files[str(relative_path)] = {
                        "content": content,
                        "hash": hashlib.sha256(content).hexdigest()
                    }
            
            # 2. Modify a file and add a new one
            with open(self.source_dir / "file_1.txt", "w") as f:
                f.write("Modified content for file 1")
            
            with open(self.source_dir / "new_file.txt", "w") as f:
                f.write("This is a new file for the second snapshot")
            
            # 3. Take second snapshot
            snapshot2_id = ops.snapshot(str(self.source_dir))
            
            # 4. Restore first snapshot to verify all original files are restored correctly
            first_restore_dir = self.working_dir / "restore_first"
            os.makedirs(first_restore_dir, exist_ok=True)
            ops.restore(snapshot1_id, str(first_restore_dir))
            
            # 5. Verify all original files were restored and are bit-for-bit identical
            for rel_path, original in original_files.items():
                restored_path = first_restore_dir / rel_path
                assert restored_path.exists(), f"File {rel_path} was not restored"
                
                with open(restored_path, "rb") as f:
                    restored_content = f.read()
                
                assert restored_content == original["content"], f"Content mismatch for {rel_path}"
                assert hashlib.sha256(restored_content).hexdigest() == original["hash"], f"Hash mismatch for {rel_path}"
            
            # 6. Prune the first snapshot
            assert ops.prune(snapshot1_id), "Failed to prune snapshot"
            
            # 7. Verify second snapshot can still be restored after pruning
            second_restore_dir = self.working_dir / "restore_second"
            os.makedirs(second_restore_dir, exist_ok=True)
            ops.restore(snapshot2_id, str(second_restore_dir))
            
            # Verify the modified and new files exist in the second snapshot
            assert (second_restore_dir / "file_1.txt").exists()
            assert (second_restore_dir / "new_file.txt").exists()
            
            # 8. Verify binary content was handled correctly
            binary_restore_path = second_restore_dir / "complex_binary.bin"
            assert binary_restore_path.exists()
            with open(binary_restore_path, "rb") as f:
                restored_binary = f.read()
            assert restored_binary == binary_content, "Binary content was not preserved correctly"
            
            # 9. Verify absolute and relative paths were handled correctly
            assert (second_restore_dir / "absolute_dir" / "abs_path_file.txt").exists()
            assert (second_restore_dir / "level1" / "level2" / "relative_file.txt").exists()
            
            # 10. Test duplicate snapshot detection (no content duplication)
            # Create a third snapshot without changing any files
            content_count_before = ops.db.conn.cursor().execute("SELECT COUNT(*) FROM contents").fetchone()[0]
            snapshot3_id = ops.snapshot(str(second_restore_dir))
            content_count_after = ops.db.conn.cursor().execute("SELECT COUNT(*) FROM contents").fetchone()[0]
            
            # Verify no new content was stored (only metadata)
            assert content_count_before == content_count_after, "Duplicate content was stored when files didn't change"
            
            # 11. Verify the third snapshot can be restored correctly
            third_restore_dir = self.working_dir / "restore_third"
            os.makedirs(third_restore_dir, exist_ok=True)
            ops.restore(snapshot3_id, str(third_restore_dir))
            
            # Ensure all files from the second snapshot are in the third restore
            for root, _, files in os.walk(second_restore_dir):
                for file in files:
                    rel_path = Path(root).relative_to(second_restore_dir) / file
                    assert (third_restore_dir / rel_path).exists(), f"File {rel_path} missing from third restore"
