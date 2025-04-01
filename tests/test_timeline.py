import os
import pytest
import shutil
import hashlib
from pathlib import Path
from datetime import datetime

from backuptool.operations import BackupOperations
from backuptool.database import BackupDatabase
from tests.conftest import TestBase


class TestTimelineScenarios(TestBase):
    """
    Tests that simulate realistic backup scenarios with complex timelines.
    
    These tests create multiple snapshots over time with various file changes,
    restores, and prune operations to ensure the backup system works correctly
    in realistic scenarios.
    """
    
    def _create_file_with_content(self, path, content):
        """Helper to create a file with specific content."""
        with open(path, 'wb' if isinstance(content, bytes) else 'w') as f:
            f.write(content)
    
    def _verify_file_content(self, path, expected_content):
        """Helper to verify file content matches expected content."""
        mode = 'rb' if isinstance(expected_content, bytes) else 'r'
        with open(path, mode) as f:
            actual_content = f.read()
        assert actual_content == expected_content, f"Content mismatch for {path}"
    
    def _create_fixed_file_structure(self, base_dir):
        """
        Create a fixed file structure for testing.
        
        Args:
            base_dir: The base directory to create files in
            
        Returns:
            A dictionary mapping relative paths to file contents
        """
        file_map = {}
        
        # Create a fixed directory structure
        dirs = [
            base_dir / "dir_1",
            base_dir / "dir_2",
            base_dir / "dir_1" / "subdir_1",
            base_dir / "dir_2" / "subdir_2"
        ]
        
        for directory in dirs:
            os.makedirs(directory, exist_ok=True)
        
        # Create text files
        text_files = [
            (base_dir / "file_1.txt", "This is test file 1"),
            (base_dir / "file_2.txt", "This is test file 2"),
            (base_dir / "dir_1" / "file_3.txt", "This is test file 3 in dir_1"),
            (base_dir / "dir_2" / "file_4.txt", "This is test file 4 in dir_2"),
            (base_dir / "dir_1" / "subdir_1" / "file_5.txt", "This is test file 5 in subdir_1")
        ]
        
        for file_path, content in text_files:
            self._create_file_with_content(file_path, content)
            rel_path = file_path.relative_to(base_dir)
            file_map[str(rel_path)] = content
        
        # Create binary files
        binary_files = [
            (base_dir / "binary_1.bin", b"\x00\x01\x02\x03\x04"),
            (base_dir / "dir_2" / "binary_2.bin", b"\x05\x06\x07\x08\x09"),
            (base_dir / "dir_2" / "subdir_2" / "binary_3.bin", b"\x0A\x0B\x0C\x0D\x0E\x0F")
        ]
        
        for file_path, content in binary_files:
            self._create_file_with_content(file_path, content)
            rel_path = file_path.relative_to(base_dir)
            file_map[str(rel_path)] = content
        
        return file_map
    
    def _modify_files_stage_1(self, base_dir, file_map):
        """
        Apply first set of modifications to files.
        
        Args:
            base_dir: The base directory containing files
            file_map: The current mapping of relative paths to contents
            
        Returns:
            Updated file map with modifications
        """
        # Make a copy of the file map so we don't modify the original
        updated_map = file_map.copy()
        
        # Modify existing files
        modifications = [
            ("file_1.txt", "This is test file 1 - MODIFIED"),
            ("dir_1\\file_3.txt", "This is test file 3 in dir_1 - MODIFIED")
        ]
        
        for rel_path, new_content in modifications:
            file_path = base_dir / rel_path
            self._create_file_with_content(file_path, new_content)
            updated_map[rel_path] = new_content
        
        # Add new files
        additions = [
            ("new_file_1.txt", "This is a new file 1"),
            ("dir_1\\new_file_2.txt", "This is a new file 2 in dir_1")
        ]
        
        for rel_path, content in additions:
            file_path = base_dir / rel_path
            self._create_file_with_content(file_path, content)
            updated_map[rel_path] = content
        
        # Delete files
        deletions = ["dir_2\\file_4.txt"]
        
        for rel_path in deletions:
            file_path = base_dir / rel_path
            try:
                os.unlink(file_path)
                del updated_map[rel_path]
            except OSError:
                pass  # File might already be deleted
            except KeyError:
                # The key might be using a different path separator
                pass
        
        return updated_map
    
    def _modify_files_stage_2(self, base_dir, file_map):
        """
        Apply second set of modifications to files.
        
        Args:
            base_dir: The base directory containing files
            file_map: The current mapping of relative paths to contents
            
        Returns:
            Updated file map with modifications
        """
        # Make a copy of the file map so we don't modify the original
        updated_map = file_map.copy()
        
        # Modify existing files
        modifications = [
            ("file_2.txt", "This is test file 2 - MODIFIED IN STAGE 2"),
            ("dir_1\\new_file_2.txt", "This is a new file 2 in dir_1 - MODIFIED")
        ]
        
        for rel_path, new_content in modifications:
            file_path = base_dir / rel_path
            self._create_file_with_content(file_path, new_content)
            updated_map[rel_path] = new_content
        
        # Add new files
        additions = [
            ("new_file_3.txt", "This is a new file 3 added in stage 2"),
            ("dir_2\\new_file_4.bin", b"\x10\x11\x12\x13\x14")
        ]
        
        for rel_path, content in additions:
            file_path = base_dir / rel_path
            self._create_file_with_content(file_path, content)
            updated_map[rel_path] = content
        
        # Delete files
        deletions = ["new_file_1.txt"]
        
        for rel_path in deletions:
            file_path = base_dir / rel_path
            try:
                os.unlink(file_path)
                del updated_map[rel_path]
            except OSError:
                pass  # File might already be deleted
            except KeyError:
                # The key might be using a different path separator
                pass
        
        return updated_map
    
    def test_complex_timeline(self):
        """
        Test a complex timeline of snapshots, changes, restores, and prunes.
        
        This test simulates a realistic backup scenario with:
        - Multiple snapshots over time
        - File modifications, additions, and deletions between snapshots
        - Restores from various points in time
        - Pruning of old snapshots
        - Verification of data integrity throughout
        """
        with BackupOperations(str(self.db_path)) as ops:
            # Initial set of files
            print("\nCreating initial file structure...")
            initial_files = self._create_fixed_file_structure(self.source_dir)
            
            # Dictionary to track snapshots and their file maps
            snapshots = {}
            
            # Take initial snapshot
            print("Taking initial snapshot...")
            snapshot_id = ops.snapshot(str(self.source_dir))
            snapshots[snapshot_id] = initial_files.copy()
            
            # First set of modifications
            print("\nMaking first set of changes...")
            modified_files_1 = self._modify_files_stage_1(self.source_dir, initial_files)
            
            # Take second snapshot
            print("Taking second snapshot...")
            snapshot_id = ops.snapshot(str(self.source_dir))
            snapshots[snapshot_id] = modified_files_1.copy()
            
            # Second set of modifications
            print("\nMaking second set of changes...")
            modified_files_2 = self._modify_files_stage_2(self.source_dir, modified_files_1)
            
            # Take third snapshot
            print("Taking third snapshot...")
            snapshot_id = ops.snapshot(str(self.source_dir))
            snapshots[snapshot_id] = modified_files_2.copy()
            
            # Create restore directories for each snapshot
            restore_dirs = {}
            for snap_id in snapshots.keys():
                restore_dir = self.working_dir / f"restore_{snap_id}"
                os.makedirs(restore_dir, exist_ok=True)
                restore_dirs[snap_id] = restore_dir
            
            # Test restoring each snapshot and verify content
            print("\nTesting restores for each snapshot...")
            for snap_id, file_map in snapshots.items():
                print(f"Restoring snapshot {snap_id}...")
                ops.restore(snap_id, str(restore_dirs[snap_id]))
                
                # Verify all files from the snapshot exist in the restored directory
                for rel_path, content in file_map.items():
                    restored_path = restore_dirs[snap_id] / rel_path
                    assert restored_path.exists(), f"File {rel_path} not restored from snapshot {snap_id}"
                    self._verify_file_content(restored_path, content)
            
            # Test pruning the first snapshot
            print("\nPruning first snapshot...")
            first_snapshot_id = list(snapshots.keys())[0]
            assert ops.prune(first_snapshot_id), f"Failed to prune snapshot {first_snapshot_id}"
            
            # Verify remaining snapshots can still be restored
            remaining_ids = list(snapshots.keys())[1:]
            for snap_id in remaining_ids:
                print(f"\nVerifying snapshot {snap_id} after pruning...")
                
                # Create a new restore directory
                verify_dir = self.working_dir / f"verify_{snap_id}"
                os.makedirs(verify_dir, exist_ok=True)
                
                # Restore and verify
                ops.restore(snap_id, str(verify_dir))
                
                # Check all files
                file_map = snapshots[snap_id]
                for rel_path in file_map.keys():
                    restored_path = verify_dir / rel_path
                    assert restored_path.exists(), f"File {rel_path} missing after pruning and restore"
                    self._verify_file_content(restored_path, file_map[rel_path])
    
    def test_incremental_changes(self):
        """
        Test a scenario with incremental changes to the same files over time.
        
        This test focuses on how the backup system handles incremental changes
        to the same set of files, testing both storage efficiency and restore
        accuracy from different points in time.
        """
        with BackupOperations(str(self.db_path)) as ops:
            # Create initial file structure with consistent files
            base_files = {}
            for i in range(5):
                filename = f"file_{i}.txt"
                content = f"Initial content for file {i}\n"
                file_path = self.source_dir / filename
                self._create_file_with_content(file_path, content)
                base_files[filename] = content
            
            # Take initial snapshot
            snapshot_ids = []
            file_versions = []
            
            snapshot_id = ops.snapshot(str(self.source_dir))
            snapshot_ids.append(snapshot_id)
            file_versions.append(base_files.copy())
            
            # Make incremental changes to files over multiple iterations
            current_files = base_files.copy()
            
            # Define specific files to modify in each iteration
            modifications = [
                # Iteration 1: modify files 0, 1, 2
                [0, 1, 2],
                # Iteration 2: modify files 1, 3, 4
                [1, 3, 4],
                # Iteration 3: modify files 0, 2, 4
                [0, 2, 4],
                # Iteration 4: modify files 1, 2, 3
                [1, 2, 3],
                # Iteration 5: modify files 0, 3, 4
                [0, 3, 4]
            ]
            
            for iteration, files_to_modify in enumerate(modifications):
                # Modify each selected file by appending new content
                for file_idx in files_to_modify:
                    filename = f"file_{file_idx}.txt"
                    file_path = self.source_dir / filename
                    new_content = current_files[filename] + f"Change from iteration {iteration + 1}\n"
                    self._create_file_with_content(file_path, new_content)
                    current_files[filename] = new_content
                
                # Take a snapshot after changes
                snapshot_id = ops.snapshot(str(self.source_dir))
                snapshot_ids.append(snapshot_id)
                file_versions.append(current_files.copy())
            
            # Verify database deduplication
            # The number of content entries should be less than the number of file entries
            cursor = ops.db.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM contents")
            content_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM files")
            file_count = cursor.fetchone()[0]
            
            # We expect content count to be less than file count due to deduplication
            print(f"\nContent count: {content_count}, File count: {file_count}")
            assert content_count < file_count, "Content deduplication may not be working correctly"
            
            # Restore each snapshot and verify file contents at each point in time
            for i, (snapshot_id, file_version) in enumerate(zip(snapshot_ids, file_versions)):
                restore_dir = self.working_dir / f"restore_incr_{i}"
                os.makedirs(restore_dir, exist_ok=True)
                
                # Restore the snapshot
                ops.restore(snapshot_id, str(restore_dir))
                
                # Verify all files have the correct version for this snapshot
                for filename, expected_content in file_version.items():
                    restored_path = restore_dir / filename
                    assert restored_path.exists(), f"File {filename} missing in snapshot {i}"
                    self._verify_file_content(restored_path, expected_content)
    
    def test_large_files_and_restores(self):
        """
        Test handling of large files and large restore operations.
        
        This test verifies that the backup system can correctly handle large files
        and restore operations involving many files.
        """
        # Create a moderate number of files including some large ones
        with BackupOperations(str(self.db_path)) as ops:
            file_map = {}
            
            # Create some regular small files
            for i in range(10):
                filename = f"small_file_{i}.txt"
                content = f"This is a small file with index {i}. " * 20  # ~600 bytes
                file_path = self.source_dir / filename
                self._create_file_with_content(file_path, content)
                file_map[filename] = content
            
            # Create a few medium-sized files (100KB)
            for i in range(3):
                filename = f"medium_file_{i}.bin"
                # Create deterministic binary content
                content = bytes([i % 256 for _ in range(100 * 1024)])  # 100KB
                file_path = self.source_dir / filename
                self._create_file_with_content(file_path, content)
                file_map[filename] = content
            
            # Create one large file (1MB)
            large_file = "large_file.bin"
            # Create deterministic binary content
            large_content = bytes([i % 256 for i in range(1024 * 1024)])  # 1MB
            large_path = self.source_dir / large_file
            self._create_file_with_content(large_path, large_content)
            file_map[large_file] = large_content
            
            # Take a snapshot
            print("\nTaking snapshot of directory with large files...")
            snapshot_id = ops.snapshot(str(self.source_dir))
            
            # Restore to a different directory
            print("Restoring snapshot with large files...")
            ops.restore(snapshot_id, str(self.restore_dir))
            
            # Verify all files were restored correctly
            for filename, content in file_map.items():
                restored_path = self.restore_dir / filename
                assert restored_path.exists(), f"File {filename} not restored"
                self._verify_file_content(restored_path, content)
            
            # Verify the large file specifically
            restored_large = self.restore_dir / large_file
            assert restored_large.exists(), "Large file not restored"
            assert restored_large.stat().st_size == len(large_content), "Large file size mismatch"
            
            # For large files, instead of loading entire content, just check the hash
            original_hash = hashlib.sha256(large_content).hexdigest()
            with open(restored_large, 'rb') as f:
                restored_hash = hashlib.sha256(f.read()).hexdigest()
            assert restored_hash == original_hash, "Large file content mismatch"
