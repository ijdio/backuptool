import os
import sys
import subprocess
import pytest
from pathlib import Path
import sqlite3
from backuptool.operations import BackupOperations
from tests.conftest import TestBase


class TestCLI(TestBase):
    """Test CLI commands with proper isolation."""

    def _run_cli_command(self, args):
        """Run a CLI command with the test database path."""
        # Add the database path if not specified
        if "--db-path" not in " ".join(args):
            args = ["--db-path", str(self.db_path)] + args
            
        # Run the command
        cmd = [sys.executable, "-m", "backuptool"] + args
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result

    def test_snapshot_command(self):
        """Test the snapshot command via CLI."""
        # Run the snapshot command
        result = self._run_cli_command([
            "snapshot", 
            "--target-directory", str(self.source_dir)
        ])
        
        # Verify command succeeded
        assert result.returncode == 0
        assert "Snapshot 1 created successfully" in result.stdout
        
        # Verify the snapshot exists in the database
        with BackupOperations(str(self.db_path)) as ops:
            snapshots = ops.db.get_snapshots()
            assert len(snapshots) == 1

    def test_list_command(self):
        """Test the list command via CLI."""
        # First create a snapshot
        self._run_cli_command([
            "snapshot", 
            "--target-directory", str(self.source_dir)
        ])
        
        # Run the list command
        result = self._run_cli_command(["list"])
        
        # Verify command succeeded
        assert result.returncode == 0
        assert "SNAPSHOT" in result.stdout
        assert "TIMESTAMP" in result.stdout
        assert "SIZE" in result.stdout
        assert "DISTINCT_SIZE" in result.stdout
        assert "total" in result.stdout
        
    def test_restore_command(self):
        """Test the restore command via CLI."""
        # First create a snapshot
        self._run_cli_command([
            "snapshot", 
            "--target-directory", str(self.source_dir)
        ])
        
        # Run the restore command
        result = self._run_cli_command([
            "restore",
            "--snapshot-number", "1",
            "--output-directory", str(self.restore_dir)
        ])
        
        # Verify command succeeded
        assert result.returncode == 0
        assert f"Snapshot 1 restored to {self.restore_dir}" in result.stdout
        
        # Verify files were restored
        source_files = set(f.name for f in self.source_dir.glob("*"))
        restored_files = set(f.name for f in self.restore_dir.glob("*"))
        assert source_files == restored_files
        
    def test_prune_command(self):
        """Test the prune command via CLI."""
        # First create a snapshot
        self._run_cli_command([
            "snapshot", 
            "--target-directory", str(self.source_dir)
        ])
        
        # Run the prune command
        result = self._run_cli_command([
            "prune",
            "--snapshot", "1"
        ])
        
        # Verify command succeeded
        assert result.returncode == 0
        assert "Snapshot 1 pruned successfully" in result.stdout
        
        # Verify the snapshot was removed
        with BackupOperations(str(self.db_path)) as ops:
            snapshots = ops.db.get_snapshots()
            assert len(snapshots) == 0
            
    def test_db_path_argument(self):
        """Test that the --db-path argument works correctly."""
        # Create a custom database path
        custom_db_path = self.working_dir / "custom.db"
        
        # Run a command with the custom database path
        # The --db-path argument must come before the subcommand
        result = self._run_cli_command(["--db-path", str(custom_db_path), "snapshot", "--target-directory", str(self.source_dir)])
        assert result.returncode == 0
        
        # Verify the database was created at the custom path
        assert custom_db_path.exists()
    
    def test_check_command(self):
        """Test that the check command works correctly."""
        # First create a snapshot to have some data
        result = self._run_cli_command(["snapshot", "--target-directory", str(self.source_dir)])
        assert result.returncode == 0
        
        # Run the check command - should pass
        result = self._run_cli_command(["check"])
        assert result.returncode == 0
        assert "Database integrity check passed" in result.stdout
        
        # Manually corrupt the database
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            
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
                conn.commit()
        
        # Run the check command again - should fail
        result = self._run_cli_command(["check"])
        assert result.returncode == 1
        assert "Database integrity check FAILED" in result.stdout
        assert "Corrupted content detected" in result.stdout
