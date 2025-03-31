import os
import pytest
import tempfile
import uuid
import shutil
import time
from pathlib import Path

from backuptool.operations import BackupOperations
from backuptool.database import BackupDatabase


# ---- Individual fixtures for flexible test composition ----

@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def test_db_path(temp_dir):
    """Create a unique database path for testing."""
    return temp_dir / f"test_{uuid.uuid4().hex[:8]}.db"


@pytest.fixture
def source_dir(temp_dir):
    """Create a source directory with test files."""
    source_dir = temp_dir / "source"
    os.makedirs(source_dir)
    
    # Create some test files
    for i in range(1, 5):
        with open(source_dir / f"file_{i}.txt", "w") as f:
            f.write(f"Content of file {i}")
    
    # Create a binary file
    with open(source_dir / "binary.bin", "wb") as f:
        f.write(os.urandom(1024))  # 1KB of random data
    
    return source_dir


@pytest.fixture
def restore_dir(temp_dir):
    """Create a restore directory for testing."""
    restore_dir = temp_dir / "restore"
    os.makedirs(restore_dir)
    return restore_dir


@pytest.fixture
def db_connection(test_db_path):
    """Create and return a database connection."""
    db = BackupDatabase(str(test_db_path))
    yield db
    
    # Safe cleanup
    try:
        db.close()
    except Exception:
        pass
    
    # Wait a moment to ensure all file handles are released
    time.sleep(0.1)
    
    # Try to remove the database file explicitly
    try:
        if os.path.exists(test_db_path):
            os.unlink(test_db_path)
    except (PermissionError, OSError):
        pass


# ---- Base test class for inheritance-based testing ----

class TestBase:
    """Base class for all backup tool tests providing isolation and cleanup."""
    
    def setUp(self):
        """
        Set up the test environment.
        
        This method:
        1. Creates a temporary directory
        2. Sets up source and restore directories
        3. Creates a unique database path
        4. Creates test files
        5. Initializes the database
        """
        # Create temporary directory
        self.temp_dir = tempfile.TemporaryDirectory()
        self.working_dir = Path(self.temp_dir.name)
        
        # Create source and restore directories
        self.source_dir = self.working_dir / "source"
        self.restore_dir = self.working_dir / "restore"
        os.makedirs(self.source_dir)
        os.makedirs(self.restore_dir)
        
        # Create a unique database path for each test
        self.db_path = self.working_dir / f"test_{uuid.uuid4().hex[:8]}.db"
        
        # Create test files
        self._create_test_files()
        
        # Initialize database
        self.db = BackupDatabase(str(self.db_path))
    
    def tearDown(self):
        """
        Clean up after the test.
        
        This method:
        1. Closes the database connection
        2. Removes the database file
        3. Cleans up the temporary directory
        """
        self._safe_cleanup()
    
    @pytest.fixture(autouse=True)
    def _setup_teardown_fixture(self):
        """
        Pytest fixture to automatically call setUp and tearDown.
        
        This fixture is automatically used by all test methods in classes
        that inherit from TestBase.
        """
        # Setup
        self.setUp()
        
        # Run the test
        yield
        
        # Teardown
        self.tearDown()
    
    def _create_test_files(self):
        """Create test files in the source directory."""
        # Create some test files
        for i in range(1, 5):
            with open(self.source_dir / f"file_{i}.txt", "w") as f:
                f.write(f"Content of file {i}")
        
        # Create a binary file
        with open(self.source_dir / "binary.bin", "wb") as f:
            f.write(os.urandom(1024))  # 1KB of random data
    
    def _safe_cleanup(self):
        """
        Safely clean up resources, handling potential issues with
        database connections and file locks.
        """
        # Close database connection
        try:
            self.db.close()
        except Exception:
            pass
        
        # Wait a moment to ensure all file handles are released
        time.sleep(0.1)
        
        # Try to remove the database file explicitly
        try:
            if os.path.exists(self.db_path):
                os.unlink(self.db_path)
        except (PermissionError, OSError):
            pass
        
        # Clean up temporary directory
        try:
            self.temp_dir.cleanup()
        except (PermissionError, OSError) as e:
            print(f"Warning: Could not clean up temporary directory: {e}")
    
    def run_command(self, args):
        """
        Run a backuptool command with the test database.
        
        Args:
            args: List of command arguments
            
        Returns:
            The BackupOperations instance used
        """
        # Add database path if not specified
        if "--db-path" not in " ".join(args):
            args = ["--db-path", str(self.db_path)] + args
        
        # Parse the command
        command = args[0] if args else None
        
        # Initialize operations
        ops = BackupOperations(str(self.db_path))
        
        try:
            if command == "snapshot":
                target_dir = next((args[i+1] for i, arg in enumerate(args) if arg == "--target-directory"), None)
                if target_dir:
                    ops.snapshot(target_dir)
            elif command == "list":
                ops.list_snapshots()
            elif command == "restore":
                snapshot_id = next((int(args[i+1]) for i, arg in enumerate(args) if arg == "--snapshot-number"), None)
                output_dir = next((args[i+1] for i, arg in enumerate(args) if arg == "--output-directory"), None)
                if snapshot_id and output_dir:
                    ops.restore(snapshot_id, output_dir)
            elif command == "prune":
                snapshot_id = next((int(args[i+1]) for i, arg in enumerate(args) if arg == "--snapshot"), None)
                if snapshot_id:
                    ops.prune(snapshot_id)
        except Exception as e:
            print(f"Error executing command: {e}")
            raise
        
        return ops


# ---- Helper functions for both approaches ----

def create_test_files(directory, count=5):
    """Create test files in the specified directory."""
    # Create text files
    for i in range(1, count):
        with open(directory / f"file_{i}.txt", "w") as f:
            f.write(f"Content of file {i}")
    
    # Create a binary file
    with open(directory / "binary.bin", "wb") as f:
        f.write(os.urandom(1024))  # 1KB of random data
