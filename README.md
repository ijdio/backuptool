# Backup Tool

This is a command-line file backup & restore utility designed to capture incremental snapshots of a directory using content-based deduplication.

- **Snapshot Creation**: Reads file contents, computes SHA-256 hashes, and stores only unique content.
- **Incremental Backups**: Detects changes and avoids redundant storage using content hashing.
- **Restore Functionality**: Reconstructs bit-for-bit file structure and content from any snapshot.
- **Pruning**: Removes obsolete snapshots without compromising restore fidelity.
- **Integrity checking**: Uses SHA-256 hash verification to detect corrupted file content.
- **Error Handling**: File I/O is delicate; it manages exceptions with robust fallback handling and database rollback.

## Installation

### Prerequisites

- Python 3.8+
- SQLite3 (included in Python standard library)

### Installation Options

#### Using a Virtual Environment (recommended)

```bash
# Clone the repository
git clone https://github.com/ijdio/backuptool.git
cd backuptool

# Create and activate a virtual environment
python -m venv .venv

# On Windows
.venv\Scripts\activate

# On macOS/Linux
source .venv/bin/activate

# Install dependencies and the package
pip install -e .
```

#### Installing Development Dependencies

If you intend to run tests, install the development dependencies:

```bash
# Install the package with development dependencies
pip install -e ".[dev]"

# Or install development dependencies separately
pip install pytest pytest-cov
```

#### Global Installation

```bash
# Clone the repository
git clone https://github.com/ijdio/backuptool.git
cd backuptool

# Install in development mode
pip install -e ".[dev]"
```

### Running the Tool

After installation, you can run the tool using one of these methods:

1. If you installed locally or in a virtual environment:
   ```bash
   python -m backuptool <command> [options]
   ```

2. If you installed globally:
   ```bash
   backuptool <command> [options]
   ```

## Usage

### Basic Commands

#### Taking a Snapshot

Captures the state of a directory with deduplication logic. Avoids storing duplicate files across snapshots.

```bash
python -m backuptool snapshot --target-directory=/path/to/backup
```

#### Listing Snapshots

Displays all snapshots with metadata, including disk usage (in kilobytes).

```bash
python -m backuptool list
```

Example output:
```
SNAPSHOT  TIMESTAMP             SIZE  DISTINCT_SIZE 
1         2025-03-30 20:47:09   224   0
2         2025-03-30 20:47:38   224   0
3         2025-03-31 15:00:52   224   0
4         2025-03-31 20:33:50   176   176
total                           400
```

#### Restoring a Snapshot

Recreates a snapshot exactly as it was taken. Ensures restored files are bit-for-bit identical.

```bash
python -m backuptool restore --snapshot-number=2 --output-directory=/path/to/restore
```

#### Pruning a Snapshot

Deletes a specified snapshot and cleans up unreferenced data. Other snapshots remain unaffected.

```bash
python -m backuptool prune --snapshot=1
```

#### Checking Database Integrity

Verifies the integrity of the database by checking all records and ensuring they are uncompromised.

```bash
python -m backuptool check
```

### Advanced Usage

#### Custom Database Location

By default, the tool stores its database in a file named `backups.db` in the current directory. You can specify a custom location:

```bash
python -m backuptool --db-path=/path/to/database.db snapshot --target-directory=/path/to/backup
```

## Core Functional Modules

`backuptool/cli.py`
   Main entry point -- implements the command-line interface
   - Maps user commands (snapshot, list, restore, prune, check) to their implementations
   - Accepts snapshot IDs, directory paths, and other flags with clear defaults
   - Handles exceptions, messaging, & error codes

`backuptool/operations.py`
   Handles backup logic.
   - Snapshotting: Reads file contents, computes SHA-256 hashes, and stores only unique content.
   - Listing: Displays all snapshots with metadata, including disk usage (in kilobytes).
   - Restoring: Reconstructs the exact file structure and content from any snapshot.
   - Pruning: Removes obsolete snapshots while ensuring remaining snapshots are still restorable.
   - Checking: Verifies Database integrity

`backuptool/database.py`
   Manages low-level database operations.
   - Maintains data integrity & transactions
   - Uses commit/rollback for consistency
   - Content deduplication

The tool uses SQLite for data storage with three main tables:
- `snapshots`: Records metadata for each snapshot
- `files`: Maps files to their content in each snapshot
- `contents`: Stores the actual file content, referenced by SHA-256 hash

## Development

### Running Tests

```bash
# If you haven't installed development dependencies yet
pip install -e ".[dev]"

# Run tests
python -m pytest

# Run tests with coverage
python -m pytest --cov=backuptool
```

### Testing Strategy

This project includes an expansive suite of tests using pytest and are designed to run in isolated environments using fixtures and subprocess-based CLI invocations.

- Tests span from unit-level checks to full integration scenarios.
- All tests run in isolated environments using temporary files and directories.
- Emphasis on correctness, storage efficiency, fault tolerance, and CLI behavior under realistic conditions.

#### Common Test Environment (`conftest.py`)

Sets up reusable, isolated test environments using `pytest` fixtures and a base test class.

- **Fixtures**: Create temporary directories (`temp_dir`, `source_dir`, `restore_dir`) and unique database paths (`test_db_path`).
- **Data Setup**: `source_dir` includes sample text and binary files to simulate real usage.
- **Database Management**: `db_connection` initializes and cleans up test databases between tests.
- **`TestBase` Class**: Provides auto-applied setup/teardown for working directories, test files, and clean DB initialization.

#### Health Checks (`test_health.py`)

Verifies basic system integrity:

- Component imports
- Database file creation
- Proper use of `BackupOperations` as a context manager
- Hashing correctness
- Corruption detection via integrity checks

#### CLI Functional Tests (`test_cli.py`)

Verifies CLI behavior using subprocess execution of the full application:

- Snapshot: Confirms snapshot creation, output, and DB persistence.
- List: Validates snapshot metadata output.
- Restore: Confirms accurate file and directory reconstruction.
- Prune: Validates snapshot deletion and storage cleanup.
- Custom DB Path: Ensures `--db-path` is respected.
- Integrity Check: Simulates and detects database corruption.

#### Sanity Checks (`test_sanity_checks.py`)

Covers the explicit integration tests specified in the requirements text:

- Snapshot and full restore verification
- Byte-level content equality using checksums
- Prune safety across shared data
- Binary file handling
- Path handling for nested and special-character filenames
- Deduplication validation via hash and content count checks

#### Snapshot Tests (`test_snapshot.py`)

Focused tests on snapshot behavior:

- Snapshot creation and structure validation
- Accurate restoration
- Deduplication with identical file content
- Independent management of multiple snapshots

#### Timeline Tests (`test_timeline.py`)

Tests related to chronological behavior and time-based operations.

- **Complex Timeline Simulation**: Creates multiple snapshots with varying file changes to reflect real usage patterns.
- **Temporal Ordering**: Validates that snapshots are timestamped accurately and sorted correctly.
- **Time-Based Operations**: Confirms correct behavior of restore and prune operations using snapshot timestamps.
- **Edge Case Handling**: Tests closely-timed snapshots to ensure stability and correctness.

#### Overall Testing Strategy

- **Comprehensive Coverage**: Includes unit tests and full end-to-end scenarios.
- **Isolation**: All tests run in isolated environments with fresh temp directories and databases.
- **Realistic Scenarios**: Simulates real-world directory structures and usage patterns.
- **Resource Management**: Ensures file and database resources are cleaned up properly.
- **Error Handling**: Covers edge cases and failure modes explicitly.
- **Performance Considerations**: Includes tests for deduplication and handling of larger files.