# Backup Tool

A robust Python utility for creating incremental directory snapshots with content-based deduplication, providing efficient backup and restore functionality.

## Features

- **Content-based Deduplication**: Stores each unique file content only once, saving storage space
- **Incremental Snapshots**: Only backs up files that have changed since the last snapshot
- **Integrity Checking**: Built-in verification to detect corrupted file content
- **Efficient Pruning**: Removes snapshots while preserving shared content
- **Detailed Reporting**: Shows size metrics and deduplication statistics

## Installation

### Prerequisites

- Python 3.8 or higher
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

If you want to contribute to the project or run tests, install the development dependencies:

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
pip install -e .
```

### Running the Tool

After installation, you can run the tool using one of these methods:

1. If you installed locally or in a virtual environment:
   ```bash
   python -m backuptool <command> [options]
   ```

2. If you installed globally:
   ```bash
   # Using the entry point script
   backuptool <command> [options]
   ```

## Usage

### Basic Commands

#### Taking a Snapshot

```bash
python -m backuptool snapshot --target-directory=/path/to/backup
```

#### Listing Snapshots

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

```bash
python -m backuptool restore --snapshot-number=2 --output-directory/path/to/restore
```

#### Pruning a Snapshot

```bash
python -m backuptool prune --snapshot 1
```

#### Checking Database Integrity

```bash
python -m backuptool check
```

### Advanced Usage

#### Custom Database Location

By default, the tool stores its database in a file named `backups.db` in the current directory. You can specify a custom location:

```bash
python -m backuptool --db-path /path/to/database.db snapshot --target-directory /path/to/backup
```

## How It Works

### Snapshot Process

1. When a snapshot is created, the tool walks through the target directory
2. For each file:
   - A SHA-256 hash of the file content is calculated
   - If this hash is new, the file content is stored in the database
   - Metadata about the file (path, hash) is recorded in the snapshot
3. This approach ensures that duplicate content across snapshots is stored only once

### Restore Process

1. Recreates the directory structure from the snapshot
2. For each file in the snapshot:
   - Retrieves the content hash
   - Fetches the associated content from the database
   - Writes the content to the appropriate path

### Pruning Process

1. Removes all file references associated with the specified snapshot
2. Deletes any content that is no longer referenced by any snapshot
3. Preserves all content that is shared with other snapshots

## Technical Details

The tool uses SQLite for data storage with three main tables:
- `snapshots`: Records metadata for each snapshot
- `files`: Maps files to their content in each snapshot
- `contents`: Stores the actual file content, referenced by hash

## Performance Considerations

- Files larger than 1GB are skipped by default to prevent memory issues
- Database connections are managed using context managers to ensure proper cleanup
- Transactions are used to ensure database integrity during operations

## Error Handling

The tool implements robust error handling:
- Permission errors are caught and reported clearly
- Missing files or directories trigger appropriate error messages
- Database integrity issues are detected with specific information about corrupted content

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