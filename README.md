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

### Install from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/backuptool.git
cd backuptool

# Install in development mode
pip install -e .
```

## Usage

### Basic Commands

#### Taking a Snapshot

```bash
python -m backuptool snapshot --target-directory /path/to/backup
```

#### Listing Snapshots

```bash
python -m backuptool list
```

Example output:
```
SNAPSHOT ID  TIMESTAMP                 SIZE       DISTINCT SIZE  
------------------------------------------------------------
1            2025-03-30 15:30:45       156        156            
2            2025-03-31 09:45:12       172        30             
------------------------------------------------------------
TOTAL                                  186        
```

#### Restoring a Snapshot

```bash
python -m backuptool restore --snapshot-number 2 --output-directory /path/to/restore
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
# Install development dependencies
pip install pytest pytest-cov

# Run tests
python -m pytest

# Run tests with coverage
python -m pytest --cov=backuptool
```

## License

[MIT License](LICENSE)
