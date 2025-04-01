"""
Backuptool - A robust file backup and restore utility.

This package provides tools for creating incremental directory snapshots 
with content-based deduplication, providing efficient backup and restore 
functionality.
"""

__version__ = "0.1.0"
__author__ = "ijd"

# Export public API
from .operations import BackupOperations
from .database import BackupDatabase

__all__ = ["BackupOperations", "BackupDatabase"]