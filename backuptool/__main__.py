#!/usr/bin/env python3
"""
Command-line interface entry point for the backuptool package.

This module allows the package to be executed as a script using:
python -m backuptool
"""

import sys
from .cli import main


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
