import argparse

def main():

    parser = argparse.ArgumentParser(description="File backup tool")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    args = parser.parse_args()
    
    if args.command == "list":
        print("Listing snapshots...")
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
