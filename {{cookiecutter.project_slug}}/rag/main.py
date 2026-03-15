from ingest import ingest
from query import query
import sys

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)

    command = sys.argv[1]

    if command == "ingest":
        ingest()
    elif command == "query" and len(sys.argv) == 3:
        query(sys.argv[2])
