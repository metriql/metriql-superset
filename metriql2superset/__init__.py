import argparse
import json
from .metadata import MetriqlMetadata
from .superset import DatabaseOperation
import sys

__version__ = "0.6"


def main(args: list = None):
    parser = argparse.ArgumentParser(description="Generates Tableau TDS files for metriql datasets")

    parser.add_argument("command", choices=["create-database", "list-databases", "sync-database"],
                        help="command to execute")

    parser.add_argument("--metriql-url", help="metriql URL")
    parser.add_argument("--file", help="Read dataset from file instead of stdin")

    parser.add_argument("--database-id", help="Superset database id that will be used to syncronize the datasets")
    parser.add_argument("--database-name", help="Superset database name that will be used when creating databases")

    parser.add_argument("--superset-url", help="Superset URL that you want to analyze metriql data")

    parser.add_argument("--superset-username", help="Superset username for generating API token")
    parser.add_argument("--superset-password", help="Superset password for generating API token")

    parsed = parser.parse_args(args=args)
    operation = DatabaseOperation(parsed.superset_url, parsed.superset_username, parsed.superset_password)
    if parsed.command == "version":
        print(__version__)
    elif parsed.command == "create-database":
        operation.create(parsed.metriql_url, parsed.database_name)
        print('Successfully created!')
    elif parsed.command == "list-databases":
        databases = operation.list()
        print(json.dumps(databases))
    elif parsed.command == "sync-database":
        if parsed.file is not None:
            source = open(parsed.file).read()
        else:
            source = sys.stdin.readline()
        metriql_metadata = MetriqlMetadata(parsed.metriql_url, json.loads(source))
        if parsed.database_id is None:
            raise Exception("--database-id argument is required")
        operation.sync(int(parsed.database_id), metriql_metadata)
