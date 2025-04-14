import argparse
import sys

from dotenv import load_dotenv

from scripts.create_migration import CreateMigration
from scripts.migrate import Migrate

load_dotenv()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", help="action to be taken", type=str)
    parser.add_argument(
        "--name", help="name of migration (create_migration commend)", type=str
    )

    parser.add_argument(
        "--force", help="force all migrations if set to true", type=bool
    )

    args = parser.parse_args(sys.argv[1:])

    if args.action == "migrate":
        Migrate(force=args.force).run()
    elif args.action == "create_migration":
        assert bool(args.name), "migration name is mandatory"
        CreateMigration(args.name).run()


if __name__ == "__main__":
    main()
