import os
from datetime import datetime
from pathlib import Path


class CreateMigration:
    def __init__(self, name: str):
        self.name = name
        self.migration_dir = os.getenv("MIGRATIONS_DIR")

    def run(self):
        now = datetime.today().strftime("%Y%m%d_%H%M%S")
        filename = f"{now}_{self.name}.py"
        self.check_dir()

        with open(f"{self.migration_dir}/{filename}", "w") as f:
            f.write(f"NAME='{self.name}'\n\n")
            f.write("IGNORE = False\n\n")
            f.write("def main():\n")
            f.write("    # write logic here\n\tpass")

    def check_dir(self):
        if not Path(self.migration_dir).exists():
            Path(self.migration_dir).mkdir()
