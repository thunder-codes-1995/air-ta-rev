import importlib
import os
import re
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Migrate:
    def __init__(self, module_name=os.getenv("MIGRATIONS_DIR"), force=False):
        self.module_name = module_name
        self.force = force

    def run(self):
        for file in self.get_ordered_files():
            if self.should_skip(file):
                continue
            self.exec(file)

    def should_skip(self, file):
        return not bool(re.match(r"\d{8}_\d{6}_.+\.py", file.name))

    def get_ordered_files(self):
        files = Path(self.module_name).rglob("*.py")
        ordered = []

        for file in files:
            if self.should_skip(file):
                continue
            _split = file.name.split("_")
            datetime = int(f"{_split[0]}{_split[1]}")
            ordered.append((file, datetime))

        ordered = sorted(ordered, key=lambda item: item[1])
        ordered = [item[0] for item in ordered]
        return ordered

    def check_if_valid(self, module, filename):
        """check if files has all required variables : IGNORE,NAME and main function"""
        if not hasattr(module, "IGNORE"):
            raise ValueError(
                f"Invalid file '{filename}', `IGNORE`  variable is missing"
            )

        if not hasattr(module, "NAME"):
            raise ValueError(f"Invalid file '{filename}', `NAME`  variable is missing")

        if not hasattr(module, "main"):
            raise ValueError(f"Invalid file '{filename}', `main`  function is missing")

    def exec(self, file):
        filename = file.stem
        module = importlib.import_module(f"{self.module_name}.{filename}")
        self.check_if_valid(module, filename)
        should_ignore = getattr(module, "IGNORE")
        # NOTE get executed migrations from db
        executed = []

        if "main" not in dir(module):
            return

        if self.force or (filename not in executed and not should_ignore):
            func = getattr(module, "main")
            func()
            self.store(filename)

    def store(self, name):
        """store migration name to db"""
        pass
