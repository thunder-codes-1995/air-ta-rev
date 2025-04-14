import os
from dataclasses import dataclass

import pandas as pd
from core.logger import Logger
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Check:
    data: pd.DataFrame
    log_path: str

    def __post_init__(self):
        self.logger = Logger(f"{os.getenv('LOG_FOLDER')}/{self.log_path}")

    def is_empty(self, filename: str, msg=""):
        if self.data.empty:
            self.logger.error(f"file {filename} is empty{msg}")
            raise ValueError(f"file {filename} is empty{msg}")
