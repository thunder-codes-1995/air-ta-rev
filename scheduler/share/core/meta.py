import os
from typing import List, Optional

import pandas as pd
from share.core.logger import Logger

logger = Logger(f"{os.getenv('LOG_FOLDER')}/logs.log")


class Meta:
    @classmethod
    def columns(cls): ...

    @classmethod
    def name(cls): ...


def read_meta(
    meta_class: Meta,
    date: int,
    path: str,
    override_header: bool = False,
    index_col: Optional[List[str]] = None,
    cols_range=None,
    sep=",",
    encoding="utf-8",
) -> pd.DataFrame:
    filename: str = f"{path}/{meta_class.name()}.{date}.csv"
    args = {}

    if override_header:
        args["names"] = meta_class.columns()
    if index_col is not None:
        args["index_col"] = index_col
    if cols_range:
        args["usecols"] = cols_range

    try:
        return pd.read_csv(
            filename,
            header=None if override_header else 0,
            skiprows=1 if override_header else 0,
            **args,
            delimiter=sep,
            encoding=encoding,
        )
    except FileNotFoundError:
        logger.error(f"file {path}/{meta_class.name()}.{date}.csv is not found")
