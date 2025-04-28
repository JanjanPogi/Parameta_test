import pandas as pd
import duckdb
import os
from pathlib import Path


class MarketExtractor:
    def __init__(self):
        self.data = None
        self.path = Path(os.getcwd())

    # pandas read
    # sql/duckdb read
    def extract(self, file, type='pandas'):
        if type == 'pandas':
            if file.suffix == '.csv':
                df = pd.read_csv(file)
            elif file.suffix == '.parq':
                df = pd.read_parquet(file)
        elif type == 'duckdb':
            if file.suffix == '.csv':
                df = duckdb.read_csv(file)
            elif file.suffix == '.parq':
                df = duckdb.read_parquet(file)
        return df

