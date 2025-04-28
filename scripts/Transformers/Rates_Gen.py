from Transformers.Transformer import Transformer
import pandas as pd
import numpy as np
from tqdm import tqdm
import logging

class Rates(Transformer):
    def __init__(self, data, fixingsData, interval_size=5000, timediff='1hour'):
        super().__init__(data, fixingsData)
        self.interval_size = interval_size
        self.timediff = timediff
        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'])
        self.fixings['timestamp'] = pd.to_datetime(self.fixings['timestamp'])
        self.data = self.data.sort_values(by='timestamp')
        self.fixings = self.fixings.sort_values(by='timestamp')

    def run(self):
        self.vectorize()
        return self.data

    def calc_diff(self, chunk):
        df = pd.merge_asof(chunk, self.fixings, on=['timestamp'], by=['ccy_pair'], tolerance=pd.Timedelta(self.timediff), direction='backward')
        df['new_price'] = np.where(df['convert_price'], (df['price'] / df['conversion_factor']) + df['spot_mid_rate'], df['price'])
        return df


    def vectorize(self):
        processed_chunks = []
        n = len(self.data)
        logging.debug(f"Running with batch size {self.interval_size}")

        for start in tqdm(range(0, n, self.interval_size)):
            end = start + self.interval_size
            chunk = self.data.iloc[start:end].copy()

            processed_chunk = self.calc_diff(chunk)
            processed_chunks.append(processed_chunk)

        self.data = pd.concat(processed_chunks, ignore_index=True)
