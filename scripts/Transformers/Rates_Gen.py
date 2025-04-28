from Transformers.Transformer import Transformer
import pandas as pd
import numpy as np

class Rates(Transformer):
    def __init__(self, data, fixingsData):
        super().__init__(data, fixingsData)
        self.data['timestamp'] = pd.to_datetime(self.data['timestamp'])
        self.fixings['timestamp'] = pd.to_datetime(self.fixings['timestamp'])
        self.data = self.data.sort_values(by='timestamp')
        self.fixings = self.fixings.sort_values(by='timestamp')

    def run(self):
        self.vectorize()
        return self.data

    def calc_diff(self, chunk, timediff='1hour'):
        df = pd.merge_asof(chunk, self.fixings, on=['timestamp'], by=['ccy_pair'], tolerance=pd.Timedelta(timediff), direction='backward')
        df['new_price'] = np.where(df['convert_price'], (df['price'] / df['conversion_factor']) + df['spot_mid_rate'], df['price'])
        return df


    def vectorize(self, batch_size=5000):
        processed_chunks = []
        n = len(self.data)

        for start in range(0, n, batch_size):
            end = start + batch_size
            chunk = self.data.iloc[start:end].copy()

            processed_chunk = self.calc_diff(chunk)
            processed_chunks.append(processed_chunk)

        self.data = pd.concat(processed_chunks, ignore_index=True)
