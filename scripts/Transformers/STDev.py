from Transformers.Transformer import Transformer
import pandas as pd
import numpy as np
from datetime import timedelta
from tqdm import tqdm

class STDev(Transformer):
    def __init__(self, data, start_date, hours_forward=45, window_size=20):
        super().__init__(data)
        self.data['snap_time'] = pd.to_datetime(self.data['snap_time'])
        self.data = self.data.sort_values(by=['security_id', 'snap_time'])
        self.window_size = window_size
        self.start_date = pd.to_datetime(start_date)
        self.end_date = self.start_date + timedelta(hours=hours_forward)
        self.lookback_start = self.start_date - timedelta(hours=20)
        self.data = self.data[(self.data['snap_time'] >= self.lookback_start) & (self.data['snap_time'] <= self.end_date)]

    def fill_missing_snap_times(self):
        filled_data = []

        # Precompute time index once
        hourly_index = pd.date_range(start=self.lookback_start, end=self.end_date, freq='h')

        for security_id, group in tqdm(self.data.groupby('security_id')):
            group = group.set_index('snap_time').sort_index()
            group = group.reindex(hourly_index)
            group['security_id'] = security_id
            group[['bid', 'mid', 'ask']] = group[['bid', 'mid', 'ask']].ffill()
            filled_data.append(group)

        return pd.concat(filled_data).reset_index().rename(columns={'index': 'snap_time'})

    def run(self):
        filled = self.fill_missing_snap_times()
        filled = filled.sort_values(by=['security_id', 'snap_time'])

        filled['bid_std'] = filled.groupby('security_id')['bid'].transform(lambda x: x.rolling(self.window_size, min_periods=self.window_size).std())
        filled['mid_std'] = filled.groupby('security_id')['mid'].transform(lambda x: x.rolling(self.window_size, min_periods=self.window_size).std())
        filled['ask_std'] = filled.groupby('security_id')['ask'].transform(lambda x: x.rolling(self.window_size, min_periods=self.window_size).std())

        # Only return results in the desired window
        result = filled[(filled['snap_time'] >= self.start_date) & (filled['snap_time'] <= self.end_date)]
        return result[['snap_time', 'security_id', 'bid_std', 'mid_std', 'ask_std']]
