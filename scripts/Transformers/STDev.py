from Transformers.Transformer import Transformer
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import timedelta
import logging

class STDev(Transformer):
    def __init__(self, data, start_date, days_forward=3, window_size=20):
        super().__init__(data)
        self.data['snap_time'] = pd.to_datetime(self.data['snap_time'])
        self.data = self.data.sort_values(by=['security_id', 'snap_time'])
        self.days_forward = days_forward
        self.window_size = window_size
        self.start_date = pd.to_datetime(start_date)
        self.end_date = self.start_date + timedelta(days=self.days_forward)
        filter_start_date = self.start_date - timedelta(days=7)  # 
        self.data = self.data[(self.data['snap_time'] >= filter_start_date) & (self.data['snap_time'] <= self.end_date)]

    def run(self):
        results = []

        for security_id, group in self.data.groupby('security_id'):
            group = group.reset_index(drop=True)
            group['bid_std'] = group['bid'].rolling(window=self.window_size, min_periods=self.window_size).std(ddof=1)
            group['mid_std'] = group['mid'].rolling(window=self.window_size, min_periods=self.window_size).std(ddof=1)
            group['ask_std'] = group['ask'].rolling(window=self.window_size, min_periods=self.window_size).std(ddof=1)
            group = group[(group['snap_time'] >= self.start_date) & (group['snap_time'] <= self.end_date)]
            results.append(group[['snap_time', 'security_id', 'bid_std', 'mid_std', 'ask_std']])

        self.data = pd.concat(results, ignore_index=True)
        return self.data
