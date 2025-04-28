from .MarketExtractor import MarketExtractor 
import pandas as pd
import numpy as np


class FXExtractor(MarketExtractor):
    def __init__(self, price_file, fixingsData):
        super().__init__()
        self.path = self.path / 'rates_test' / 'data'
        self.data = self.extract(self.path / price_file)
        self.fixing = fixingsData
    