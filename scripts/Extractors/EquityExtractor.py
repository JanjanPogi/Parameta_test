from .MarketExtractor import MarketExtractor 
import os

class EquityExtractor(MarketExtractor):
    def __init__(self, price_file):
        super().__init__()
        self.path = self.path / 'stdev_test' / 'data'
        self.data = self.extract(self.path / price_file)

    def run(self):
        return self.data
