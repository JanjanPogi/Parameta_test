from .MarketExtractor import MarketExtractor 

class EquityExtractor(MarketExtractor):
    def __init__(self, price_file):
        super().__init__()
        self.path = self.path / 'stdev_test' / 'data'
        self.data = self.extract(self.path / price_file)
