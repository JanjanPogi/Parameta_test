from .MarketExtractor import MarketExtractor 
import pandas as pd
import numpy as np


class Fixing(MarketExtractor):
    def __init__(self, reference_file, spot_file):
        super().__init__()
        self.path = self.path / 'rates_test' / 'data'
        self.fixings = self.read_reference_file(reference_file=reference_file, spot_file=spot_file)

    def read_reference_file(self, reference_file, spot_file):
        spots = self.extract(self.path / spot_file)
        reference = self.extract(self.path / reference_file)
        # assume missing ccy_pair in the spots and not in the reference file does not need conversion
        missing_rates = set(spots['ccy_pair'].unique()) - set(reference['ccy_pair'].unique())
        reference = pd.concat([reference, pd.DataFrame(map(lambda x: [x, False, np.nan], missing_rates), columns=reference.columns)])
        return spots.merge(reference, on=['ccy_pair'], how='inner')
    