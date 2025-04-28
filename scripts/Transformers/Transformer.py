import pandas as pd
import os
from pathlib import Path


class Transformer:
    def __init__(self, marketdata, fixingsdata=''):
        self.path = Path(os.getcwd())
        self.data = marketdata
        try:
            if not (fixingsdata.fixing.fixings.empty):
                self.fixings = fixingsdata.fixing.fixings
        except (AttributeError, ValueError):
            self.fixings = pd.DataFrame()

    def run(self):
        pass
