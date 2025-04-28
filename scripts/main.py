from Extractors.FXExtractor import FXExtractor
from Extractors.EquityExtractor import EquityExtractor
from Extractors.Fixings import Fixing
from Transformers.Rates_Gen import Rates
# from Transformer.STDev import ST
from Uploaders.Uploaders import Uploader
from pathlib import Path

def main(jobs):
    for job in jobs:
        extractor = job['Extractor']
        transformer = job['Transformer'](extractor.data, extractor)
        data = transformer.run()
        
        # job['Uploader'](data[~(data['new_price'].isnull(Path()))]).upload_csv()
        # job['Uploader'](data[(data['new_price'].isnull())]).upload_csv()
        print(transformer.data)



if __name__ == '__main__':
    jobs = [
        {
            'Extractor': FXExtractor('rates_price_data.parq', Fixing('rates_ccy_data.csv', 'rates_spot_rate_data.parq')), 
            'Transformer': Rates,
            'Uploader': Uploader,
        },
    ]
    main(jobs)