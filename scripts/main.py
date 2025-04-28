from Extractors.FXExtractor import FXExtractor
from Extractors.EquityExtractor import EquityExtractor
from Extractors.Fixings import Fixing
from Transformers.Rates_Gen import Rates
# from Transformer.STDev import ST
from Uploaders.Uploaders import Uploader
from pathlib import Path
import os
import logging
import time

def main(jobs):
    for job in jobs:
        start_time = time.time()
        logging.debug(f'Running name job {job["Name"]} {start_time}')
        extractor = job['Extractor']
        if hasattr(extractor, 'fixing'):
            transformer = job['Transformer'](extractor.data, extractor.fixing)
        else:
            transformer = job['Transformer'](extractor.data)
        data = transformer.run()

        if job['Name'] == 'FX_New':
            job['Uploader'](data[(data['new_price'].isnull())]).upload_csv(Path(os.getcwd()) / 'rates_test' / 'results' / 'fulldata.csv' )
            job['Uploader'](data[~(data['new_price'].isnull())]).upload_csv(Path(os.getcwd()) / 'rates_test' / 'results' / 'missing_rates.csv' )

        end_time = time.time()
        logging.debug(f'Succesfully ran the job {job['Name']} {end_time}')
        logging.debug(f'Ran at total {end_time - start_time}')



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s')
    jobs = [
        {
            'Name': 'FX_New',
            'Extractor': FXExtractor('rates_price_data.parq', Fixing('rates_ccy_data.csv', 'rates_spot_rate_data.parq')), 
            'Transformer': Rates,
            'Uploader': Uploader,
        },
    ]
    main(jobs)