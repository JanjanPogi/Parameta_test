from Extractors.FXExtractor import FXExtractor
from Extractors.EquityExtractor import EquityExtractor
from Extractors.Fixings import Fixing
from Transformers.Rates_Gen import Rates
from Transformers.STDev import STDev
from Uploaders.Uploaders import Uploader
from pathlib import Path
import os
import logging
import time

def main(jobs):
    for job in jobs:
        start_time = time.time()
        logging.debug('========================================================')
        logging.debug(f'Running name job {job["Name"]} {start_time}')
        extractor = job['Extractor']
        if hasattr(extractor, 'fixing'):
            transformer = job['Transformer'](extractor.data, extractor.fixing, **job.get('Params', {}))
        else:
            transformer = job['Transformer'](extractor.data, **job.get('Params', {}))
        data = transformer.run()

        if not os.path.exists(job['Path']):
            os.makedirs(job['Path'])

        if job['Name'] == 'FX_New':
            job['Uploader'](data[~(data['new_price'].isnull())]).upload_csv(job['Path'] / 'fulldata.csv' )
            job['Uploader'](data[(data['new_price'].isnull())]).upload_csv(job['Path'] / 'missing_rates.csv' )
        else:
            job['Uploader'](data).upload_csv(job['Path'] / 'stdev_test.csv')

        end_time = time.time()
        logging.debug(f'Succesfully ran the job {job['Name']} {end_time}')
        logging.debug(f'Job {job["Name"]} ran at total {end_time - start_time}')
        logging.debug('========================================================\n')



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s')
    jobs = [
        {
            'Name': 'FX_New',
            'Extractor': FXExtractor('rates_price_data.parq', Fixing('rates_ccy_data.csv', 'rates_spot_rate_data.parq')), 
            'Transformer': Rates,
            'Uploader': Uploader,
            'Path': Path(os.getcwd()) / 'rates_test' / 'results',
            'Params': {
                'interval_size': 5000,
                'timediff': '1hour'
            }
        },
        {
            'Name': 'STDev',
            'Extractor': EquityExtractor('stdev_price_data.parq'),
            'Transformer': STDev,
            'Uploader': Uploader,
            'Path': Path(os.getcwd()) / 'stdev_test' / 'results',
            'Params': {
                'start_date': "2021-11-20 00:00:00",
                'hours_forward': 81,
                'window_size': 20,
            }
        }
    ]
    main(jobs)