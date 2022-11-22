from datetime import date
import quandl
import numpy as np
import pandas as pd
from local_settings import settings as settings
import sqlalchemy
import pymysql

pymysql.install_as_MySQLdb()

import quandl


class MyRESTClient:
    def __init__(self, auth_key: str = None):
        if not ('api_key' in settings.keys() and
                'number_of_retries' in settings.keys() and
                'max_wait_between_retries' in settings.keys() and
                'retry_backoff_factor' in settings.keys() and
                'retry_status_codes' in settings.keys()):
            raise Exception('Bad quandl config file.')

        # https://github.com/quandl/quandl-python#configuration
        quandl.ApiConfig.api_key = settings['api_key']
        quandl.ApiConfig.NUMBER_OF_RETRIES = settings['number_of_retries']
        quandl.ApiConfig.MAX_WAIT_BETWEEN_RETRIES = settings['max_wait_between_retries']
        quandl.ApiConfig.RETRY_BACKOFF_FACTOR = settings['retry_backoff_factor']
        quandl.ApiConfig.RETRY_STATUS_CODES = settings['retry_status_codes']

        self._session = quandl

    def get_tickers(self) -> pd.DataFrame:
        tickers = self._session.get_table('SHARADAR/TICKERS', paginate=True)

        # Filter tickers for equities and funds. Remove instituational tables.
        tickers = tickers[(tickers['table'] == 'SEP') | \
                          (tickers['table'] == 'SFP')]

        # Set NaNs to None and strings to boolean
        tickers.replace({np.nan: None}, inplace=True)

        # Convert isdelated to active
        tickers['active'] = tickers['isdelisted'].apply(lambda x: bool(x == 'N'))

        # Rename and get only fields of interest
        tickers = tickers.rename(columns={'permaticker': 'quandl_id',
                                          'siccode': 'sic'})

        # Set type of quandl_id to int64
        tickers['quandl_id'] = tickers['quandl_id'].astype(int)

        # Return only columns we want
        cols = ['ticker', 'name', 'active', 'sic',
                'sector', 'industry', 'quandl_id', 'category']
        tickers = tickers[cols]

        # Prevents duplicate data sent by API provider
        tickers = tickers.drop_duplicates(subset='ticker')
        return tickers

engine = sqlalchemy.create_engine('mysql://root:Pass1234@localhost:3306/tickers')
df.reset_index()
df.to_sql('tickers', engine, if_exists='append')


