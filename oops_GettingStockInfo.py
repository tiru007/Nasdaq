from datetime import date
import quandl
import numpy as np
import pandas as pd
from local_settings import settings as settings
import sqlalchemy
import pymysql
import yfinance

pymysql.install_as_MySQLdb()

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

    def get_bars(self, market: str = 'stock', ticker: str = None,
                 from_: date = None, to: date = None) -> pd.DataFrame:

        # Convert np.NaT to None
        from_ = None if pd.isnull(from_) else from_
        to = None if pd.isnull(to) else to

        # Set date to most recent year if None
        to = to if to else date.today()
        from_ = from_ if from_ else date(2000, 1, 1)

        tables = ['SHARADAR/SEP', 'SHARADAR/SFP']

        for table in tables:
            df = self._session.get_table(table,
                                         ticker=ticker,
                                         date={'gte': from_, 'lte': to},
                                         paginate=True)

            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values(by='date')
                df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
                return df

        return None