from datetime import date
import quandl
import numpy as np
import pandas as pd
from local_settings import settings as settings

class MyRESTClient():
    def __int__(self, auth_key: '4Q3AdcqfszJbQLDSPnq7'):
        quandl.ApiConfig.api_key = auth_key

        self._session = quandl

    def get_tickers(self) -> pd.DataFrame:
        tickers = self._session.get_table('SHARADAR/TICKERS', paginate=True)
        tickers = tickers[(tickers['table'] == 'SEP') | (tickers['table'] == 'SFP')]
        tickers.replace({np.nan: None}, inplace=True)

        tickers['active'] = tickers['isdelisted'].apply(lambda x: bool(x == 'N'))

        tickers = tickers.rename(columns={'permaticker':'quandl_id','siccode':'sic'})

        tickers['quandl_id'] = tickers['quandl_id'].astype(int)

        cols = ['ticker', 'name', 'active', 'sic', 'sector', 'industry', 'quandl_id', 'category']
        tickers = tickers[cols]

        tickers = tickers.drop_duplicates(subset='ticker')
        return tickers

    client = MyRESTClient()
    client

    df = client.get_tickers()
    df

    df.to_sql