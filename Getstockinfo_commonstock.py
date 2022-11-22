from datetime import date
import quandl
import numpy as np
import pandas as pd
from local_settings import settings as settings
import sqlalchemy
import pymysql
import yfinance as yf

engine = sqlalchemy.create_engine('mysql://root:Pass1234@localhost:3306/')

query = "select * from tickers.tickers where category like 'Domestic Common Stock'"
nasdaq = pd.read_sql(query, engine)
# nasdaq = pd.read_sql('select * from tickers.tickers where category like `'Domestic Preferred Stock'`')

nasdaq_dcs = nasdaq['ticker'].to_list()

indicies = ['nasdaq_dcs']

mapper = {'nasdaq_dcs': nasdaq_dcs}

for index in indicies:
    engine = sqlalchemy.create_engine('mysql://root:Pass1234@localhost:3306/'+index)
    for symbol in mapper[index]:
        try:
            print("this is valid")
            df = yf.download(symbol, start='2020-01-01')
            df = df.reset_index()
            #dbname = symbol.rsplit(".")[0].lower()
            df.to_sql(symbol, engine)
        except AttributeError:
            print
            "This is not a valid number"