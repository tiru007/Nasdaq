import sqlalchemy
import pymysql
import ta
import pandas as pd
import numpy as np
import yfinance

pymysql.install_as_MySQLdb()
engine = sqlalchemy.create_engine('mysql://root:Pass1234@localhost:3306/')


class Recommender:
    engine = sqlalchemy.create_engine('mysql://root:Pass1234@localhost:3306/')

    def __init__(self, index):
        self.index = index

    def gettables(self):
        query = f"""SELECT table_name From information_schema.tables
            WHERE table_schema = '{self.index}'"""
        df = pd.read_sql(query, self.engine)
        df['Schema'] = self.index
        return df

    def getprices(self):
        prices = []
        for table, schema in zip(self.gettables().TABLE_NAME, self.gettables().Schema):
            sql = schema + '.' + f'`{table}`'
            prices.append(pd.read_sql(f"SELECT Date, Close FROM {sql}", self.engine))
        return prices

    def maxdate(self):
        req = self.index+'.'+f'`{self.gettables().TABLE_NAME[0]}`'
        return pd.read_sql(f"SELECT MAX(Date) FROM {req}", self.engine)

    def updateDB(self):
        maxdate = self.maxdate()['MAX(Date)'][0]
        engine = sqlalchemy.create_engine('mysql://root:Pass1234@localhost:3306/'+self.index)
        for symbol in self.gettables().TABLE_NAME:
            data = yfinance.download(symbol, start=maxdate)
            data = data[data.index > maxdate]
            data = data.reset_index()
            data.to_sql(symbol, engine, if_exists='append')
        print(f'{self.index} successfully updated')

    def MACDdesicion(self, df):
        df['MACD_diff'] = ta.trend.macd_diff(df.Close)
        df['Decision MACD'] = np.where((df.MACD_diff) > 0 & (df.MACD_diff.shift(1) < 0), True, False)

    def Goldencrossdecision(self, df):
        df['SMA50'] = ta.trend.sma_indicator(df.Close, window=50)
        df['SMA200'] = ta.trend.sma_indicator(df.Close, window=200)
        df['Signal'] = np.where(df['SMA50'] > df['SMA200'], True, False)
        df['Decision GC'] = df.Signal.diff()

    def Deathcrossdecision(self, df):
        df['SMA50'] = ta.trend.sma_indicator(df.Close, window=50)
        df['SMA200'] = ta.trend.sma_indicator(df.Close, window=200)
        df['Signal'] = np.where(df['SMA200'] > df['SMA50'], True, False)
        df['Decision DC'] = df.Signal.diff()

    def RSI_SMAdecision(self, df):
        df['RSI'] = ta.momentum.rsi(df.Close, window=10)
        df['SMA200'] = ta.trend.sma_indicator(df.Close, window=200)
        df['Decision RSI/SMA'] = np.where((df.Close > df.SMA200) & (df.RSI < 30), True, False)

    def applytechnicals(self):
        prices = self.getprices()
        for frame in prices:
            self.MACDdesicion(frame)
            self.Goldencrossdecision(frame)
            self.RSI_SMAdecision(frame)
            self.Deathcrossdecision(frame)
        return prices

    def recommend(self):
        indicators = ['Decision MACD', 'Decision GC', 'Decision RSI/SMA']
        for symbol, frame in zip(self.gettables().TABLE_NAME, self.applytechnicals()):
            if frame.empty is False:
                for indicator in indicators:
                    if frame[indicator].iloc[-1] == True:
                        print(f"{indicator} Buying Signal for " + symbol)

niftyinstance = Recommender('nasdaq_dcs')

niftyinstance.recommend()

niftyinstance.updateDB()

niftyinstance.gettables()