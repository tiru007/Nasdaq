import ta
import numpy as np
import yfinance as yf

def gettables(symbol):
    df = yf.download(symbol, start='2020-01-01')
    return df

def MACDdesicion(df):
    df['MACD_diff'] = ta.trend.macd_diff(df.Close)
    df['Decision MACD'] = np.where((df.MACD_diff) > 0 & (df.MACD_diff.shift(1) < 0), True, False)
    if df['Decision MACD'].iloc[-1] == True:
      print(f"MACDdesicion Buying Signal for " + symbol)


def Goldencrossdecision(df):
    df['SMA50'] = ta.trend.sma_indicator(df.Close, window=50)
    df['SMA200'] = ta.trend.sma_indicator(df.Close, window=200)
    df['GCSignal'] = np.where(df['SMA50'] > df['SMA200'], True, False)
    df['Decision GC'] = df.GCSignal.diff()
    if df['Decision GC'].iloc[-1] == True:
       print(f"Goldencrossdecision Buying Signal for " + symbol)

def Deathcrossdecision(df):
    df['SMA50'] = ta.trend.sma_indicator(df.Close, window=50)
    df['SMA200'] = ta.trend.sma_indicator(df.Close, window=200)
    df['DCSignal'] = np.where(df['SMA200'] > df['SMA50'], True, False)
    df['Decision DC'] = df.DCSignal.diff()
    if df['DCSignal'].iloc[-1] == True:
       print(f"Deatchcrossdecision Buying Signal for " + symbol)


def RSI_SMAdecision(df):
    df['RSI'] = ta.momentum.rsi(df.Close, window=10)
    df['SMA200'] = ta.trend.sma_indicator(df.Close, window=200)
    df['Decision RSI/SMA'] = np.where((df.Close > df.SMA200) & (df.RSI < 30), True, False)
    if df['Decision RSI/SMA'].iloc[-1] == True:
       print(f"RSI_SMA decision Buying Signal for " + symbol)

symbol = "APN.U"
df = gettables(symbol)

MACDdesicion(df)
Goldencrossdecision(df)
RSI_SMAdecision(df)
Deathcrossdecision(df)