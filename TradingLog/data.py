from ib_insync import IB, util, Stock
from parser import Parse
import pandas as pd
from datetime import datetime, timedelta
import math
import pandas_datareader as dr

from helpers import hour_rounder

ib  = IB


class DataEngine():
    def __init__(self):
        pass
    
    
    def connect_to_ibkr(self):

            #Start loop is needed in jupyter notebooks
            if get_ipython().__class__.__name__ == 'ZMQInteractiveShell':
                util.startLoop()
            else:
                pass

            #Attempt to connect to IBKR for data through TWS or IB Gateway
            try:
                #TWS Connection Live
                ib.connect('127.0.0.1', 7497, clientId=1)
                #ib.setCallback('error', onError)
                print('Connection Accepted through TWS')
                pass

            except Exception:
                #IB Gateway connection
                ib.connect('127.0.0.1', 4001, clientId=1)
                print('Connection Accepted through Gateway')
                pass


    def disconnect_ibkr(self):
        ib.disconnect()


    def data_from_ibkr(self,optxs,ticker):
        '''
        Returns df, txs, ticker
        '''
        self.connect_to_ibkr()

        txs = optxs[optxs['symbol'] == ticker]
        txs['date'] = pd.to_datetime(txs['date'],format='%Y%m%d')


        #Convert Expiration to DT
        txs['expiration'] = pd.to_datetime(txs['expiration'],yearfirst=True)
        txs['date_time'] = txs['date_time'].apply(lambda x: hour_rounder(pd.to_datetime(x))).astype('datetime64[ns]')
        txs['units'] = txs['units'].astype('float64')

        expir_date = txs['expiration'].max()
        currency = txs['currency'].iloc[0]
        start_date = txs['date'].min()
        days = expir_date - start_date
        today = datetime.today()

        if today > expir_date:
            days = (today-start_date) + timedelta(60)


        ####### Retrieve Data from IBKR ##########   
        contract = Stock(symbol=ticker,exchange='SMART',currency=currency)
        ib.reqContractDetails(contract)

        #Determines correct durationStr for ribeqHistoricalData
        if days.days < 365:
            dur_str = f"{days.days} D"

        elif days.days >= 365:
            dur_str = f"{math.ceil(days.days/365)} Y"

        #Request bars for OHLCV
        bars = ib.reqHistoricalData(contract,durationStr=dur_str,endDateTime=expir_date,
                                        barSizeSetting='1 hour',whatToShow='TRADES',useRTH=False)

        if util.df(bars).empty:
            contract = Stock(symbol=ticker,exchange='NASDAQ',currency=currency)
            bars = ib.reqHistoricalData(contract,durationStr=dur_str,endDateTime=expir_date,
                                        barSizeSetting='1 hour',whatToShow='TRADES',useRTH=False)
            if util.df(bars) == None:
                contract = Stock(symbol=ticker,exchange='NYSE',currency=currency)
                bars = ib.reqHistoricalData(contract,durationStr=dur_str,endDateTime=expir_date,
                                            barSizeSetting='1 hour',whatToShow='TRADES',useRTH=False)
    
        df = util.df(bars)
        df = df.rename(columns={'date':'date_time'})

        txs = pd.merge(txs,df[['date_time','open','close']],on='date_time', how='left').drop_duplicates()
        df = df[df['date_time'] >=txs['date_time'].min()-timedelta(60)]

        self.disconnect_ibkr()
            
        return df,txs, ticker 
    

    def data_from_yahoo(self,optxs,ticker):

        #Filter Optx for specific ticker
        txs = optxs[optxs['symbol'] == ticker]
        txs['date'] = pd.to_datetime(txs['date'],format='%Y%m%d')


        #Convert Expiration to DT
        txs['expiration'] = pd.to_datetime(txs['expiration'],yearfirst=True)
        txs['date_time'] = txs['date_time'].apply(lambda x: hour_rounder(pd.to_datetime(x))).astype('datetime64[ns]')
        txs['units'] = txs['units'].astype('float64')

        expir_date = txs['expiration'].max()
        currency = txs['currency'].iloc[0]
        start_date = txs['date'].min()
        days = expir_date - start_date
        today = datetime.today()

        df = dr.get_data_yahoo(ticker)
        df = df.reset_index()
        df.columns = df.columns.str.lower()
        df = df.rename(columns={'date':'date_time'})

        return df


        txs = pd.merge(txs,df[['date_time','open','close']],on='date_time', how='left').drop_duplicates()
        df = df[df['date_time'] >=txs['date_time'].min()-timedelta(60)]

        self.disconnect_ibkr()
            
        return df,txs, ticker 
