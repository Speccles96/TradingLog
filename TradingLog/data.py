from ib_insync import IB, util, Stock
import pandas as pd
from datetime import datetime, timedelta
import math
import pandas_datareader as dr
from tqdm import tqdm
import yahooquery as yq 

ib  = IB()


class DataEngine():
    def __init__(self):
        self.optxs_history = dict()
        self.stktxs_history = dict()

        pass
    
    
    def connect_to_ibkr(self):
            '''
            Helper function to connect to ibkr with ib_insync
            '''

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
            
            except ConnectionRefusedError:
                self.disconnect_ibkr()

            except IndexError:
                print("Ticker not in transactions")




    def disconnect_ibkr(self):
        '''
        Disconnects from IBKR need to call to avoid reconnect issues
        '''
        ib.disconnect()




    def build_optxs_history(self,txs,engine='ibkr'):

        '''
        This function builds the option transaction history from your parsed transaction DataFrame
        Appends the each ticker with OHLCV data from IBKR

        Parameters
        ----------
        txs: DataFrame
            Transaction Data frame from Parser to iterate through tickers and 
    
            
        Returns
        -------
        No returns
        Appends to optxs_history with all tickers, ohlc and transactions for processing later
        '''
        if engine.lower() =='ibkr':
            self.optxs_history_from_ibkr(txs)
        elif engine.lower() == 'yahoo':
            pass
            




    def security_from_ibkr(self,optxs,ticker):
        '''
        Returns df, txs, ticker
        '''
        self.connect_to_ibkr()

        txs = optxs[optxs['symbol'] == ticker]
        txs['date'] = pd.to_datetime(txs['date'],format='%Y%m%d')


        #Convert Expiration to DT
        txs['expiration'] = pd.to_datetime(txs['expiration'],yearfirst=True)
        txs['date_time'] = pd.to_datetime(txs['date_time'])
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
    
        ohlc = util.df(bars)
        ohlc = ohlc.rename(columns={'date':'date_time'})
        ohlc['date_time'] = pd.to_datetime(ohlc['date_time'])

        txs = pd.merge(txs,ohlc[['date_time','open','close']],on='date_time', how='left').drop_duplicates()
        ohlc = ohlc[ohlc['date_time'] >=txs['date_time'].min()-timedelta(60)]

        self.disconnect_ibkr()
            
        return ohlc,txs, ticker 
    
    def security_from_yahoo(self,optxs,ticker):

        #Filter Optx for specific ticker
        txs = optxs[optxs['symbol'] == ticker]
        txs['date'] = pd.to_datetime(txs['date'],format='%Y%m%d')


        #Convert Expiration to DT
        txs['expiration'] = pd.to_datetime(txs['expiration'],yearfirst=True)
        #txs['date_time'] = txs['date_time'].apply(lambda x: hour_rounder(pd.to_datetime(x))).astype('datetime64[ns]')
        txs['units'] = txs['units'].astype('float64')

        expir_date = txs['expiration'].max()
        currency = txs['currency'].iloc[0]
        start_date = txs['date'].min()
        days = expir_date - start_date
        today = datetime.today()

        df = yq.Ticker("AAPL",asynchronous=True).history(start='2020-01-01', interval='1h')
        df = df.reset_index()
        df.columns = df.columns.str.lower()
        df = df.rename(columns={'date':'date_time'})

        return df


##############################################################
              ##### Multiple securities######
##############################################################
    
    def multiple_securities_from_ibkr(self,optxs,ticker,connect_status=None):
        '''
        Returns df, txs, ticker
        '''

        #Starts IBKR connect if not connected
        if connect_status == 0:
            self.connect_to_ibkr()
        else:
            pass


        #Filters for transactions related top ticker
        txs = optxs[optxs['symbol'] == ticker]
        txs['date'] = pd.to_datetime(txs['date'],format='%Y%m%d')


        #Convert Expiration to DT
        txs['expiration'] = pd.to_datetime(txs['expiration'],yearfirst=True)
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

        #Determines correct durationStr for reqHistoricalData
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
    
        ohlc = util.df(bars)
        ohlc = ohlc.rename(columns={'date':'date_time'})

        txs = pd.merge(txs,ohlc[['date_time','open','close']],on='date_time', how='left').drop_duplicates()
        ohlc = ohlc[ohlc['date_time'] >=txs['date_time'].min()-timedelta(60)]
        

        if connect_status == -1:
            self.disconnect_ibkr()
        else:
            pass

            
        return ohlc,txs, ticker 




    def multiple_securities_from_yahoo(self,txs):

        #txs['date'] = pd.to_datetime(txs['date'],format='%Y%m%d')


        #Convert Expiration to DT
        #txs['expiration'] = pd.to_datetime(txs['expiration'],yearfirst=True)
        txs['trade_time'] = txs['date_time']
        txs['date_time'] = pd.to_datetime(txs['date_time'])
        txs['units'] = txs['units'].astype('float64')

        try:
            expir_date = txs['expiration'].max()
            currency = txs['currency'].iloc[0]
            start_date = txs['date'].min()
            days = expir_date - start_date
            today = datetime.today()

            if today > expir_date:
                days = (today-start_date) + timedelta(60)
        except:
            pass

        self.ohlc = yq.Ticker(txs['symbol'].unique() ,asynchronous=True).history(start='2020-01-01', interval='60m').reset_index()
        self.ohlc = self.ohlc.rename(columns={'date':'date_time'})

        
        



    def optxs_history_from_ibkr(self,txs):

        for iter, symbol in tqdm(enumerate(txs['symbol'].unique())):
            try:   
                #Starts IBKR connection if no connection available
                if ib.isConnected() == False:

                    symbol_ohlc, symbol_txs, ticker = self.multiple_securities_from_ibkr(txs,symbol,0)

                    self.optxs_history[symbol] = {}
                    self.optxs_history[symbol]['ohlc'] = symbol_ohlc
                    self.optxs_history[symbol]['txs'] = symbol_txs

                #Disconnects from IBKR on last symbol in txs
                elif iter+1 == len(txs['symbol'].unique()):

                    symbol_ohlc, symbol_txs, ticker = self.multiple_securities_from_ibkr(txs,symbol,-1)

                    self.optxs_history[symbol] = {}
                    self.optxs_history[symbol]['ohlc'] = symbol_ohlc
                    self.optxs_history[symbol]['txs'] = symbol_txs

                #Appends to master option transactions if connection is available and doesn't need to be ended
                elif ib.isConnected() == True:
                    
                    symbol_ohlc, symbol_txs, ticker = self.multiple_securities_from_ibkr(txs,symbol)

                    self.optxs_history[symbol] = {}
                    self.optxs_history[symbol]['ohlc'] = symbol_ohlc
                    self.optxs_history[symbol]['txs'] = symbol_txs

            except:
                
                print(f'{symbol} invalid or data permissions not available.')
                self.disconnect_ibkr()
                continue
            


    
    def optxs_history_from_yahoo(self,txs):

        self.multiple_securities_from_yahoo(txs)
        self.txs = pd.merge(txs,self.ohlc[['date_time','open','close']],on='date_time', how='left').drop_duplicates()

        for iter, symbol in tqdm(enumerate(txs['symbol'].unique())):
            
            
            self.optxs_history[symbol] = {}
            self.optxs_history[symbol]['ohlc'] = self.ohlc[self.ohlc['symbol'] == symbol]
            self.optxs_history[symbol]['txs'] = self.optxs[self.txs['symbol'] == symbol]
            
            
