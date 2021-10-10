from ib_insync import IB, util, Stock
from parser import Parse
import pandas as pd
from datetime import datetime, timedelta
import math

import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import plotly.figure_factory as ff

ib = IB()

#TODO move to helper file
def hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +timedelta(hours=t.minute//30))


class Plotting:
    def __init__(self,df=None):
        self.df = df

    def plot(self,engine):
        pass


    def plot_ibkr(self):
        self.connect_to_ibkr()
        print('1')
        print('2')
        self.disconnect_ibkr()

        
     

    def plot_yahoo_finance(self):
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



    def graph(self,df,mask,ticker):
        '''
        Returns Plotly figure if passed a df containing 1 tickers transactions
        '''
        # standardize column names & create buy/sell dataframes
        df.columns = df.columns.str.lower()
        buys = mask[mask['units']>0]
        sells = mask[mask['units']<0]
        
        fig = make_subplots(rows=2, cols=1,specs=[[{"secondary_y": True}],
                                                [{"secondary_y": True}]])
        
        fig.add_trace(go.Candlestick(x=df['date_time'],
                open=df['open'], 
                high=df['high'],
                low=df['low'], 
                close=df['close'],
                yaxis='y2',
                whiskerwidth=.5,
                name='OHLC',
                line=dict(width=1),
                increasing={'line': {'width': 1}},
                decreasing={'line': {'width': 1}})

                ),

        fig.add_trace(go.Scatter(
                        x=sells['date_time'],
                        y=sells['open']*1.02,
                        mode='markers',
                        name='Sell',
                        marker_symbol='triangle-down',
                        yaxis='y2',
                        marker=dict(
                            color='red',
                            size=14,
                            line=dict(
                                color='DarkSlateGrey',
                                width=1)))
                    ),
        
        fig.add_trace(go.Scatter(
                        x=buys['date_time'],
                        y=buys['close']*.98,
                        mode='markers',
                        name='Buy',
                        marker_symbol='triangle-up',
                        yaxis='y2',
                        marker=dict(
                            color='green',
                            size=14,
                            line=dict(
                                color='DarkSlateGrey',
                                width=1)))),


        fig.update_layout(xaxis=dict(tickformat='%b-%y',rangebreaks=[
                                                                    dict(bounds=["sat", "mon"]),
    #                                                                  dict(values=holidays_list),
                                                                    dict(bounds=[17, 9], pattern="hour")
                                                                    ]),
                        xaxis_rangeslider_visible=True,
                        height=800,width=1400,
                        hoverdistance=0,hovermode='y',title=f"{ticker} - Total PNL: ${-mask['total_price'].astype('float64').sum()}")
        

        return fig




