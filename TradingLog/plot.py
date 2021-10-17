from ib_insync import IB, util, Stock
import pandas as pd
from datetime import datetime, timedelta
import math

import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import plotly.figure_factory as ff

ib = IB()


class Plotting:
    def __init__(self,ohlc=None):
        self.ohlc = ohlc

    def plot(self,engine):
        pass


    def plot_ibkr(self):
        self.connect_to_ibkr()
        print('1')
        print('2')
        self.disconnect_ibkr()

        
     
    def plot_yahoo_finance(self):
        pass

    




    def graph(self,ohlc,txs,ticker):
        '''
        Returns Plotly figure if passed a ohlc containing 1 tickers transactions
        '''
        # standardize column names & create buy/sell dataframes
        
        ohlc = ohlc[ohlc['symbol'] == ticker]
        txs['trade_time'] = txs['date_time'] 

        if round(sum(ohlc['date_time'].dt.minute)/len(ohlc['date_time'])) == 30:
            txs['date_time'] = txs['date_time'].apply(lambda date: date.replace(minute=30,second=00))

    

        
        txs = txs[txs['symbol'] == ticker]
        txs['units'] = txs['units'].astype('int64')
        

        txs = pd.merge(txs,ohlc[['date_time','open','close']],on='date_time', how='left').drop_duplicates()
    

        buys = txs[(txs['units']>0) & (txs['symbol'] == ticker)]
        sells = txs[(txs['units']<0) & (txs['symbol'] == ticker)]
        
        min_date = txs['date_time'].min() - timedelta(60)
        max_date = txs['date_time'].max() + timedelta(60)
        
        ohlc = ohlc[(ohlc['date_time']>min_date) & (ohlc['date_time']<max_date)]
        



        fig = make_subplots(rows=2, cols=1,specs=[[{"secondary_y": True}],
                                                [{"secondary_y": True}]])
        
        fig.add_trace(go.Candlestick(x=ohlc['date_time'],
                open=ohlc['open'], 
                high=ohlc['high'],
                low=ohlc['low'], 
                close=ohlc['close'],
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
                        hoverdistance=0,hovermode='y',title=f"{ticker} - Total PNL: ${-txs['total_price'].astype('float64').sum()}")
        
        return fig, ohlc, buys, sells




