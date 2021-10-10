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




