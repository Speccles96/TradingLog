import pandas as pd
pd.options.mode.chained_assignment = None

import glob
import numpy as np
import os

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


class Parse:
    def __init__(self, data):
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        self.path = str(data)
        self.determine_input_type()
        self.parse_input()
        

    def determine_input_type(self):
        if '.tlg' in self.path:
            self.filetype = 'tlg'
        elif '.ofx' in self.path:
            self.filetype = 'ofx'
        elif os.path.isdir(self.path) == True:
            self.filetype = 'dir'
        else:
            self.filetype = None
        return self.filetype       


    def parse_input(self):
        if self.filetype == 'tlg':
            df = pd.read_csv(self.path,sep='delimiter', header=None,engine='python')
            df = df[0].str.split('|',expand=True)
            df = df.fillna(value=np.nan)
        
        elif self.filetype == 'dir':
            all_files = glob.glob(os.path.join(self.path, "*.tlg"))     # advisable to use os.path.join as this makes concatenation OS independent

            df_from_each_file = (pd.read_csv(f,sep='delimiter', header=None,engine='python') for f in all_files)
            df = pd.concat(df_from_each_file, ignore_index=True)

            df = df[0].str.split('|',expand=True)
            df = df.fillna(value=np.nan)
    
        elif self.filetype == None:
            print(f'Error: File type not supported. Check source of file at {print(self.path)}')
            

        #Variables
        dfs = {}

        
        #Slices datafame and assigns to dictionary
        dfs['ACCOUNT_INFORMATION'] = df[df[0] == 'ACT_INF'].drop_duplicates()
        dfs['OPTION_TRANSACTIONS'] = df[df[0] == 'OPT_TRD'].drop_duplicates()
        dfs['STOCK TRANSACTIONS'] = df[df[0] == 'STK_TRD'].drop_duplicates()
        dfs['STOCK_POSITIONS'] = df[df[0] == 'STK_LOT'].drop_duplicates()
        dfs['OPTION_POSITIONS'] = df[df[0] == 'OPT_LOT'].drop_duplicates()
            
            
        #Column Transformations to output proper format for option transactions
        dfs['OPTION_TRANSACTIONS']['symbol'] = dfs['OPTION_TRANSACTIONS'][2].str.split(' ').str[0]
        dfs['OPTION_TRANSACTIONS'].columns = ['option_txs','fitid','option_ticker','opt_description',
                                            'exchange','optselltype','buy_sell_type','date','time',
                                            'currency','units','100','unit_price',
                                            'total_price','commission','currate','symbol'
                                            ]
        
        dfs['OPTION_TRANSACTIONS']['buy_sell_type'] = dfs['OPTION_TRANSACTIONS']['buy_sell_type'].str.split(';').str[0]
        dfs['OPTION_TRANSACTIONS']['strike'] = dfs['OPTION_TRANSACTIONS']['opt_description'].str.split(' ').str[2]
        dfs['OPTION_TRANSACTIONS']['opt_type']  = dfs['OPTION_TRANSACTIONS']['opt_description'].str.split(' ').str[-1]
        dfs['OPTION_TRANSACTIONS']['expiration'] = dfs['OPTION_TRANSACTIONS']['option_ticker'].str.split(' ').str[-1].str.split('C').str[0].str.split('P').str[0]
        dfs['OPTION_TRANSACTIONS'] = dfs['OPTION_TRANSACTIONS'].iloc[1:]
        dfs['OPTION_TRANSACTIONS']['date_time'] = dfs['OPTION_TRANSACTIONS']['date']+' '+dfs['OPTION_TRANSACTIONS']['time']
        pd.to_datetime(dfs['OPTION_TRANSACTIONS']['date_time'],format='%Y%m%d %H:%M:%S')
        dfs['OPTION_TRANSACTIONS']['date_time'] = dfs['OPTION_TRANSACTIONS']['date_time'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S'))
        #dfs['OPTION_TRANSACTIONS']['ticker'] = dfs['OPTION_TRANSACTIONS']['option_ticker'].str.extract(r'([^0-9][^0-9]?[^0-9]?[^0-9])')
        

        #Assigns dataframes to variables
        self.optxs = dfs['OPTION_TRANSACTIONS']
        self.stktxs = dfs['STOCK TRANSACTIONS']
        self.acctinfo = dfs['ACCOUNT_INFORMATION']
        self.optpostions = dfs['OPTION_POSITIONS']
        self.stkpositions = dfs['STOCK_POSITIONS']
        self.dfs = dfs

        
#parse_tlg('/Users/david/Downloads/')
#'/Users/david/Downloads/trade_log.tlg'