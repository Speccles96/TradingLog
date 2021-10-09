import pandas as pd
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
        

    def print_head(self):
        print(self.df.head())

    def determine_input_type(self):
        if '.tlg' in self.path:
            self.filetype = 'tlg'
        elif '.ofx' in self.path:
            self.filetype = 'ofx'
        elif os.path.isdir(self.path) == True:
            self.filetype = 'dir'
        return self.filetype       

    def parse_input(self):

        if self.filetype == '.tlg':
            df = pd.read_csv(data,sep='delimiter', header=None,engine='python')
            df = df[0].str.split('|',expand=True)
            df = df.fillna(value=np.nan)
        
        elif self.filetype == 'dir':
            all_files = glob.glob(os.path.join(self.path, "*.tlg"))     # advisable to use os.path.join as this makes concatenation OS independent

            df_from_each_file = (pd.read_csv(f,sep='delimiter', header=None,engine='python') for f in all_files)
            df = pd.concat(df_from_each_file, ignore_index=True)
            df = df[0].str.split('|',expand=True)
            df = df.fillna(value=np.nan)
    
        else:
            print('Error')
            
        print(df)
        
        #Variables
        headers = []
        headers_index = sorted([len(df)])
        dfs = {}
        previous_index = 0
        count=0
        
        #Iterates through df rows
        for line in range(0,len(df)):
            
            #Indexes by titles
            #TODO probably a better way to do this than by > 7. Fix later...maybe
            if len(df[0].iloc[line]) > 7:
                
                #Creates header name and index 
                header = df[0].iloc[line]
                header_index = df.index.get_loc(line)
                
                
                #Stores header name and index  to list 
                headers.append(header)
                headers_index.append(header_index)
                
                #appends multiple dataframes for each category header to dict for output
            previous_index = header_index
            headers_index = sorted(headers_index)
            
            
        for header in headers:
            index1 = headers_index[count]
            index2 = headers_index[count+1]
            dfs[header] = df.iloc[index1:index2]
            
            count += 1
            
            
        #Column Transformations to output proper format
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
        
        self.optxs = dfs['OPTION_TRANSACTIONS']
        self.dfs = dfs
        return 


#parse_tlg('/Users/david/Downloads/')
#'/Users/david/Downloads/trade_log.tlg'