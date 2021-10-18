from TradingLog.file_parser import Parse
from TradingLog.data import DataEngine
from TradingLog.plot import Plotting
import pandas as pd 
from datetime import datetime,timedelta

ofx_file = 'test.ofx'
tlg_file = 'test.tlg'

ofx = Parse()
tlg = Parse()
data = DataEngine()
plot = Plotting()

ofx.parse_input(ofx_file)
print(f'ofx parse_input attributes: {ofx.__dict__.keys()}')

tlg.parse_input(tlg_file)
print(f'tlg parse_input attributes: {ofx.__dict__.keys()}')

ofx.parse_input(ofx_file)
print(f'ofx parse_input attributes: {ofx.__dict__.keys()}')

tlg.parse_input(tlg_file)
print(f'tlg parse_input attributes: {ofx.__dict__.keys()}')

print(f' \n Path: {tlg.path}\n File type: {tlg.filetype}\n Option txs columns" {tlg.optxs.columns}\n Stock txs columns {tlg.stktxs.columns}')