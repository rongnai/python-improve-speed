# -*- coding: utf-8 -*-
# input: all the trader's transaction csv files 
# output: panel data by trader, date_only, and the newly created control variables
import pandas as pd
import numpy as np 
from datetime import datetime
import datetime
from datetime import date, timedelta
 
def do_format_close(row):
    val = pd.to_datetime(row['dateClosed'], format='%Y-%m-%d', errors='ignore')
    return val

def gen_open_index(row):
    start_time = datetime.datetime.strptime ("2000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S") 
    open_time = datetime.datetime.strptime (row['dateOpen'], "%Y-%m-%d %H:%M:%S")
    open_index = (open_time - start_time).days
    return open_index 

def gen_close_index(row):
    start_time = datetime.datetime.strptime ("2000-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    close_time = datetime.datetime.strptime (row['dateClosed'], "%Y-%m-%d %H:%M:%S")
    close_index = (close_time - start_time).days
    return close_index 

# the same function still slow 
def cal_active_trading2(row):
    row_index = row['index']
    day_list = day_list_full[:row_index+1] 
    day_list = sum(day_list, [])
    active_day = len(set(day_list))
    return active_day


trader_id_set = pd.read_csv('trader_id_list.csv', header=None)
trader_id_set = trader_id_set[0]

# read each trader's transaction csv file one by one.
count = 1
for i, trader_id in enumerate(trader_id_set):
    print (count)
    filename = "D:/Google Drive/History_20180911/"+str(trader_id)+".csv"
    
    try: 
        df_temp = pd.read_csv(filename)
    except:
        print("skip this trader id")
        continue 
    
    if df_temp['id'].count() > 0: # modify
        df_temp = df_temp[['lots','dateOpen','dateClosed','maxProfit','worstDrawdown','pips','netPnl','totalAccumulatedPips','totalAccumulatedPnl']]
        df_temp["trader_id"] = trader_id
        
        df_temp = df_temp.sort_values(by=['dateClosed'], ascending=True) # attention default True, after it, the index "changes"
        df_temp = df_temp.reset_index(drop=True) # add a new column "index" with the old index value
        df_temp['index'] = df_temp.index     
        
        ### Very slow ### 
        ### calculate the active trading days 
        df_temp["open_index"] = df_temp.apply(lambda row: pd.Series(gen_open_index(row)), axis=1)
        df_temp["close_index"] = df_temp.apply(lambda row: pd.Series(gen_close_index(row)), axis=1)
        day_list_full = [] 
        for index, row in df_temp.iterrows(): 
            val = list ( range(row['open_index'], row['close_index']+1) )
            day_list_full.append(val)      
        df_temp["active_day"] = df_temp.apply(lambda row: pd.Series(cal_active_trading2(row)), axis=1)
        #break
        
        df_temp["Close"] = df_temp.apply(do_format_close, axis=1)
        df_temp['temp'] = pd.to_datetime(df_temp['dateClosed'], errors='coerce')
        df_temp['date_only'] = df_temp['temp'].dt.date
        df_temp = df_temp.drop(columns=['temp'])
    
        # the last step in this stage 
        df_temp = df_temp.drop_duplicates(subset=['date_only'], keep='last')
              
        # generate the time-series 
        mytimeframe = pd.date_range('2016-08-01', periods=519, freq='1D') # 1D20min
        mytimeframe = mytimeframe.to_frame(index = False) 
        mytimeframe = mytimeframe.rename(columns={0: 'date_only'})
        mytimeframe['date_only'] = mytimeframe['date_only'].dt.date
        
        df = pd.merge(left=df_temp, right=mytimeframe, how='outer', left_on=['date_only'], right_on=['date_only'])
        df = df.fillna(method='ffill') # apply for all the variables 
        df = df.fillna(method='bfill') # key
        
        if count == 1: 
            panel_data = df 
        else: 
            panel_data = panel_data.append(df)
        count = count + 1        
        #break   
    
#panel_data.to_csv('panel_data.csv', index = False, header = True) # large 

