#!/usr/bin/env python
# coding: utf-8

# In[319]:


import math
import pandas as pd
import sqlite3


# In[381]:


import sys
sys.path


# In[320]:


# Create a new database file:
db = sqlite3.connect("output_ql.sqlite")

# chunking, so read big data by chunks instead loading the whole to cache which used up memories
for c in pd.read_csv("input.csv", header=None,names=['TS','Symbol','Qty','Px'], chunksize=1000): #add header to make things easier to read
    # Append all rows to a new database table
    c.to_sql("output_ql", db, if_exists="append")


# In[321]:


# Indexing
db.execute("CREATE INDEX Symbol ON output_ql(Symbol)") 
db.close()


# In[377]:


#Function to get the full list of Symbol in string items
#If future datasize is huge, we just need to run by Symbols
#Sorted by symbol ascending
def get_symbol_list_info():
    connection=sqlite3.connect("output_ql.sqlite")
    query="Select DISTINCT Symbol FROM output_ql ORDER BY Symbol ASC"
    return pd.read_sql_query(query,connection)
Symbol_array=get_symbol_list_info()['Symbol'].astype(str).values.tolist() #Create a list with string items for further queries


# In[378]:


#Function to get every data of a specific symbol
#Sorted by Timestamp ascending (so we can calc the timestamp increment correctly in a day)
def get_symbol_info(value):
    connection=sqlite3.connect("output_ql.sqlite")
    query=("Select * FROM output_ql where Symbol=? ORDER BY TS ASC")
    return pd.read_sql_query(query,connection,params=[value])


# In[382]:


#list to store the data
symbols_info=[]

#Go through every Symbol and get the data needed
for Symbol in Symbol_array:
    symbol_info=get_symbol_info(Symbol)
    symbol=symbol_info['Symbol'][0]
    MaxTimeGap=math.trunc(symbol_info['TS'].diff().max()) #MaxTimeGap
    Volume=symbol_info['Qty'].sum() #Total Volume
    WeightedAveragePrice=math.trunc(((symbol_info['Qty']*symbol_info['Px']).sum()/Volume)) #WeightedAveragePrice
    MaxPrice=symbol_info['Px'].max() #MaxPrice
    symbols_info.append([symbol,MaxTimeGap, Volume, WeightedAveragePrice, MaxPrice]) #append as list
    del symbol_info
#End Loop

#Exported the data to csv file format required``
output_data=pd.DataFrame(symbols_info)
output_data.to_csv('output.csv',header=None, index=False)


# In[391]:


#clean up for re-un
db = sqlite3.connect("output_ql.sqlite")
db.execute("DROP INDEX Symbol") 
db.close()


# In[ ]:




