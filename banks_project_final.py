import os
import requests
import sqlite3
import pandas as pd 
from bs4 import BeautifulSoup
import numpy as np 
from datetime import datetime


db_name='banks.db'
sql_connection= sqlite3.connect(db_name)
table_name='Largest_banks'
table_attribs=["Rank","Bank name","Market cap"]
url= 'https://web.archive.org/web/20230908081635/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_file= '/home/project/Largest_banks.csv'
message=['Start program','extract data','transform data','load to csv','load to db', 'Sql query statement']
output_path='/home/project/Largest_banks.csv'
table_name2='exchange_rate'
attribute_list= ["Currency","Rate"]
file_path= '/home/project/exchange_rate.csv'
df2=pd.read_csv(file_path,names=attribute_list)
print(df2)


print(df2.keys())

def log_progress(message):
    log_path_file='code_log.txt'
    with open(log_path_file,'a') as log_file:
        for points in message:
            timestamp= datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log_file.write(f'[{timestamp}]{points}\n')
    return points       
def extract(url, table_attribs):
    df=pd.DataFrame(columns=table_attribs)
    count=0
    html_page=requests.get(url).text
    data=BeautifulSoup(html_page,'html.parser')

    tables=data.find_all('tbody')
    rows=tables[0].find_all('tr')
    rank=[]
    banks=[]
    market_caps=[]
    for row in rows:
        if count< 10:
            col=row.find_all('td')
            if len(col)!=0:

                rank_no=col[0].get_text(strip=True)
                bank_name=col[1].get_text(strip=True)

                market_cap=col[2].get_text(strip=True)
                
                rank.append(rank_no)
                banks.append(bank_name)
                market_caps.append(market_cap)
    df=pd.DataFrame({'Rank':rank, 'Name':banks, 'MC_USD_Billion': market_caps})
    df['MC_USD_Billion' ]=df['MC_USD_Billion'].astype(float)
    print(df)
    return df 

def read_exchange_rates(file_path):
    exchange_df = pd.read_csv(file_path)
    exchange_dict = dict(zip(exchange_df['Currency'], exchange_df['Rate']))
    return exchange_dict

# File path to the CSV
file_path = '/home/project/exchange_rate.csv'
exchange_rate = read_exchange_rates(file_path)    

def transform(df,exchange_rate):
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    print(df)
    print(df['MC_EUR_Billion'][4])
    return df	

def load_to_csv(df, output_path):
    load_df_csv=df.to_csv(output_path)
    print('Table csv is ready')
    return load_df_csv    

def load_to_db(df, sql_connection, table_name):
    load_df_db=df.to_sql(table_name,sql_connection, if_exists='replace',index=False)
    print('Table db is ready')
    return load_df_db


def run_query(query_statement, sql_connection):
    cursor = sql_connection.cursor()
    for index, query in enumerate(query_statement):
        print(f"Executing Query {index + 1}:\n{query}\n")
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            print("Query Output:")
            for row in result:
                print(row)
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
        print("\n" + "-"*50 + "\n")
    
    cursor.close()
    return query

log_progress(message)
extract(url,table_attribs)
transform(extract(url,table_attribs),exchange_rate)
load_to_csv(transform(extract(url,table_attribs),exchange_rate), output_path)
load_to_db(transform(extract(url,table_attribs),exchange_rate), sql_connection, table_name)
query_statement=["select * from Largest_banks", "select Name from Largest_banks limit 5","select avg(MC_GBP_Billion) from Largest_banks"]
run_query(query_statement, sql_connection)


 