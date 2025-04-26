import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_att = ['Name', 'MC_USD_Billion']
file_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
conn = sqlite3.connect(db_name)

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_att):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_att)

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            name = col[1].get_text(strip=True)
            market_cap = col[2].get_text(strip=True)
            data_dict = {'Name': name, 'MC_USD_Billion': market_cap}
            df1 = pd.DataFrame([data_dict])
            df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df):
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)
    df_rates = pd.read_csv('./exchange_rate.csv')

    for index, row in df_rates.iterrows():
        currency = row['Currency']
        rate = row['Rate']
        new_column = f'MC_{currency}_Billion'
        df[new_column] = (df['MC_USD_Billion'] * rate).round(2)
    return df


def load_to_csv(df, file_path):
    df.to_csv(file_path)


def load_to_db(df, conn, table_name):
    df.to_sql(table_name, conn, if_exists='replace', index=False)


def run_query(query_statement, conn):
    print(query_statement)
    query_output = pd.read_sql(query_statement, conn)
    print(query_output)

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_att)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, file_path)
log_progress('Data saved to CSV file')

load_to_db(df, conn, table_name)
log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, conn)

query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement2, conn)

query_statement3 = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement3, conn)

log_progress('Process Complete.')
conn.close()

