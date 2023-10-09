# Necessary libraries for data handling, date/time manipulation, OS operations, and specific task orchestration.
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Date, Time,Text

import os
from time import time
from datetime import timedelta

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

import pprint
from metabase import Metabase
from metabase_api import Metabase_API

import pipeline_set

# Task to download data based on the specified data type.
def download_data(data_type:str):
    # Checks to return appropriate strings or download csv files based on the given data type.
    if data_type == "check":
        return("check", data_type)
    elif data_type == "metabase":
        return("metabase", data_type)
    elif data_type[:1] in ["C","V","P"] and data_type[1:] == " reload":
        data_type = data_type[:1]
        url = getattr(pipeline_set,f"url_{data_type}")
        csv_name = f"MVC_{data_type}.csv"
        os.system(f"curl -L -o {csv_name} {url}")
        return(csv_name, data_type)
    elif data_type in ["C","V","P"]:
        url = getattr(pipeline_set,f"url_{data_type}")
        csv_name = f"MVC_{data_type}.csv"
        if os.path.isfile(csv_name) is not True:
            os.system(f"curl -L -o {csv_name} {url}")
            return(csv_name, data_type)
        else:
            return(csv_name, data_type)
    else:
        return("err")

# Task to create necessary tables to store downloaded data.
@task(log_prints = True, tags=["creating_tables"])
def create_tables(csv_name:str, years:list , data_type:str):
    # Reads the csv
    df = pd.read_csv(csv_name, nrows = 1000, low_memory=False)
    # Selects specified columns
    df = df[getattr(pipeline_set,f"sel_{data_type}")]
    # Rename them 
    df.rename(columns=(getattr(pipeline_set,f"sel_rename_{data_type}")),inplace=True)
    # Convert to datetime type in datetime columns
    df.crash_date = pd.to_datetime(df.crash_date).dt.date
    df.crash_time = pd.to_datetime(df.crash_time,format= '%H:%M' ).dt.time

    # Creates empty tables in a database
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    engine = connection_block.get_connection(begin=False) 
    
    for i in years:
        df.head(n=0).to_sql(name = f"MVC_{data_type}_{i}",con = engine, dtype=(getattr(pipeline_set,f"sel_types_{data_type}")),if_exists = 'replace')

    return(engine)

# Task to transform data and load into the specified tables.
def transform_and_load(years:list, csv_name:str, engine, data_type:str):
    
    total_rows=0
    total_rows_loaded = 0
    total_time = 0

    # Iterates through chunks of data from the CSV
    df_iter = pd.read_csv(csv_name, iterator = True, chunksize = 100000, low_memory=False)
    df = next(df_iter)
    
    while len(df) > 0:
        try:
            start_time = time()
            
            # Process data
            df = df[getattr(pipeline_set,f"sel_{data_type}")]
            df.rename(columns=(getattr(pipeline_set,f"sel_rename_{data_type}")),inplace=True)
            df.crash_date = pd.to_datetime(df.crash_date).dt.date
            df.crash_time = pd.to_datetime(df.crash_time,format= '%H:%M' ).dt.time
            
            total_rows += len(df)
            
            # Appends them to the tables
            for i in years:
                df_temp = df.loc[pd.DatetimeIndex(df.crash_date).year == i]
                df_temp.to_sql(name = f"MVC_{data_type}_{i}",con = engine, if_exists = 'append')
                
                total_rows_loaded += len(df_temp)
            

            end_time = time()
            total_time += (end_time - start_time)


            print(
            f"total rows processed = {total_rows}", 
            f"total rows loaded = {total_rows_loaded}", 
            'iteration took %.2f seconds' % (end_time - start_time),
            f"total time = %.2f seconds" % (total_time),
            "", sep = "\n")

            df = next(df_iter)

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

# Task to check if the data was successfully downloaded and saved into the database.
def check_downloaded_data():
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    engine = connection_block.get_connection(begin=False) 

    res = [['year', 'Crashes','Vehicles','Person']]
    
    # Verifies data presence in the database and prints out summaries.
    for i in range(2012,2024):
        print(f"{i} spreadsheets check")
        temp = []
        temp.append(i)
        try:
            df = pd.read_sql_query('(SELECT COUNT(*) FROM "MVC_C_{}" )'.format(i),con=engine)
            t = int(df.get(key = 'count'))
        except:
            t = 0
        temp.append(t)
        try:
            df = pd.read_sql_query('(SELECT COUNT(*) FROM "MVC_V_{}" )'.format(i),con=engine)
            t = int(df.get(key = 'count'))
        except:
            t = 0
        temp.append(t)
        try:
            df = pd.read_sql_query('(SELECT COUNT(*) FROM "MVC_P_{}" )'.format(i),con=engine)
            t = int(df.get(key = 'count'))
        except:
            t = 0
        temp.append(t)
        res.append(temp)
    C,V,P =0,0,0
    st = '   Downloaded data report:' + '\n'  + '\n'
    for i in range(13):
        for j in range(4):
            if i == 0 or j == 0:
                h = res[i][j]
            else: 
                h = "{:,}".format(res[i][j])
            st += str(h).rjust(11)
        st += '\n'
        if i > 0:
            C += int(res[i][1])
            V += int(res[i][2])
            P += int(res[i][3])
    st = st + '\n' + 'total'.rjust(11) + str("{:,}".format(C)).rjust(11) + str("{:,}".format(V)).rjust(11) + str("{:,}".format(P)).rjust(11)
    print(st)

data_type = 'C'
csv_name, data_type = download_data(data_type)