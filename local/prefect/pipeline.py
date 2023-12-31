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
@task(log_prints=True, tags=["download"])
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
@task(log_prints=True, tags=["transform and load data"])
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
@task(log_prints=True, tags=["checking downloaded data into database"])
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
    
@task(log_prints=True, tags=["download templates into Metabase"])
def metabase(years:list):
    # Connection creation
    mblogin = years[0]
    mbpass = years[1]
    try:
        mb = Metabase_API('http://localhost:3001/', mblogin, mbpass)
        print("connection ok")
    except:
        print("connection failed")
        return()
    
    # Collection creation
    try:
        colid = mb.get_item_id('collection', "MVC_collection")
        print("collection 'MVC_collection' exists")
    except:
        mb.create_collection("MVC_collection", parent_collection_name='Root', return_results=False)
        print("collection 'MVC_collection' created")
        colid = mb.get_item_id('collection', "MVC_collection")

    # Checking database
    try:
        dbid = mb.get_item_id('database', "MVC_db")
        print("database ok")
    except:
        print("MVC_db not found:connect or rename database")
        return()

    # Cards creation
    crd_names = pipeline_set.crd_names
    for i in crd_names:
        crd = i
        crd['dataset_query']['database'] = dbid
        crdnm = crd['name']
        try: 
            crdid = mb.get_item_id('card', crdnm, collection_name = "MVC_collection", collection_id=colid)
        except: 
            crdid = -1
        if crdid == -1:
            mb.create_card(custom_json = crd, collection_id = colid)
            print(f"card {crdnm} created")
        else:
            mb.delete_item('card', item_name = crdnm, collection_name = "MVC_collection", collection_id=colid, verbose=False)
            mb.create_card(custom_json = crd, collection_id = colid)
            print(f"card {crdnm} overwrited")

@flow(name="MVC_main", log_prints=True, description = "Select dataset for processing: \n \
'C' for Motor Vehicle Collisions - Crashes \n\
'V' for Motor Vehicle Collisions - Vehicles \n\
'P' for Motor Vehicle Collisions - Person \n\
Select years for partitioning and upload into database(separate table for each selected year). \n\
Years presented in the dataset: \n\
[2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023] \n\
Or select 'check' for checking downloaded data into database. \n\
Or '[data_type] reload' for reloading dataset")
def MVC_main(data_type, years):

    csv_name, data_type = download_data(data_type)

    if  csv_name == "err":
        return(print("data_type error, please choose 'C','V','P' for data_type, 'check' for checking downloaded data into database, '[data_type] reload' for reloading dataset"))
    elif csv_name == "check":
        check_downloaded_data()
    elif csv_name == "metabase":
        metabase(years)
    else: 
        engine  = create_tables(csv_name, years, data_type)
        transform_and_load(years,csv_name,engine,data_type)


if __name__ == '__main__':
    years = [i for i in range(2012,2024)]

    MVC_main('C', years)
    MVC_main('V', years)
    MVC_main('P', years)
    