import pandas as pd
import logging
import os
from sqlalchemy import create_engine
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level = logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''this func will ingest the df into a db table'''
    df.to_sql(table_name, con = engine, if_exists = 'replace', index=False)

def load_raw_data():
    '''this func will load CSVs as dfs and ingest into db'''
    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv('data/'+file)
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end-start)/60
    logging.info('--------------Ingestion Complete--------------')
    logging.info(f'Total Time Taken: {total_time} minutes') 

if __name__ == '__main__':
    load_raw_data()

