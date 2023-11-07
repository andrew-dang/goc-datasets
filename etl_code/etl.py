import requests
import re
import math
import json 
import pandas as pd 
import numpy as np
from tqdm import tqdm
from datetime import datetime

import psycopg2
import psycopg2.extras as extras

# relative import 
from config import config

import logging 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


##############################
## GENERAL HELPER FUNCTIONS ##
##############################

def clean_record_title(record_title: str) -> str:
    """
    Clean the dataset title. 
    """
    record_title = re.sub(r"\n", "", record_title) # remove escape characters
    record_title = record_title.strip()
    
    return record_title

def calculate_num_iter() -> int:
    """
    Calculates the number of times we need to send API requests. 
    """
    # Do an initial call to figure out how many iterations we need (limit of 1000 rows per call)
    url = 'https://open.canada.ca/data/en/api/3/action/package_search?q=&rows=1'
    response = requests.get(url)
    
    # Convert to dictionary
    response_dict = response.json()
    
    # Get number of records, divide by 1000
    num_iter = math.floor(response_dict['result']['count']/1000)
    
    return num_iter


########################################################
## EXTRACTION: GETTING DATASETS FROM OPEN DATA PORTAL ##
########################################################

def get_datasets() -> dict:
    """
    Send API requests to Government of Canada Open Data Portal and save
    all the datasets. 
    """
    
    # Get num iter
    num_iter = calculate_num_iter()
    
    # Empty dict to store dataset details
    all_results_dict = {}
    
    for i in range (0,num_iter+1):
        url = f'https://open.canada.ca/data/en/api/3/action/package_search?q=&rows=1000&start={1 + i*1000}'
        logger.info(f"Getting datasets from following url: {url}")
        
        # Request response
        response = requests.get(url)
        
        # Converting response to dictionary
        response_dict = response.json()

        # results is a list of dictionaries, each dictionary represents a dataset
        results = response_dict['result']['results']
        
        # Get datasets from current iteration and save to a dictionary
        iter_dict = {clean_record_title(d['title']): d for d in results}
        
        # Update all_results_dict with datasets found in current iteration
        all_results_dict.update(iter_dict)
    
    return all_results_dict

def get_net_new_datasets() -> dict:
    """
    Compare datasets that we have on record vs. datasets that are available on 
    the Open Data Portal. Find what is missing and return a list of missing dataset
    IDs.
    
    NOTE: During production, we want to query our database to see what IDs we 
    currently have. For testing, I am using a JSON file. 
    
    """
    # Read in Data
    df = pd.read_json("dataset_table.json")
    
    # Create list of datasets
    existing_datasets = df.id.tolist()
    
    # Get IDs available on Open Data Portal 
    url = f'https://open.canada.ca/data/en/api/3/action/package_list'
    response = requests.get(url)
    response_dict = response.json()
    available_datasets = response_dict['result']
    
    # Calculate net new datasets 
    net_new_datasets = list(set(available_datasets) - set(existing_datasets))
    logger.info(f"There are {len(net_new_datasets)} net new datasets on the Open Data Portal.")
    
    # Get net new datasets 
    net_new_dict = {}
    logger.info("Retrieving metadata for net new datasets...")
    for dataset_id in tqdm(net_new_datasets):
        url = f'https://open.canada.ca/data/en/api/3/action/package_show?id={dataset_id}'
        response = requests.get(url)
        response_dict = response.json()

        results = response_dict['result']
        net_new_dict[clean_record_title(results['title'])] = results
    
    return net_new_dict


###############
## TRANSFORM ##
###############
def get_english_url(url: str) -> str:
    """
    The URL value is a string that appears to be a dictionary.
    This function is used to extract the English URL. 
    """
    
    if url == None:
        processed_url = None
    else:
        # Split URL by language
        urls = url.split(",")
        
        for link in urls:
            # Check if link is English
            if "u'en'" in link or "'en': u" in link:
                link = re.sub('"', "", link)
                link = re.sub("[\{ ]u'.{2}': u'", "", link)
                processed_url = re.sub("'?}?", "", link)
                break
        
    return processed_url 

def get_french_url(url: str) -> str:
    """
    The URL value is a string that appears to be a dictionary.
    This function is used to extract the French URL. 
    """
    
    if url == None:
        processed_url = None
    else:
        # Split URL by language
        urls = url.split(",")
        
        for link in urls:
            # Check if link is French
            if "u'fr'" in link or "'fr': u" in link:
                link = re.sub('"', "", link)
                link = re.sub("[\{ ]u'.{2}': u'", "", link)
                processed_url = re.sub("'?}?", "", link)
                break
    
    return processed_url
    
def create_dataset_table(datasets_data: dict) -> pd.DataFrame:
    """
    Build a pandas DataFrame containing the data from the KEYS list
    that has a row for each dataset. Each row will have the 
    dataset id, title, dataset creation date, the last update date, 
    dataset URL, keywords, and notes.
    """
    # Keys in the dictionary we want to extract
    KEYS = ["id", "title", "url", "notes_translated", "keywords", "metadata_created", "metadata_modified", "isopen"]
    
    # Iterate through each dataset getting the value from the keys above
    table_data = [[v[key] for key in KEYS] for k,v in datasets_data.items()]
    
    # Column names 
    columns = ["id", "title", "url", "notes_translated", "keywords", "created_date", "last_update_date", "isopen"]
    
    # Create DataFrame
    df = pd.DataFrame(table_data, columns=columns)
    
    ### Cleaning the  DataFrame 
    
    # Get English and French URL in separate columns 
    df['url'].replace("{u'fr': u'', u'en': u''}", None, inplace=True) # Removes strange json string 
    df['url'].replace("{'fr': u'', 'en': u''}", None, inplace=True) # Removes strange json string 
    df['url'].replace("", None, inplace=True)
    
    df['url_en'] = df['url'].apply(lambda x: get_english_url(x)) # Get English URL
    df['url_fr'] = df['url'].apply(lambda x: get_french_url(x)) # Get French URL
    df.drop('url', axis=1, inplace=True) # Drop the original URL column
    
    # Normalize JSON columns 
    # keywords
    keywords = pd.json_normalize(df['keywords'])
    df = pd.concat([df, keywords[['en', 'fr']]], axis=1)
    df.rename(columns={"en": "keywords_en", "fr": "keywords_fr"}, inplace=True)
    df.drop('keywords', axis=1, inplace=True)
    
    # notes
    notes = pd.json_normalize(df['notes_translated'])
    df = pd.concat([df, notes[['en', 'fr']]], axis=1)
    df.rename(columns={"en": "notes_en", "fr": "notes_fr"}, inplace=True)
    df.drop('notes_translated', axis=1, inplace=True)
    
    # Add ingestion date 
    today = datetime.now().strftime("%Y-%m-%dT%T.%f")
    df['ingestion_date'] = today
    
    # Convert dates to datetime data types
    df['created_date'] = pd.to_datetime(df['created_date'])
    df['last_update_date'] = pd.to_datetime(df['last_update_date'])
    df['ingestion_date'] = pd.to_datetime(df['ingestion_date'])

    # Replacing np.nan with None in object columns - url, keywords, notes
    for col in ['url_en', 'url_fr', 'keywords_en', 'keywords_fr', 'notes_en', 'notes_fr']:
        df[col] = df[col].replace(np.nan, None)

    return df

def create_resource_table(datasets_dict):
    """
    Create a table for all resources in the dataset. 
    """
    columns = ["dataset_id", "title", "resource_id", "file_format", "created_date", "last_update_date", "resource_url"]
    table_data = []
    
    # Loop through each dataset in resource dict
    for dataset in datasets_dict:
        dataset_dict = datasets_dict[dataset]
        dataset_id = dataset_dict['id']

        for resource in dataset_dict['resources']: # resource is a dictionary 
            resource_id = resource['id']
            resource_format = resource['format']
            resource_created = resource['created']
            resource_last_modified = resource['last_modified']
            resource_url = resource['url']

            table_data.append([dataset_id, dataset, resource_id, resource_format, resource_created, resource_last_modified, resource_url])
    
    # Create table using table_data
    df = pd.DataFrame(table_data, columns=columns)

    # If last update date is empty, replace with created_date
    df['last_update_date'] = df['last_update_date'].fillna(df['created_date'])

    # Add ingestion date
    today = datetime.now().strftime("%Y-%m-%dT%T.%f")
    df['ingestion_date'] = today
    
    # Convert date columns to datetime data type
    df['created_date'] = pd.to_datetime(df['created_date'])
    df['last_update_date'] = pd.to_datetime(df['last_update_date'])
    df['ingestion_date'] = pd.to_datetime(df['ingestion_date'])


    # Replacing np.nan with None in object columns = resource_url
    df['resource_url'] = df['resource_url'].replace(np.nan, None)
    
    return df

#########################################
## FUNCTIONS TO INTERACT WITH POSTGRES ##
#########################################
def create_tables():
    commands = (
    """
    DROP TABLE IF EXISTS datasets
    """,
    """
    DROP TABLE IF EXISTS resources
    """,
    """
    CREATE TABLE IF NOT EXISTS datasets (
        id text PRIMARY KEY,
        title text,
        created_date timestamp, 
        last_update_date timestamp, 
        isopen boolean,
        url_en text,
        url_fr text, 
        keywords_en text[],
        keywords_fr text[],
        notes_en text,
        notes_fr text,
        ingestion_date timestamp
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS resources (
        dataset_id text, 
        title text, 
        resource_id text, 
        file_format text, 
        created_date timestamp,
        last_update_date timestamp, 
        resource_url text,
        ingestion_date timestamp, 
        PRIMARY KEY(dataset_id, resource_id)
    )
    """
    )
    
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            cur.close()

def df_to_postgres(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame, 
    table: str): 
    """
    Upsert rows in pandas DataFrame to avoid duplicating 
    records in the table. 
    
    conn: psycopg2 connection object. 
    df: DataFrame to append to database table. 
    table: Name of the database table. 
    """
    
    # Convert df to list of tuples where a tuple represents a record
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    
    # Create cursor
    cursor = conn.cursor()
    
    # Insert each row into database table
    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT DO NOTHING" % (table, cols)
    
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1 
    
    print("Inserted pandas dataframe")
