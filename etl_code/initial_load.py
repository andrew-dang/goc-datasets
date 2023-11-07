from config import config
from etl import *

# Packages
import psycopg2
import json

def initial_load(filepath):
    """
    Load data retrieved from the GoC Open Data Portal on 
    October 31st, 2023 into the database as the initial load. 
    """
    # Create tables in database
    create_tables()
    
    # Load initial data
    with open(filepath) as f:
        data = json.load(f) 

    f.close()

    # Create dataframe 
    dataset_table = create_dataset_table(data)

    # Create connection object 
    conn = psycopg2.connect(**config())

    # Write data to Postgres
    df_to_postgres(conn, dataset_table, "datasets")
    
    # Create resources table
    resource_table = create_resource_table(data)
    
    # Write resources table to database
    df_to_postgres(conn, resource_table, "resources")

if __name__ == "__main__":
    initial_load("../data/initial_extract.json")