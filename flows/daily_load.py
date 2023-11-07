from prefect import flow, task 
from datetime import datetime 
from etl_code.etl import * 
from etl_code.config import config, db_engine
import requests
from sqlalchemy import text

@task
def get_net_new_datasets_task():
    """
    Compare datasets that we have on record vs. datasets that are available on 
    the Open Data Portal. Find what is missing and return a list of missing dataset
    IDs.

    """
    
    # Look for datasets already collected
    query = """
    SELECT id 
    FROM datasets
    """
    
    # Execute query and load results into pandas DataFrame
    df = pd.DataFrame(db_engine().connect().execute(text(query)))
    
    # Create list of datasets already collected
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
    
    # Write dict to file
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f'../data/net_new_data_{today}'

    with open(f'{filename}', "w") as f:
        json.dump(net_new_dict, f)

    f.close()

    return filename

@task
def net_new_datasets_table(filename):
    # open net new file 
    with open(filename) as f:
        data = json.load(f) 
    
    f.close()

    # Create dataset_table 
    dataset_table = create_dataset_table(data)

    return dataset_table

@task
def dataset_table_to_postgres(dataset_table):
    # Create connection
    conn = psycopg2.connect(**config())

    # Write dataset table to postgres
    df_to_postgres(conn, dataset_table, "datasets")

@task 
def net_new_resources_table(filename):
    # open net new file 
    with open(filename) as f:
        data = json.load(f) 
    
    f.close()

    # Create dataset_table 
    resource_table = create_resource_table(data)

    return resource_table

@task
def resource_table_to_postgres(resource_table):
    # Create connection
    conn = psycopg2.connect(**config())

    # Write dataset table to postgres
    df_to_postgres(conn, resource_table, "resources")

@flow
def main_flow():
    filename = get_net_new_datasets_task()
    dataset_table = net_new_datasets_table(filename)
    dataset_table_to_postgres(dataset_table)
    resource_table = net_new_resources_table(filename)
    resource_table_to_postgres(resource_table)

if __name__ == "__main__":
    main_flow()



