import psycopg2 
from config import config

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