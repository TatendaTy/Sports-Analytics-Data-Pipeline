from scrape import *
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()

functions = [league_table,top_scorers,detail_top,player_table,all_time_table,all_time_winner_club,top_scorers_seasons,goals_per_season]


def get_connection_string():
    """
    Collects the connection strings for the Blob Storage and PostgreSQL Database

    Raises:
        ValueError: If the required Azure storage settings are missing in the environment variables.

    Returns:
        str: The Azure Blob Storage connection string or the PostgreSQL Database connection string
    """    
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if connection_string:
        return connection_string

    storage_account_name = os.getenv("storage_account_name") or os.getenv("STORAGE_ACCOUNT_NAME")
    storage_account_key = os.getenv("sports_analytics_storage_key") or os.getenv("AZURE_STORAGE_KEY")

    if not storage_account_name or not storage_account_key:
        raise ValueError("Missing Azure storage settings in .env")

    return (
        "DefaultEndpointsProtocol=https;"
        f"AccountName={storage_account_name};"
        f"AccountKey={storage_account_key};"
        "EndpointSuffix=core.windows.net"
    )

def to_blob(func):
    '''
    Converts the output of a given function to Parquet format and uploads it to Azure Blob Storage.
    Args:
        func (function): The function that retrieves data to be processed and uploaded.
    Returns:
        None
    This function takes a provided function, calls it to obtain data, and then converts the data into
    an Arrow Table. The Arrow Table is serialized into Parquet format and uploaded to an Azure Blob
    Storage container specified in the function. The function's name is used as the blob name.
    Example:
        Consider the function "top_scorers". Calling "to_blob(top_scorers)" will process the output
        of "top_scorers", convert it to Parquet format, and upload it to Azure Blob Storage.
        '''

    file_name = func.__name__
    func = func()


    # Convert DataFrame to Arrow Table
    table = pa.Table.from_pandas(func)

    parquet_buffer = BytesIO()
    pq.write_table(table, parquet_buffer)

    connection_string = get_connection_string()
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_name = os.getenv("BLOB_CONTAINER_NAME", "testtech")
    blob_name = f"{file_name}.parquet"
    container_client = blob_service_client.get_container_client(container_name)

    try:
        container_client.create_container()
    except ResourceExistsError:
        pass

    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(parquet_buffer.getvalue(), overwrite=True)
    print(f"{blob_name} successfully updated")


for items in functions:
    to_blob(items)
