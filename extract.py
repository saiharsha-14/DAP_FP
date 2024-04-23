from dagster import job
from main import *
 
@job
def job_extract_and_store_data_in_mongodb():
    extract_and_store_data_in_mongodb()
 
@job
def job_ingest_csv_to_mongodb():
    ingest_csv_to_mongodb()
