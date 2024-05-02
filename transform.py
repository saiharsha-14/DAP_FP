import numpy as np
import pandas as pd
from dagster import op, Out, In, get_dagster_logger
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from pymongo import MongoClient, errors
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import NullPool
from sqlalchemy.types import *
from datetime import datetime
import psycopg2
from sqlalchemy.sql import text
import yaml

logger = get_dagster_logger()

def load_config():
    with open('config.yaml', 'r') as file:
        return yaml.safe_load(file)

config = load_config()
# Connection string to the default database('postgres')
default_admin_connection_string = config['connection']['default_admin_connection_string']
# Connection string to the target database
postgres_connection_string = config['connection']['postgres_connection_string']
# Connection string to the mongodb
mongo_connection_string = config['connection']['mongo_connection_string']

def create_database(database_name):
    try:
        # Create an engine to the default database
        engine = create_engine(default_admin_connection_string, isolation_level='AUTOCOMMIT')

        # Connect and create the database
        with engine.connect() as connection:
            connection.execute(text(f"CREATE DATABASE {database_name};"))
            logger.info(f"Database {database_name} created successfully.")
    except exc.SQLAlchemyError as error:
        if "already exists" in str(error):
            logger.info(f"Database {database_name} already exists.")
        else:
            logger.error(f"Failed to create database {database_name}: {error}")

TraffCrashEventsDataFrame = create_dagster_pandas_dataframe_type(
    name="TraffCrashEventsDataFrame",
    columns=[
        PandasColumn.string_column("crash_record_id", non_nullable=True),
        PandasColumn.datetime_column("crash_date"),
        PandasColumn.integer_column("crash_hour"),
        PandasColumn.string_column("weather_condition"),
        PandasColumn.string_column("lighting_condition"),
        PandasColumn.string_column("prim_contributory_cause"),
        PandasColumn.string_column("location"),
        PandasColumn.float_column("latitude"),
        PandasColumn.float_column("longitude")
    ]
)

CrashVictimsDataFrame = create_dagster_pandas_dataframe_type(
    name="CrashVictimsDataFrame",
    columns=[
        PandasColumn.string_column("CRASH_RECORD_ID", non_nullable=True),
        PandasColumn.float_column("AGE"),
        PandasColumn.string_column("SEX"),
        PandasColumn.string_column("INJURY_CLASSIFICATION"),
        PandasColumn.string_column("SAFETY_EQUIPMENT"),
        PandasColumn.string_column("AIRBAG_DEPLOYED"),
        PandasColumn.string_column("DRIVER_ACTION"),
        PandasColumn.string_column("PHYSICAL_CONDITION"),
        PandasColumn.string_column("CELL_PHONE_USE")
    ]
)

def transform_traffic_crash_events_data():
    # Connect to the MongoDB database
    client = MongoClient(mongo_connection_string)
    db = client["TrafficIncidentsDB"]
    collection = db['traffic_crash_events']
    # Specifying the fields to include in the results
    fields = {
        'crash_record_id' : 1,
        'crash_date': 1,
        'crash_hour': 1,
        'weather_condition': 1,
        'lighting_condition': 1,
        'prim_contributory_cause': 1,
        'location': 1,
        'latitude': 1,
        'longitude': 1,
        '_id': 0  # Exclude MongoDB's default '_id' field unless needed
    }
    
    non_char_cols = ['crash_hour', 'latitude', 'longitude']
    col_names = ['weather_condition', 'lighting_condition', 'prim_contributory_cause', 'location']

    #Execute the query with field projection
    data = list(collection.find({"crash_date": {"$gte": "2024-01-01T00:00:00"}}, fields))
    
    traffic_crash_events_df = pd.DataFrame(data)
    traffic_crash_events_df['crash_date'] = pd.to_datetime(traffic_crash_events_df['crash_date'], errors='coerce')
    for col in non_char_cols:
        traffic_crash_events_df[col] = pd.to_numeric(traffic_crash_events_df[col], errors='coerce')
    
    traffic_crash_events_df['crash_hour'] = traffic_crash_events_df['crash_hour'].replace('', np.nan)
    traffic_crash_events_df['crash_hour'].fillna(-1, inplace=True)
    for col_name in col_names:
        traffic_crash_events_df[col_name] = traffic_crash_events_df[col_name].replace('', np.nan)
        traffic_crash_events_df[col_name].fillna('No Data', inplace=True)
    for col in ['latitude', 'longitude']:
        traffic_crash_events_df[col] = traffic_crash_events_df[col].replace('', np.nan)
        traffic_crash_events_df[col].fillna(-1.0, inplace=True)

    traffic_crash_events_df['crash_hour'] = traffic_crash_events_df['crash_hour'].astype(int)
    traffic_crash_events_df['latitude'] = traffic_crash_events_df['latitude'].astype(float)
    traffic_crash_events_df['longitude'] = traffic_crash_events_df['longitude'].astype(float)
    # Return the data frame    
    return traffic_crash_events_df

def transform_crash_victims_data():
    # Connect to MongoDB using the specified URI
    client = MongoClient(mongo_connection_string)
    # Connect to the appropriate database and collection
    db = client["TrafficIncidentsDB"]
    collection = db['crash_victims']
    # Specify the fields to include in the results
    fields = {
        'CRASH_RECORD_ID': 1,
        'AGE': 1,
        'SEX': 1,
        'INJURY_CLASSIFICATION': 1,
        'SAFETY_EQUIPMENT': 1,
        'AIRBAG_DEPLOYED': 1,
        'DRIVER_ACTION': 1,
        'PHYSICAL_CONDITION': 1,
        'CELL_PHONE_USE': 1,
        '_id': 0  # Exclude MongoDB's default '_id' field unless needed
    }

    col_names = ['SEX', 'INJURY_CLASSIFICATION', 'SAFETY_EQUIPMENT', 'AIRBAG_DEPLOYED', 'DRIVER_ACTION', 'PHYSICAL_CONDITION', 'CELL_PHONE_USE']

    start_date_2024 = datetime(2024, 1, 1)

    #Execute the query with field projection
    data = list(collection.find({"CRASH_DATE": {"$gte": start_date_2024}}, fields))

    crash_victims_df = pd.DataFrame(data)
    crash_victims_df['AGE'] = pd.to_numeric(crash_victims_df['AGE'], errors='coerce')
    crash_victims_df['AGE']= crash_victims_df['AGE'].replace('', np.nan)
    crash_victims_df['AGE'].fillna(-1, inplace=True)

    for col_name in col_names:
        crash_victims_df[col_name] = crash_victims_df[col_name].replace('', np.nan)
        crash_victims_df[col_name].fillna('No Data', inplace=True)

    # Return the transformed data frame
    return crash_victims_df

def join(traffic_crash_events_df, crash_victims_df) -> pd.DataFrame:
    # Join the two data frames    
    merged_df = traffic_crash_events_df.merge(
        right=crash_victims_df,
        how="left",
        left_on="crash_record_id",
        right_on="CRASH_RECORD_ID"
    )
    
    # Drop the CRASH_RECORD_ID column as we already have a crash_record_id column
    merged_df.drop(["CRASH_RECORD_ID"],axis=1,inplace=True)
    
    # Return the joined data frames
    return merged_df

def load(merged_df):
    try:
        # Try to create the database first; this will do nothing if the database already exists
        db_name = config['connection']['database_name']
        create_database(db_name)
        # Create a connection to the PostgreSQL database
        engine = create_engine(
            postgres_connection_string,
            poolclass=NullPool
        )
        # Create a dictionary with column names as the key and the VARCHAR type as the value.
        #This will be used to specify data types for the created database. We will change some of these types later.
        database_datatypes = dict(
            zip(merged_df.columns,[VARCHAR]*len(merged_df.columns))
        )
        
        # Set date column to have the TIMESTAMP datatype
        database_datatypes["crash_date"] = TIMESTAMP
        
        # Set columns with DOUBLE PRECISION datatype
        for column in ["latitude","longitude","AGE"]:
            database_datatypes[column] = DECIMAL
        
        # # Set columns with INT datatype
        for column in ["crash_hour"]:
            database_datatypes[column] = INT
            
        # Open the connection to the PostgreSQL server
        with engine.connect() as conn:
            rowcount = merged_df.to_sql(
                name="traffic_incidents_table",
                schema="public",
                dtype=database_datatypes,
                con=engine,
                index=False,
                if_exists="replace"
            )
            logger.info("{} records loaded".format(rowcount))
            
        # Close the connection to PostgreSQL and dispose of the connection engine
        engine.dispose(close=True)
        
        logger.info("Data successfully loaded into PostgreSQL.")
        # Return the number of rows inserted
        return rowcount > 0
    
    # Trap and handle any relevant errors
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False
    
# @op(ins={"start1": In(bool), "start2": In(bool)}, out=Out(bool))
# def transform_and_load(start1: bool, start2: bool):
@op( out=Out(bool))
def transform_and_load():
    # Extract and transform traffic_crash_events data
    traffic_crash_events_data = transform_traffic_crash_events_data()
    # Extract and transform crash_victims data
    crash_victims_data = transform_crash_victims_data()
    # Load and join traffic_crash_events and crash_victims data
    joined_data = join(traffic_crash_events_data, crash_victims_data) 

    #loading the data into PostgreSQL
    loaded_data = load(joined_data)
    return loaded_data