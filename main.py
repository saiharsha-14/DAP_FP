import json
import logging
import pymongo
import csv
from datetime import datetime
from dagster import op, Out
from pymongo import errors
import pandas as pd
import yaml

# Set up a basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def load_config():
    with open('config.yaml', 'r') as file:
        return yaml.safe_load(file)

config = load_config()
# Connection string to the mongodb
mongo_connection_string = config['connection']['mongo_connection_string']
 
@op(out=Out(bool))
def extract_and_store_json_data_in_mongodb() -> bool: 
    result = False
    client = pymongo.MongoClient(mongo_connection_string)
    db = client["TrafficIncidentsDB"]
    collection = db['traffic_crash_events']
    # Indexing is created for crash_record_id for quicker access
    collection.create_index([('crash_record_id', pymongo.ASCENDING)], unique=False)

    file_path = "D:\\traff_pep_dataset.json"  # Use double backslashes for Windows paths

    try:
        # Load JSON data from file
        with open(file_path, 'r') as file:
            full_data = json.load(file)
 
        data_entries = full_data['data']
        columns = ['row_id', 'guid', 'meta1', 'created_at', 'meta2', 'updated_at', 'meta3', 'meta4', 'crash_record_id', 'crash_date_est_i', 'crash_date', 'posted_speed_limit', 'traffic_control_device', 'device_condition', 'weather_condition', 'lighting_condition', 'first_crash_type', 'trafficway_type', 'lane_cnt', 'alignment', 'roadway_surface_cond', 'road_defect', 'report_type', 'crash_type', 'intersection_related_i', 'private_property_i', 'hit_and_run_i', 'damage', 'date_police_notified', 'prim_contributory_cause','sec_contributory_cause', 'street_no', 'street_direction', 'street_name', 'beat_of_occurrence', 'photos_taken_i','statements_taken_i', 'dooring_i', 'work_zone_i', 'work_zone_type', 'workers_present_i', 'num_units', 'most_severe_injury', 'injuries_total', 'injuries_fatal', 'injuries_incapacitating', 'injuries_non_incapacitating', 'injuries_reported_not_evident', 'injuries_no_indication', 'injuries_unknown', 'crash_hour', 'crash_day_of_week', 'crash_month', 'latitude', 'longitude', 'location'
        ]

        # Transform data into dictionaries expected by MongoDB
        data_dicts = [dict(zip(columns, entry)) for entry in data_entries]
 
        for data_dict in data_dicts:
            # Use row_id as the unique identifier for MongoDB documents
            data_dict["_id"] = data_dict["row_id"]
            try:
                # Insert data into MongoDB
                collection.insert_one(data_dict)
            except errors.DuplicateKeyError as dke:
                logger.error("Duplicate Key Error: %s" % dke)
                continue
 
        logger.info("Data successfully loaded and inserted into MongoDB.")
        result = True
 
    except Exception as e:
        logger.error("An error occurred: {}".format(e))
        result = False
 
    return result

@op(out=Out(bool))
def ingest_csv_data_to_mongodb() -> bool:
    client = pymongo.MongoClient(mongo_connection_string)
    db = client["TrafficIncidentsDB"]
    collection = db['crash_victims']
    #Indexing is created for CRASH_RECORD_ID for quicker access
    collection.create_index([('CRASH_RECORD_ID', pymongo.ASCENDING)], unique=False)
    file_path = "D:\\Traffic_Crashes_-_People.csv"  # Use double backslashes for Windows paths
    
    try:
        with open(file_path, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            all_data = []

            for row in reader:
                # Parse CRASH_DATE and DATE_POLICE_NOTIFIED to datetime objects
                if 'CRASH_DATE' in row:
                    row['CRASH_DATE'] = datetime.strptime(row['CRASH_DATE'], '%m/%d/%Y %I:%M:%S %p')
        
                all_data.append(row)
            collection.insert_many(all_data)
        logger.info("CSV data successfully loaded and inserted into MongoDB.")
        return True

    except pymongo.errors.BulkWriteError as bwe:
        logger.error("Bulk write error occurred due to duplicate keys or other issues.")
        logger.error(bwe.details)
        return False

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False