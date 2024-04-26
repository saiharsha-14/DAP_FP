import json
import logging
import pymongo
from dagster import op, Out
from pymongo import errors
import pandas as pd

# Set up a basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
 
@op(out=Out(bool))
def extract_and_store_json_data_in_mongodb(context) -> bool: 
    result = False
    mongo_connection_string = "mongodb://mydap:mydapnci7@localhost:27017/myDatabase?authSource=admin"
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
 
        context.log.info("Data successfully loaded and inserted into MongoDB.")
        result = True
 
    except Exception as e:
        context.log.error("An error occurred: {}".format(e))
        result = False
 
    return result

@op(out=Out(bool))
def ingest_csv_data_to_mongodb(context) -> bool:
    mongo_connection_string = "mongodb://mydap:mydapnci7@localhost:27017/myDatabase?authSource=admin"
    client = pymongo.MongoClient(mongo_connection_string)
    db = client["TrafficIncidentsDB"]
    collection = db['crash_victims']
    #Indexing is created for CRASH_RECORD_ID for quicker access
    collection.create_index([('CRASH_RECORD_ID', pymongo.ASCENDING)], unique=False)
    file_path = "D:\\Traffic_Crashes_-_People.csv"  # Use double backslashes for Windows paths

    try:
        # Read CSV data using pandas
        data_df = pd.read_csv(file_path)
        # Convert DataFrame to dictionary records for MongoDB insertion
        data_records = data_df.to_dict('records')
        # Insert data into MongoDB, use pandas to handle potential duplicate keys elegantly
        result = collection.insert_many(data_records, ordered=False)
        context.log.info(f"Inserted {len(result.inserted_ids)} records successfully into MongoDB.")
        return True

    except pymongo.errors.BulkWriteError as bwe:
        context.log.error("Bulk write error occurred due to duplicate keys or other issues.")
        context.log.error(bwe.details)
        return False

    except Exception as e:
        context.log.error(f"An error occurred: {e}")
        return False