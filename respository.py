from dagster import repository, job, graph
from main import *
from transform import *
from vis import *

@job
def dagster_etl_pipeline():
    # Start extraction process
    # traffic_crash_events_data = extract_and_store_json_data_in_mongodb()
    # crash_victims_data = ingest_csv_data_to_mongodb()

    # loaded_data = transform_and_load(traffic_crash_events_data, crash_victims_data)
    loaded_data = transform_and_load()
    #visualise all the 7 research questions
    visualize_time_of_day_impact(loaded_data)
    visualize_age_gender_impact(loaded_data)
    visualize_safety_measures_effectiveness(loaded_data)
    visualize_environmental_impact(loaded_data)
    visualize_crash_causes(loaded_data)
    visualize_cell_phone_impact(loaded_data)
    visualize_geographic_patterns(loaded_data)

@repository
def my_repository():
    return [dagster_etl_pipeline]
