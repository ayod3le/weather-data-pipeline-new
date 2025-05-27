# import pandas as pd
# import json
# import os
# import glob
# from datetime import datetime, timezone

# RAW_DATA_PATH = "raw_data_landing_zone"
# TRANSFORMED_DATA_PATH = "transformed_data_landing_zone"
# os.makedirs(TRANSFORMED_DATA_PATH, exist_ok=True)

# def transform_single_record(raw_data):
#     """Transforms a single raw weather data record (JSON/dict) into a flat dictionary."""
#     transformed_record = {
#         'city_name': raw_data.get('name'),
#         'country': raw_data.get('sys', {}).get('country'),
#         'longitude': raw_data.get('coord', {}).get('lon'),
#         'latitude': raw_data.get('coord', {}).get('lat'),
#         'weather_condition': raw_data.get('weather', [{}])[0].get('main'),
#         'weather_description': raw_data.get('weather', [{}])[0].get('description'),
#         'temperature_celsius': raw_data.get('main', {}).get('temp'),
#         'feels_like_celsius': raw_data.get('main', {}).get('feels_like'),
#         'temp_min_celsius': raw_data.get('main', {}).get('temp_min'),
#         'temp_max_celsius': raw_data.get('main', {}).get('temp_max'),
#         'pressure_hpa': raw_data.get('main', {}).get('pressure'),
#         'humidity_percent': raw_data.get('main', {}).get('humidity'),
#         'visibility_meters': raw_data.get('visibility'),
#         'wind_speed_mps': raw_data.get('wind', {}).get('speed'), # meters per second
#         'wind_deg': raw_data.get('wind', {}).get('deg'),
#         'cloudiness_percent': raw_data.get('clouds', {}).get('all'),
#         'data_calc_timestamp_unix': raw_data.get('dt'),
#         'sunrise_unix': raw_data.get('sys', {}).get('sunrise'),
#         'sunset_unix': raw_data.get('sys', {}).get('sunset'),
#         'timezone_shift_seconds': raw_data.get('timezone'),
#         'source_api_id': raw_data.get('id') # City ID from API
#     }

#     # Convert Unix timestamps to ISO 8601 UTC strings
#     if transformed_record['data_calc_timestamp_unix']:
#         transformed_record['data_calc_datetime_utc'] = datetime.fromtimestamp(
#             transformed_record['data_calc_timestamp_unix'], timezone.utc
#         ).isoformat()
#     else:
#         transformed_record['data_calc_datetime_utc'] = None

#     if transformed_record['sunrise_unix']:
#         transformed_record['sunrise_datetime_utc'] = datetime.fromtimestamp(
#             transformed_record['sunrise_unix'], timezone.utc
#         ).isoformat()
#     else:
#         transformed_record['sunrise_datetime_utc'] = None
    
#     if transformed_record['sunset_unix']:
#         transformed_record['sunset_datetime_utc'] = datetime.fromtimestamp(
#             transformed_record['sunset_unix'], timezone.utc
#         ).isoformat()
#     else:
#         transformed_record['sunset_datetime_utc'] = None
    
#     # Add a timestamp for when this transformation occurred
#     transformed_record['transformation_datetime_utc'] = datetime.now(timezone.utc).isoformat()
    
#     return transformed_record

# def process_all_raw_data():
#     print("Starting data transformation...")
#     all_transformed_records = []
    
#     # Use glob to find all JSON files in the raw data directory
#     json_files = glob.glob(os.path.join(RAW_DATA_PATH, "*.json"))
    
#     if not json_files:
#         print(f"No JSON files found in {RAW_DATA_PATH}. Make sure get_weather_data.py has run successfully.")
#         return

#     for file_path in json_files:
#         print(f"Processing file: {file_path}")
#         with open(file_path, 'r') as f:
#             try:
#                 raw_data = json.load(f)
#                 transformed_record = transform_single_record(raw_data)
#                 all_transformed_records.append(transformed_record)
#             except json.JSONDecodeError:
#                 print(f"Error decoding JSON from file {file_path}. Skipping.")
#             except Exception as e:
#                 print(f"Error processing file {file_path}: {e}. Skipping.")
    
#     if not all_transformed_records:
#         print("No records were transformed. Exiting.")
#         return

#     # Create a Pandas DataFrame
#     df = pd.DataFrame(all_transformed_records)
    
#     # Optional: Further DataFrame-level transformations/cleaning
#     # Example: Ensure correct data types (Pandas often infers well from dicts)
#     # df['temperature_celsius'] = pd.to_numeric(df['temperature_celsius'], errors='coerce')
#     # df['data_calc_datetime_utc'] = pd.to_datetime(df['data_calc_datetime_utc'], errors='coerce')

#     # Save the transformed data to a CSV file
#     output_filename = os.path.join(TRANSFORMED_DATA_PATH, f"transformed_weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
#     df.to_csv(output_filename, index=False)
#     print(f"Successfully transformed data saved to {output_filename}")
#     print(f"DataFrame shape: {df.shape}")
#     print("First 5 rows of transformed data:")
#     print(df.head())

# if __name__ == "__main__":
#     process_all_raw_data()

import pandas as pd
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient # Import BlobServiceClient

load_dotenv()

AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
RAW_AZURE_CONTAINER_NAME = "raw-weather-data"
TRANSFORMED_AZURE_CONTAINER_NAME = "transformed-weather-data"

# No longer need local RAW_DATA_PATH, TRANSFORMED_DATA_PATH, os.makedirs, glob

def transform_single_record(raw_data):
    """Transforms a single raw weather data record (JSON/dict) into a flat dictionary."""
    transformed_record = {
        'city_name': raw_data.get('name'),
        'country': raw_data.get('sys', {}).get('country'),
        'longitude': raw_data.get('coord', {}).get('lon'),
        'latitude': raw_data.get('coord', {}).get('lat'),
        'weather_condition': raw_data.get('weather', [{}])[0].get('main'),
        'weather_description': raw_data.get('weather', [{}])[0].get('description'),
        'temperature_celsius': raw_data.get('main', {}).get('temp'),
        'feels_like_celsius': raw_data.get('main', {}).get('feels_like'),
        'temp_min_celsius': raw_data.get('main', {}).get('temp_min'),
        'temp_max_celsius': raw_data.get('main', {}).get('temp_max'),
        'pressure_hpa': raw_data.get('main', {}).get('pressure'),
        'humidity_percent': raw_data.get('main', {}).get('humidity'),
        'visibility_meters': raw_data.get('visibility'),
        'wind_speed_mps': raw_data.get('wind', {}).get('speed'),
        'wind_deg': raw_data.get('wind', {}).get('deg'),
        'cloudiness_percent': raw_data.get('clouds', {}).get('all'),
        'data_calc_timestamp_unix': raw_data.get('dt'),
        'sunrise_unix': raw_data.get('sys', {}).get('sunrise'),
        'sunset_unix': raw_data.get('sys', {}).get('sunset'),
        'timezone_shift_seconds': raw_data.get('timezone'),
        'source_api_id': raw_data.get('id')
    }

    dt_unix = transformed_record.get('data_calc_timestamp_unix')
    if dt_unix is not None:
        transformed_record['data_calc_datetime_utc'] = datetime.fromtimestamp(
            dt_unix, timezone.utc
        ).isoformat()
    else:
        transformed_record['data_calc_datetime_utc'] = None

    sunrise_unix = transformed_record.get('sunrise_unix')
    if sunrise_unix is not None:
        transformed_record['sunrise_datetime_utc'] = datetime.fromtimestamp(
            sunrise_unix, timezone.utc
        ).isoformat()
    else:
        transformed_record['sunrise_datetime_utc'] = None
    
    sunset_unix = transformed_record.get('sunset_unix')
    if sunset_unix is not None:
        transformed_record['sunset_datetime_utc'] = datetime.fromtimestamp(
            sunset_unix, timezone.utc
        ).isoformat()
    else:
        transformed_record['sunset_datetime_utc'] = None
    
    transformed_record['transformation_datetime_utc'] = datetime.now(timezone.utc).isoformat()
    return transformed_record

def process_azure_data():
    print("Starting data transformation from Azure Blob Storage...")
    all_transformed_records = []

    if not AZURE_CONNECTION_STRING:
        print("Error: AZURE_STORAGE_CONNECTION_STRING is not set in .env file.")
        return

    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        raw_container_client = blob_service_client.get_container_client(RAW_AZURE_CONTAINER_NAME)
    except Exception as e:
        print(f"Error creating BlobServiceClient or ContainerClient: {e}")
        return

    blob_list = list(raw_container_client.list_blobs()) # Convert to list to check length and iterate
    
    if not blob_list:
        print(f"No JSON files found in Azure container {RAW_AZURE_CONTAINER_NAME}. Run get_weather_data.py first.")
        return

    for blob_item in blob_list:
        print(f"Processing blob: {blob_item.name}")
        try:
            blob_client = raw_container_client.get_blob_client(blob=blob_item.name)
            downloader = blob_client.download_blob()
            raw_data_bytes = downloader.readall()
            raw_data_str = raw_data_bytes.decode('utf-8') # Decode bytes to string
            raw_data = json.loads(raw_data_str) # Parse JSON string

            if not isinstance(raw_data, dict):
                print(f"Warning: Data in blob {blob_item.name} is not a dictionary. Skipping.")
                continue
            
            transformed_record = transform_single_record(raw_data)
            all_transformed_records.append(transformed_record)
        except json.JSONDecodeError:
            print(f"Error decoding JSON from blob {blob_item.name}. Skipping.")
        except Exception as e:
            print(f"Error processing blob {blob_item.name}: {e}. Skipping.")
    
    if not all_transformed_records:
        print("No records were transformed. Exiting.")
        return

    df = pd.DataFrame(all_transformed_records)
    
    # Save the transformed data to a CSV blob in Azure
    output_blob_name = f"transformed_weather_data_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    
    try:
        transformed_blob_client = blob_service_client.get_blob_client(container=TRANSFORMED_AZURE_CONTAINER_NAME, blob=output_blob_name)
        csv_data = df.to_csv(index=False) # Get CSV as a string
        transformed_blob_client.upload_blob(csv_data.encode('utf-8'), overwrite=True) # Upload as bytes
        print(f"Successfully transformed data and uploaded to Azure Blob: {TRANSFORMED_AZURE_CONTAINER_NAME}/{output_blob_name}")
        print(f"DataFrame shape: {df.shape}")
        print("First 5 rows of transformed data (not saving locally):")
        print(df.head())
    except Exception as e:
        print(f"Error uploading transformed data to Azure: {e}")


if __name__ == "__main__":
    process_azure_data()