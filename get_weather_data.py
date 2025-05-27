# import os
# import requests
# import json
# from datetime import datetime
# from dotenv import load_dotenv

# load_dotenv() # Loads variables from .env file

# API_KEY = os.getenv("WEATHER_API_KEY")
# BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# LOCATIONS = ["London", "New York", "Tokyo", "Lagos", "Berlin", "Sydney"] # Add/change as you like
# RAW_DATA_LANDING_ZONE = "raw_data_landing_zone"

# # Create the directory if it doesn't exist
# os.makedirs(RAW_DATA_LANDING_ZONE, exist_ok=True)

# def fetch_and_save_weather_data():
#     print("Starting weather data extraction...")
#     for city in LOCATIONS:
#         params = {
#             'q': city,
#             'appid': API_KEY,
#             'units': 'metric' # Or 'imperial' for Fahrenheit
#         } # <<<<---- CORRECTED: Closing brace for params dictionary was missing here

#         print(f"Fetching data for {city}...") # <<<<---- CORRECTED: Indentation
#         try:
#             response = requests.get(BASE_URL, params=params)
#             response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)

#             # This is the part that was missing from your provided code:
#             weather_data = response.json()
            
#             # Generate a timestamp for the filename
#             timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
#             # Ensure city names with spaces are handled correctly in filenames
#             safe_city_name = city.replace(' ', '_') 
#             filename = os.path.join(RAW_DATA_LANDING_ZONE, f"raw_weather_{safe_city_name}_{timestamp_str}.json")
            
#             with open(filename, 'w') as f:
#                 json.dump(weather_data, f, indent=4)
#             print(f"Successfully fetched and saved data for {city} to {filename}")

#         except requests.exceptions.HTTPError as http_err:
#             print(f"HTTP error occurred for {city}: {http_err}")
#             # If the API key is invalid (401), it might be good to stop instead of continuing
#             if response and response.status_code == 401:
#                 print("Error 401: Invalid API Key. Please check your .env file and OpenWeatherMap account.")
#                 break # Stop further requests if API key is bad
#             continue # Skip to next city for other HTTP errors
#         except requests.exceptions.RequestException as req_err: # More general request exception
#             print(f"Request error occurred for {city}: {req_err}")
#             continue
#         except Exception as err:
#             print(f"Other error occurred for {city}: {err}")
#             continue
#         # The misplaced closing brace from params was here: }
        
#     print("Weather data extraction finished.")

# # Main execution block
# if __name__ == "__main__":
#     fetch_and_save_weather_data()

import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient # Import BlobServiceClient

load_dotenv()

API_KEY = os.getenv("WEATHER_API_KEY")
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING") # Get connection string
RAW_AZURE_CONTAINER_NAME = "raw-weather-data" # Your Azure container for raw data

BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
LOCATIONS = ["London", "New York", "Tokyo", "Lagos", "Berlin", "Sydney"]

# No longer need local RAW_DATA_LANDING_ZONE or os.makedirs for local storage

def fetch_and_upload_weather_data():
    print("Starting weather data extraction and Azure upload...")

    if not AZURE_CONNECTION_STRING:
        print("Error: AZURE_STORAGE_CONNECTION_STRING is not set in .env file.")
        return
    if not API_KEY:
        print("Error: WEATHER_API_KEY is not set in .env file.")
        return

    try:
        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    except Exception as e:
        print(f"Error creating BlobServiceClient: {e}")
        return

    for city in LOCATIONS:
        params = {
            'q': city,
            'appid': API_KEY,
            'units': 'metric'
        }

        print(f"Fetching data for {city}...")
        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            weather_data = response.json()

            # Generate a timestamp for the blob name
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_city_name = city.replace(' ', '_')
            blob_name = f"raw_weather_{safe_city_name}_{timestamp_str}.json"

            # Get a BlobClient
            blob_client = blob_service_client.get_blob_client(container=RAW_AZURE_CONTAINER_NAME, blob=blob_name)

            # Upload the JSON data (needs to be bytes)
            blob_client.upload_blob(json.dumps(weather_data, indent=4).encode('utf-8'), overwrite=True)
            print(f"Successfully fetched and uploaded data for {city} to Azure Blob: {RAW_AZURE_CONTAINER_NAME}/{blob_name}")

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred for {city}: {http_err}")
            if response and response.status_code == 401:
                print("Error 401: Invalid API Key. Please check your .env file and OpenWeatherMap account.")
                break
            continue
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred for {city}: {req_err}")
            continue
        except Exception as err:
            print(f"Other error occurred for {city} during API call or upload: {err}")
            continue

    print("Weather data extraction and Azure upload finished.")

if __name__ == "__main__":
    fetch_and_upload_weather_data()