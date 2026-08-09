# cspell:ignore dotenv gtfs gtfsr pb2 protobuf
import os
import requests
from datetime import datetime
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from dotenv import load_dotenv, find_dotenv

# Imported working Azure function!
from azure_uploader import upload_json_blob 

# Load secrets
load_dotenv(find_dotenv())
NTA_API_KEY = os.getenv("NTA_API_KEY")

# The exact TFI endpoint for live vehicle positions
TRANSIT_API_URL = "https://api.nationaltransport.ie/gtfsr/v2/Vehicles"

API_HEADERS = {
    "Cache-Control": "no-cache",
    "x-api-key": NTA_API_KEY
}

def fetch_and_store_transit_data():
    """Fetches live NTA transit data, translates it to JSON, and uploads to Azure."""
    try:
        print("Fetching live transit data from NTA...")
        response = requests.get(TRANSIT_API_URL, headers=API_HEADERS)

        if response.status_code == 401:
            print(" 401 Unauthorized: Your API key is likely still activating. Please wait 10-15 minutes and try again.")
            return
            
        response.raise_for_status()
        
        # 1. Parsed the compressed Protobuf binary data
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        
        # 2. Converted into a clean Python dictionary (JSON format)
        live_data_dict = MessageToDict(feed)
        
        # 3. Creating unique filename based on the exact second
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"transit_vehicles_{timestamp}.json"
        
        # 4. Uploads it directly to the Data Lake!
        upload_json_blob(
            container_name="raw-transit", 
            blob_name=filename, 
            data=live_data_dict
        )
        
    except Exception as e:
        print(f" Error in pipeline: {e}")

if __name__ == "__main__":
    fetch_and_store_transit_data()