import os
import requests
from datetime import datetime
from dotenv import load_dotenv, find_dotenv

# Import the Azure upload function to reuse the established storage connection logic
from azure_uploader import upload_json_blob 

# Load environment variables to retrieve the Azure connection string
load_dotenv(find_dotenv())

# Public Eirgrid API endpoint for live power system demand
EIRGRID_API_URL = "https://smartgriddashboard.eirgrid.com/DashboardService.svc/data?typeofrequest=cachedareq&field=SystemDemand"

def fetch_and_store_eirgrid_data():
    """Fetches live Irish power grid demand and uploads to Azure."""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    try:
        print("Fetching live power demand data from Eirgrid...")
        
        # Include standard headers to ensure the API request is accepted
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        }
        
        # Eirgrid has an SSL mismatch, bypass verification
        response = requests.get(EIRGRID_API_URL, headers=headers, verify=False)
        response.raise_for_status()
        
        # Parse the API response into a JSON dictionary format
        grid_data_dict = response.json()
        
        # Generate a unique filename appended with the current timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"grid_demand_{timestamp}.json"
        
        # Upload the raw JSON data to the designated Azure storage container
        upload_json_blob(
            container_name="raw-eirgrid", 
            blob_name=filename, 
            data=grid_data_dict
        )
        print(f"SUCCESS: Uploaded '{filename}' to Azure container 'raw-eirgrid'.")
        
    except Exception as e:
        print(f"ERROR in Eirgrid pipeline: {e}")

if __name__ == "__main__":
    fetch_and_store_eirgrid_data()
