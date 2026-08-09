# cspell:ignore dotenv
import os
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# To load secret environment variables from your .env file
load_dotenv()
def upload_json_blob(container_name: str, blob_name: str, data: dict):
    """
    Uploads a Python dictionary directly as a JSON file to Azure Data Lake.
    """
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")

    try:
        # Initialize Azure Client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        
        # Serialize dict to JSON string & upload
        json_data = json.dumps(data, indent=4).encode('utf-8')
        blob_client.upload_blob(json_data, overwrite=True)
        print(f" Successfully uploaded '{blob_name}' to container '{container_name}'.")
        
    except Exception as e:
        print(f" Failed to upload '{blob_name}': {e}")
        raise

if __name__ == "__main__":
    # Test payload
    sample_data = {
        "status": "success",
        "message": "Azure Data Lake Storage connection verified!",
        "pipeline_stage": "ingestion_test"
    }
    
    # Upload test payload
    upload_json_blob(
        container_name="raw-transit", 
        blob_name="test_connection.json", 
        data=sample_data
    )