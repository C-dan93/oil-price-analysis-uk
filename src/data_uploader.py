"""
os: Used to access environment variables (like the Azure connection string)

requests: Though imported, it's not used in this script (would be for future HTTP requests)

BlobServiceClient: Main Azure Blob Storage client for interacting with storage

load_dotenv: Loads environment variables from a .env file (for secure credential storage)
"""
import os
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

def upload_sample_data():

"""
    Uploads a sample CSV file to the 'raw-data' container in Azure Blob Storage
    and lists all files currently in that container.

    This function:
    1. Reads the Azure connection string from environment variables.
    2. Connects to Azure Blob Storage.
    3. Creates a small test dataset (CSV format) in memory.
    4. Uploads the test dataset to the 'raw-data' container.
    5. Lists all blobs (files) in the container to confirm the upload.

    Parameters:
        None – all configuration is read from environment variables.

    Returns:
        None – Prints upload status and container contents to the console.
"""
    try:
        # Get Azure connection string from environment variables
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        
        # Create BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Get container client
        container_client = blob_service_client.get_container_client("raw-data")
        
        # Upload a test file (we'll start with a sample)
        print("📤 Uploading test data to Azure...")
        
        # Create sample test data
        test_data = "date,price\n2024-01-01,75.50\n2024-01-02,76.20"
        
        # Upload to blob
        blob_client = container_client.upload_blob(
            name="test_oil_prices.csv",
            data=test_data,
            overwrite=True
        )
        
        print("✅ Test data uploaded successfully!")
        
        # List files to confirm
        print("📁 Files in container:")
        blobs = container_client.list_blobs()
        for blob in blobs:
            print(f"   - {blob.name}")
            
    except Exception as e:
        print(f"❌ Upload failed: {e}")

if __name__ == "__main__":
    upload_sample_data()
