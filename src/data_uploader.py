import os
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

def upload_sample_data():
    try:
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
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