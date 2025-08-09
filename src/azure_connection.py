import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

def test_azure_connection():
    try:
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        
        if not connection_string:
            print("No connection string found!")
            return False
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        print("Testing Azure connection...")
        containers = blob_service_client.list_containers()
        
        print("Connection successful!")
        print("Found containers:")
        for container in containers:
            print(f"   - {container.name}")
        
        return True
        
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_azure_connection()