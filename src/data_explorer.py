import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

def explore_crude_oil_data():
    try:
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Download the real crude oil data
        blob_client = blob_service_client.get_blob_client(
            container="raw-data", 
            blob="crude-oil-price.csv"
        )
        
        print("📥 Downloading crude oil data from Azure...")
        blob_data = blob_client.download_blob().readall()
        
        # Convert to pandas DataFrame
        df = pd.read_csv(StringIO(blob_data.decode('utf-8')))
        
        print("✅ Data loaded successfully!")
        print(f"📊 Dataset shape: {df.shape}")
        print(f"📅 Date range: {df.columns}")
        print("\n🔍 First 5 rows:")
        print(df.head())
        
        print("\n📈 Data info:")
        print(df.info())
        
        return df
        
    except Exception as e:
        print(f"❌ Failed to explore data: {e}")

if __name__ == "__main__":
    explore_crude_oil_data()