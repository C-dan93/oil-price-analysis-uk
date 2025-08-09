import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import StringIO

load_dotenv()
"""
    Downloads and explores crude oil price data from Azure Blob Storage.

    This function:
    1. Reads the Azure connection string from environment variables.
    2. Connects to Azure Blob Storage.
    3. Downloads the 'crude-oil-price.csv' file from the 'raw-data' container.
    4. Loads the CSV into a pandas DataFrame.
    5. Prints dataset shape, column names, sample rows, and info summary.
    
    Parameters:
        None – all configuration is read from environment variables.

    Returns:
        pandas.DataFrame: DataFrame containing crude oil price data.
                          Returns None if download or processing fails.
"""
def explore_crude_oil_data():
    try:
        # Connect to Azure Blob Storage
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Get blob client for crude oil price data
        blob_client = blob_service_client.get_blob_client(
            container="raw-data", 
            blob="crude-oil-price.csv"
        )

        # Download the real crude oil data
        print("📥 Downloading crude oil data from Azure...")
        blob_data = blob_client.download_blob().readall()
        
        # Convert to pandas DataFrame
        df = pd.read_csv(StringIO(blob_data.decode('utf-8')))
        
        # Display dataset details
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
