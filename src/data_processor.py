import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

def process_oil_data():
    try:
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Download crude oil data
        blob_client = blob_service_client.get_blob_client(container="raw-data", blob="crude-oil-price.csv")
        blob_data = blob_client.download_blob().readall()
        df = pd.read_csv(StringIO(blob_data.decode('utf-8')))
        
        print("📊 Original data shape:", df.shape)
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Filter for 2015-2024 (your project scope)
        df_filtered = df[(df['date'] >= '2015-01-01') & (df['date'] <= '2024-12-31')]
        
        print(f"🎯 Filtered data (2015-2024): {df_filtered.shape}")
        print(f"📅 Date range: {df_filtered['date'].min()} to {df_filtered['date'].max()}")
        
        # Basic statistics
        print(f"\n💰 Oil Price Statistics (2015-2024):")
        print(f"   Average: ${df_filtered['price'].mean():.2f}")
        print(f"   Min: ${df_filtered['price'].min():.2f}")
        print(f"   Max: ${df_filtered['price'].max():.2f}")
        
        # Save processed data back to Azure
        processed_csv = df_filtered.to_csv(index=False)
        processed_blob = blob_service_client.get_blob_client(
            container="raw-data", 
            blob="oil_prices_2015_2024.csv"
        )
        processed_blob.upload_blob(processed_csv, overwrite=True)
        
        print("✅ Processed data saved as 'oil_prices_2015_2024.csv'")
        
        return df_filtered
        
    except Exception as e:
        print(f"❌ Processing failed: {e}")

if __name__ == "__main__":
    process_oil_data()