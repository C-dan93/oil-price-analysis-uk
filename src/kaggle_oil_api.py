import os
import kaggle
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import zipfile

load_dotenv()

def fetch_oil_data_kaggle_api():
    """
    Download oil price dataset from Kaggle using API
    Dataset: https://www.kaggle.com/datasets/sc231997/crude-oil-price
    """
    try:
        print("🛢️ Fetching oil price data from Kaggle API...")
        
        # Configure Kaggle API (reads from ~/.kaggle/kaggle.json)
        os.environ['KAGGLE_CONFIG_DIR'] = os.path.expanduser('~/.kaggle')
        
        # Download the specific dataset
        dataset_path = "sc231997/crude-oil-price"
        download_path = "./kaggle_data"
        
        print(f"📥 Downloading dataset: {dataset_path}")
        
        # Create download directory
        os.makedirs(download_path, exist_ok=True)
        
        # Download dataset
        kaggle.api.dataset_download_files(
            dataset_path, 
            path=download_path, 
            unzip=True
        )
        
        print("✅ Dataset downloaded successfully!")
        
        # Find the CSV file (it might be in a zip or directly accessible)
        csv_files = []
        for file in os.listdir(download_path):
            if file.endswith('.csv'):
                csv_files.append(file)
                
        print(f"📁 Found CSV files: {csv_files}")
        
        if not csv_files:
            print("❌ No CSV files found in download")
            return None
            
        # Read the main CSV file
        csv_file = csv_files[0]  # Take the first CSV found
        file_path = os.path.join(download_path, csv_file)
        
        print(f"📊 Reading: {csv_file}")
        df = pd.read_csv(file_path)
        
        print(f"✅ Loaded {len(df)} rows of oil price data")
        print(f"📅 Columns: {list(df.columns)}")
        print(f"📈 Date range: {df['date'].min()} to {df['date'].max()}")
        
        # Clean up downloaded files
        import shutil
        shutil.rmtree(download_path)
        print("🧹 Cleaned up temporary files")
        
        # Upload to Azure
        upload_to_azure(df, "oil_prices_kaggle_api.csv")
        
        return df
        
    except Exception as e:
        print(f"❌ Error fetching Kaggle data: {e}")
        print("💡 Make sure you have:")
        print("   1. Installed kaggle: pip install kaggle")
        print("   2. Downloaded kaggle.json from your account")
        print("   3. Placed it in: ~/.kaggle/kaggle.json")
        return None

def process_oil_data_automated(df):
    """
    Process the Kaggle oil data automatically (filter 2015-2024)
    """
    try:
        print("🔄 Processing oil data for project timeframe...")
        
        # Convert date column
        df['date'] = pd.to_datetime(df['date'])
        
        # Filter for 2015-2024
        df_filtered = df[(df['date'] >= '2015-01-01') & (df['date'] <= '2024-12-31')]
        
        print(f"✅ Filtered to {len(df_filtered)} rows (2015-2024)")
        print(f"💰 Price range: ${df_filtered['price'].min():.2f} to ${df_filtered['price'].max():.2f}")
        
        # Upload processed data
        upload_to_azure(df_filtered, "oil_prices_2015_2024_automated.csv")
        
        return df_filtered
        
    except Exception as e:
        print(f"❌ Error processing data: {e}")
        return None

def upload_to_azure(df, filename):
    """Upload DataFrame to Azure"""
    try:
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        csv_data = df.to_csv(index=False)
        
        blob_client = blob_service_client.get_blob_client(
            container="raw-data",
            blob=filename
        )
        
        blob_client.upload_blob(csv_data, overwrite=True)
        print(f"☁️ Uploaded to Azure: {filename}")
        
    except Exception as e:
        print(f"❌ Azure upload failed: {e}")

if __name__ == "__main__":
    print("🚀 Starting automated Kaggle oil price collection...")
    
    # Download raw data
    df_raw = fetch_oil_data_kaggle_api()
    
    if df_raw is not None:
        # Process for project timeframe
        df_processed = process_oil_data_automated(df_raw)
        
        if df_processed is not None:
            print("🎉 Kaggle oil price automation complete!")
            print("📊 Ready for integration with other datasets!")
        else:
            print("❌ Failed to process oil data")
    else:
        print("❌ Failed to download from Kaggle")
# Test run
