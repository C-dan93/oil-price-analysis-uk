import os
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import time

load_dotenv()

def fetch_uk_fossil_fuel_data():
    try:
        print("🌍 Fetching UK fossil fuel consumption from World Bank API...")
        
        # World Bank API endpoint for UK fossil fuel consumption
        # Indicator: EG.USE.COMM.FO.ZS (Fossil fuel energy consumption % of total)
        # Country: GB (United Kingdom)
        # Date range: 2015-2024
        
        url = "https://api.worldbank.org/v2/country/GB/indicator/EG.USE.COMM.FO.ZS"
        params = {
            'date': '2015:2024',  # Your project timeframe
            'format': 'json',
            'per_page': 50
        }
        
        response = requests.get(url, params=params)
        print(f"📡 API Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            # World Bank API returns [metadata, data] format
            if len(data) > 1 and data[1]:  # Check if data exists
                records = data[1]  # Actual data is in second element
                
                # Convert to pandas DataFrame
                df_data = []
                for record in records:
                    if record['value'] is not None:  # Skip null values
                        df_data.append({
                            'year': record['date'],
                            'country': record['country']['value'],
                            'fossil_fuel_consumption_percent': record['value'],
                            'indicator_name': record['indicator']['value']
                        })
                
                df = pd.DataFrame(df_data)
                df = df.sort_values('year')  # Sort by year
                
                print(f"✅ Data retrieved successfully!")
                print(f"📊 Shape: {df.shape}")
                print(f"📅 Years: {df['year'].min()} to {df['year'].max()}")
                print(f"📈 Fossil fuel consumption range: {df['fossil_fuel_consumption_percent'].min():.1f}% to {df['fossil_fuel_consumption_percent'].max():.1f}%")
                
                print(f"\n🔍 Sample data:")
                print(df.head())
                
                # Upload to Azure
                upload_to_azure(df, "uk_fossil_fuel_consumption.csv")
                
                return df
            else:
                print("❌ No data returned from API")
                return None
                
        else:
            print(f"❌ API request failed: {response.status_code}")
            print(response.text)
            return None
            
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return None

def upload_to_azure(df, filename):
    try:
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Convert DataFrame to CSV
        csv_data = df.to_csv(index=False)
        
        # Upload to Azure
        blob_client = blob_service_client.get_blob_client(
            container="raw-data",
            blob=filename
        )
        
        blob_client.upload_blob(csv_data, overwrite=True)
        print(f"☁️ Uploaded to Azure: {filename}")
        
    except Exception as e:
        print(f"❌ Azure upload failed: {e}")

if __name__ == "__main__":
    df = fetch_uk_fossil_fuel_data()
    if df is not None:
        print("🎉 World Bank data collection complete!")
    else:
        print("❌ Failed to collect World Bank data")