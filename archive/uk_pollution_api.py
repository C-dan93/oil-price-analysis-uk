import os
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import json

load_dotenv()

def fetch_uk_pollution_data():
    try:
        print("🏭 Fetching UK air pollution data from government API...")
        
        # UK government emissions data endpoint
        # Let's try to explore the API structure first
        base_url = "https://naei.energysecurity.gov.uk"
        
        # Try different potential endpoints
        endpoints_to_try = [
            "/api/data",
            "/data/csv", 
            "/api/emissions",
            "/data"
        ]
        
        for endpoint in endpoints_to_try:
            try:
                url = base_url + endpoint
                print(f"🔍 Trying: {url}")
                
                response = requests.get(url, timeout=10)
                print(f"   Status: {response.status_code}")
                
                if response.status_code == 200:
                    print(f"✅ Found working endpoint: {endpoint}")
                    
                    # Check content type
                    content_type = response.headers.get('content-type', '')
                    print(f"   Content type: {content_type}")
                    
                    if 'json' in content_type:
                        data = response.json()
                        print(f"   JSON data preview: {str(data)[:200]}...")
                    elif 'csv' in content_type:
                        print(f"   CSV data preview: {response.text[:200]}...")
                    else:
                        print(f"   Text preview: {response.text[:200]}...")
                    
                    break
                    
            except requests.exceptions.Timeout:
                print(f"   Timeout for {endpoint}")
            except Exception as e:
                print(f"   Error: {e}")
        
        # Alternative approach - try to find downloadable data files
        print("\n📥 Looking for direct data downloads...")
        
        # Common UK government data patterns
        data_urls = [
            "https://naei.energysecurity.gov.uk/data/data-selector-results?country=united-kingdom&pollutant=co2&sector=all&year=2015-2024",
            "https://naei.energysecurity.gov.uk/downloads/",
        ]
        
        for data_url in data_urls:
            try:
                print(f"🔍 Checking: {data_url}")
                response = requests.get(data_url, timeout=10)
                print(f"   Status: {response.status_code}")
                
                if response.status_code == 200:
                    print(f"✅ Found data source!")
                    # Process the data here
                    break
                    
            except Exception as e:
                print(f"   Error: {e}")
        
        # For now, let's create sample UK pollution data based on typical patterns
        print("\n📊 Creating sample UK pollution data (real API integration pending)...")
        
        sample_data = {
            'year': list(range(2015, 2025)),
            'co2_emissions_mt': [400.2, 390.5, 385.1, 380.6, 375.2, 365.8, 340.2, 355.7, 360.1, 358.9],
            'sector': ['Total'] * 10,
            'country': ['United Kingdom'] * 10
        }
        
        df = pd.DataFrame(sample_data)
        
        print(f"📊 Sample data shape: {df.shape}")
        print(f"📅 Years: {df['year'].min()} to {df['year'].max()}")
        print(f"\n🔍 Sample data:")
        print(df.head())
        
        # Upload to Azure
        upload_to_azure(df, "uk_pollution_sample.csv")
        
        print("\n⚠️ Note: This is sample data. Real API integration needs further exploration of the government website structure.")
        
        return df
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def upload_to_azure(df, filename):
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
    df = fetch_uk_pollution_data()
    if df is not None:
        print("🎉 UK pollution data collection complete!")