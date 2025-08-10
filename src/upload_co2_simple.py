import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

def process_and_upload_co2_data():
    """
    Processes the real UK CO2 emissions CSV file and uploads it to Azure.
    Extracts 2015-2022 data and calculates total annual emissions.
    """
    print("🚀 Starting real UK CO2 data processing...")
    
    try:
        print("📊 Processing real UK CO2 emissions data...")
        
        # Check file exists first
        csv_filename = 'carbon_dioxide_as_carbon_emission_summary_10-08-2025.csv'
        if not os.path.exists(csv_filename):
            print(f"❌ File not found: {csv_filename}")
            return None
            
        print(f"✅ File found: {csv_filename}")
        
        # Read it differently to handle the unusual structure
        print("📖 Reading file lines...")
        with open(csv_filename, 'r') as file:
            lines = file.readlines()
        
        print(f"✅ Read {len(lines)} lines from file")
        
        if len(lines) < 3:
            print("❌ File doesn't have enough lines")
            return None
            
        # Extract headers (years)
        print("🔍 Extracting headers...")
        headers = lines[1].strip().split(',')
        print(f"✅ Found {len(headers)} columns")
        print(f"First 10 headers: {headers[:10]}")
        
        # Find indices for 2015-2022
        try:
            year_2015_idx = headers.index('2015')
            year_2022_idx = headers.index('2022')
            project_years = headers[year_2015_idx:year_2022_idx + 1]
            print(f"✅ Found data for years: {project_years}")
        except ValueError as e:
            print(f"❌ Could not find years in headers: {e}")
            return None
        
        # Process each sector's data
        print("🔄 Processing sector data...")
        sectors_data = []
        total_emissions_by_year = [0] * len(project_years)
        
        processed_lines = 0
        for line_num, line in enumerate(lines[2:], start=3):  # Skip title and header rows
            if line.strip():
                try:
                    row = line.strip().split(',')
                    sector = row[0].replace('"', '')  # Remove quotes
                    
                    if len(row) < year_2022_idx + 1:
                        print(f"⚠️ Line {line_num}: Not enough columns for sector '{sector}'")
                        continue
                    
                    # Extract emissions for 2015-2022
                    sector_emissions = []
                    for i, year_idx in enumerate(range(year_2015_idx, year_2022_idx + 1)):
                        try:
                            emission_value = float(row[year_idx])
                            sector_emissions.append(emission_value)
                            total_emissions_by_year[i] += emission_value
                        except (ValueError, IndexError) as e:
                            print(f"⚠️ Error processing {sector} year index {year_idx}: {e}")
                            sector_emissions.append(0)
                    
                    sectors_data.append({
                        'sector': sector,
                        'emissions_2015_2022': sector_emissions
                    })
                    processed_lines += 1
                    
                except Exception as e:
                    print(f"❌ Error processing line {line_num}: {e}")
                    continue
        
        print(f"✅ Processed {processed_lines} sectors")
        
        # Create annual totals DataFrame
        print("📊 Creating annual totals...")
        annual_totals = []
        for i, year in enumerate(project_years):
            annual_totals.append({
                'year': int(year),
                'co2_emissions_kt': round(total_emissions_by_year[i], 1),
                'co2_emissions_mt': round(total_emissions_by_year[i] / 1000, 1),  # Convert to million tonnes
                'data_source': 'UK_Government_NAEI'
            })
        
        df_annual = pd.DataFrame(annual_totals)
        
        print(f"📈 Annual CO2 Emissions (Million Tonnes):")
        for _, row in df_annual.iterrows():
            print(f"   {row['year']}: {row['co2_emissions_mt']} MT")
        
        # Calculate trend
        start_emission = df_annual.iloc[0]['co2_emissions_mt']
        end_emission = df_annual.iloc[-1]['co2_emissions_mt']
        total_reduction = start_emission - end_emission
        percent_reduction = (total_reduction / start_emission) * 100
        
        print(f"\n🎯 Key Insights:")
        print(f"   📉 Total reduction (2015-2022): {total_reduction:.1f} MT ({percent_reduction:.1f}%)")
        covid_2020 = df_annual[df_annual['year'] == 2020]['co2_emissions_mt']
        if not covid_2020.empty:
            print(f"   🦠 COVID impact (2020): {covid_2020.iloc[0]} MT")
        
        # Upload to Azure
        print("☁️ Uploading to Azure...")
        upload_to_azure(df_annual, "uk_co2_emissions_real.csv")
        
        return df_annual
        
    except Exception as e:
        print(f"❌ Error processing data: {e}")
        import traceback
        traceback.print_exc()
        return None

def upload_to_azure(df, filename):
    """Upload DataFrame to Azure blob storage"""
    try:
        print(f"🔗 Connecting to Azure...")
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        
        if not connection_string:
            print("❌ No Azure connection string found in environment!")
            return
            
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Convert to CSV
        csv_data = df.to_csv(index=False)
        print(f"📄 Created CSV data ({len(csv_data)} characters)")
        
        # Upload
        blob_client = blob_service_client.get_blob_client(
            container="raw-data",
            blob=filename
        )
        
        blob_client.upload_blob(csv_data, overwrite=True)
        print(f"✅ Uploaded to Azure: {filename}")
        
    except Exception as e:
        print(f"❌ Azure upload failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    df = process_and_upload_co2_data()
    if df is not None:
        print("🎉 Real CO2 data successfully processed and uploaded!")
        print("📊 Ready to integrate with oil price and fossil fuel data!")
    else:
        print("❌ Failed to process CO2 data")