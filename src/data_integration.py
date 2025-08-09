

import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import StringIO
import matplotlib.pyplot as plt

load_dotenv()

def integrate_all_datasets():
    try:
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        print("🔗 Integrating all datasets for oil price impact analysis...")
        
        # Load oil prices (2015-2024)
        print("📊 Loading oil price data...")
        oil_blob = blob_service_client.get_blob_client(container="raw-data", blob="oil_prices_2015_2024.csv")
        oil_data = oil_blob.download_blob().readall()
        df_oil = pd.read_csv(StringIO(oil_data.decode('utf-8')))
        df_oil['date'] = pd.to_datetime(df_oil['date'])
        df_oil['year'] = df_oil['date'].dt.year
        
        # Load UK fossil fuel consumption
        print("⛽ Loading fossil fuel consumption data...")
        fuel_blob = blob_service_client.get_blob_client(container="raw-data", blob="uk_fossil_fuel_consumption.csv")
        fuel_data = fuel_blob.download_blob().readall()
        df_fuel = pd.read_csv(StringIO(fuel_data.decode('utf-8')))
        
        # Load UK pollution data
        print("🏭 Loading pollution data...")
        pollution_blob = blob_service_client.get_blob_client(container="raw-data", blob="uk_pollution_sample.csv")
        pollution_data = pollution_blob.download_blob().readall()
        df_pollution = pd.read_csv(StringIO(pollution_data.decode('utf-8')))
        
        # Create annual summaries for integration
        print("🔄 Creating annual summaries...")
        
        # Oil prices - annual averages
        oil_annual = df_oil.groupby('year').agg({
            'price': ['mean', 'min', 'max', 'std']
        }).round(2)
        oil_annual.columns = ['oil_price_avg', 'oil_price_min', 'oil_price_max', 'oil_price_volatility']
        oil_annual = oil_annual.reset_index()
        
        # Combine all datasets
        print("🚀 Integrating datasets...")
        
        # Start with oil prices as base
        integrated_df = oil_annual.copy()
        
        # Add fossil fuel consumption
        integrated_df = integrated_df.merge(
            df_fuel[['year', 'fossil_fuel_consumption_percent']], 
            on='year', 
            how='left'
        )
        
        # Add pollution data
        integrated_df = integrated_df.merge(
            df_pollution[['year', 'co2_emissions_mt']], 
            on='year', 
            how='left'
        )
        
        print("✅ Integration complete!")
        print(f"📊 Integrated dataset shape: {integrated_df.shape}")
        print(f"📅 Years covered: {integrated_df['year'].min()} to {integrated_df['year'].max()}")
        
        print("\n🔍 Integrated dataset preview:")
        print(integrated_df.head())
        
        # Basic correlation analysis
        print("\n📈 Correlation Analysis:")
        correlations = integrated_df[['oil_price_avg', 'fossil_fuel_consumption_percent', 'co2_emissions_mt']].corr()
        print(correlations)
        
        # Save integrated dataset
        csv_data = integrated_df.to_csv(index=False)
        integrated_blob = blob_service_client.get_blob_client(
            container="raw-data",
            blob="integrated_uk_oil_analysis.csv"
        )
        integrated_blob.upload_blob(csv_data, overwrite=True)
        print(f"\n☁️ Saved integrated dataset: integrated_uk_oil_analysis.csv")
        
        # Key insights
        print("\n🎯 Key Insights for DataKirk Analysis:")
        print(f"   📊 Oil price volatility (2015-2024): ${integrated_df['oil_price_volatility'].mean():.2f} avg std dev")
        print(f"   ⛽ UK fossil fuel dependency: {integrated_df['fossil_fuel_consumption_percent'].mean():.1f}% average")
        print(f"   🏭 CO2 emissions trend: {integrated_df['co2_emissions_mt'].iloc[-1] - integrated_df['co2_emissions_mt'].iloc[0]:.1f} MT change")
        
        return integrated_df
        
    except Exception as e:
        print(f"❌ Integration failed: {e}")
        return None

if __name__ == "__main__":
    df = integrate_all_datasets()
    if df is not None:
        print("\n🎉 Data integration complete! Ready for BI analysis!")
        print("📊 Your integrated dataset is ready for Power BI or further analysis.")