import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

def integrate_all_real_datasets():
    try:
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        print("🔗 Integrating ALL REAL datasets for oil price impact analysis...")
        
        # Load oil prices (2015-2024)
        print("📊 Loading oil price data...")
        oil_blob = blob_service_client.get_blob_client(container="raw-data", blob="oil_prices_2015_2024.csv")
        oil_data = oil_blob.download_blob().readall()
        df_oil = pd.read_csv(StringIO(oil_data.decode('utf-8')))
        df_oil['date'] = pd.to_datetime(df_oil['date'])
        df_oil['year'] = df_oil['date'].dt.year
        
        # Load UK fossil fuel consumption (REAL World Bank data)
        print("⛽ Loading fossil fuel consumption data...")
        fuel_blob = blob_service_client.get_blob_client(container="raw-data", blob="uk_fossil_fuel_consumption.csv")
        fuel_data = fuel_blob.download_blob().readall()
        df_fuel = pd.read_csv(StringIO(fuel_data.decode('utf-8')))
        
        # Load REAL UK CO2 emissions data (just uploaded!)
        print("🏭 Loading REAL UK CO2 emissions data...")
        co2_blob = blob_service_client.get_blob_client(container="raw-data", blob="uk_co2_emissions_real.csv")
        co2_data = co2_blob.download_blob().readall()
        df_co2 = pd.read_csv(StringIO(co2_data.decode('utf-8')))
        
        print("✅ All REAL datasets loaded!")
        
        # Create annual oil price summaries
        oil_annual = df_oil.groupby('year').agg({
            'price': ['mean', 'min', 'max', 'std']
        }).round(2)
        oil_annual.columns = ['oil_price_avg', 'oil_price_min', 'oil_price_max', 'oil_price_volatility']
        oil_annual = oil_annual.reset_index()
        
        # Filter for overlapping years (2015-2022 for CO2 data)
        oil_annual = oil_annual[(oil_annual['year'] >= 2015) & (oil_annual['year'] <= 2022)]
        df_fuel = df_fuel[(df_fuel['year'] >= 2015) & (df_fuel['year'] <= 2022)]
        
        print(f"📅 Analysis period: 2015-2022 (8 years)")
        
        # Integrate all datasets
        print("🚀 Integrating all REAL datasets...")
        
        # Start with oil prices
        integrated_df = oil_annual.copy()
        
        # Add fossil fuel consumption
        integrated_df = integrated_df.merge(
            df_fuel[['year', 'fossil_fuel_consumption_percent']], 
            on='year', 
            how='left'
        )
        
        # Add REAL CO2 emissions
        integrated_df = integrated_df.merge(
            df_co2[['year', 'co2_emissions_mt']], 
            on='year', 
            how='left'
        )
        
        print("✅ Integration complete!")
        print(f"📊 Final dataset shape: {integrated_df.shape}")
        
        print("\n🔍 Integrated REAL dataset preview:")
        print(integrated_df)
        
        # Enhanced correlation analysis with REAL data
        print("\n📈 REAL Data Correlation Analysis:")
        numeric_cols = ['oil_price_avg', 'fossil_fuel_consumption_percent', 'co2_emissions_mt']
        correlations = integrated_df[numeric_cols].corr()
        print(correlations)
        
        # Save integrated REAL dataset
        csv_data = integrated_df.to_csv(index=False)
        integrated_blob = blob_service_client.get_blob_client(
            container="raw-data",
            blob="integrated_uk_oil_analysis_REAL.csv"
        )
        integrated_blob.upload_blob(csv_data, overwrite=True)
        print(f"\n☁️ Saved REAL integrated dataset: integrated_uk_oil_analysis_REAL.csv")
        
        # REAL Data Business Insights
        print("\n🎯 REAL Data Insights for DataKirk:")
        oil_volatility = integrated_df['oil_price_volatility'].mean()
        fuel_consumption = integrated_df['fossil_fuel_consumption_percent'].mean()
        co2_reduction = integrated_df['co2_emissions_mt'].iloc[0] - integrated_df['co2_emissions_mt'].iloc[-1]
        
        print(f"   📊 Oil price volatility (2015-2022): ${oil_volatility:.2f} avg std dev")
        print(f"   ⛽ UK fossil fuel dependency: {fuel_consumption:.1f}% average")
        print(f"   🌱 CO2 emissions reduction: {co2_reduction:.1f} MT (REAL government data)")
        print(f"   🎯 Correlation oil prices vs CO2: {correlations.loc['oil_price_avg', 'co2_emissions_mt']:.3f}")
        
        return integrated_df
        
    except Exception as e:
        print(f"❌ Integration failed: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    df = integrate_all_real_datasets()
    if df is not None:
        print("\n🎉 REAL DATA INTEGRATION COMPLETE!")
        print("📊 Your analysis now uses 100% real data from official sources!")
        print("🚀 Ready for executive presentation and Power BI dashboard!")