import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

def integrate_all_four_datasets():
    """
    Integrate all 4 data sources:
    1. Oil prices (Kaggle API)
    2. Fossil fuel consumption (World Bank API)  
    3. CO2 emissions (UK Government)
    4. Weather data (Enhanced sample)
    """
    try:
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        print("🔗 Integrating ALL 4 DATA SOURCES...")
        
        # 1. Load automated oil prices
        print("🛢️ Loading oil price data (Kaggle API)...")
        oil_blob = blob_service_client.get_blob_client(container="raw-data", blob="oil_prices_2015_2024_automated.csv")
        oil_data = oil_blob.download_blob().readall()
        df_oil = pd.read_csv(StringIO(oil_data.decode('utf-8')))
        df_oil['date'] = pd.to_datetime(df_oil['date'])
        df_oil['year'] = df_oil['date'].dt.year
        
        # 2. Load fossil fuel consumption (World Bank API)
        print("⛽ Loading fossil fuel data (World Bank API)...")
        fuel_blob = blob_service_client.get_blob_client(container="raw-data", blob="uk_fossil_fuel_consumption.csv")
        fuel_data = fuel_blob.download_blob().readall()
        df_fuel = pd.read_csv(StringIO(fuel_data.decode('utf-8')))
        
        # 3. Load real CO2 emissions (UK Government)
        print("🏭 Loading CO2 emissions (UK Government)...")
        co2_blob = blob_service_client.get_blob_client(container="raw-data", blob="uk_co2_emissions_real.csv")
        co2_data = co2_blob.download_blob().readall()
        df_co2 = pd.read_csv(StringIO(co2_data.decode('utf-8')))
        
        # 4. Load weather data
        print("🌤️ Loading weather data...")
        weather_blob = blob_service_client.get_blob_client(container="raw-data", blob="uk_weather_real.csv")
        weather_data = weather_blob.download_blob().readall()
        df_weather = pd.read_csv(StringIO(weather_data.decode('utf-8')))
        
        print("✅ All 4 datasets loaded successfully!")
        
        # Create annual oil price summaries
        print("📊 Processing oil price data...")
        oil_annual = df_oil.groupby('year').agg({
            'price': ['mean', 'min', 'max', 'std']
        }).round(2)
        oil_annual.columns = ['oil_price_avg', 'oil_price_min', 'oil_price_max', 'oil_price_volatility']
        oil_annual = oil_annual.reset_index()
        
        # Filter all datasets to 2015-2022 (common period)
        oil_annual = oil_annual[(oil_annual['year'] >= 2015) & (oil_annual['year'] <= 2022)]
        df_fuel = df_fuel[(df_fuel['year'] >= 2015) & (df_fuel['year'] <= 2022)]
        df_co2 = df_co2[(df_co2['year'] >= 2015) & (df_co2['year'] <= 2022)]
        df_weather = df_weather[(df_weather['year'] >= 2015) & (df_weather['year'] <= 2022)]
        
        print(f"📅 Analysis period: 2015-2022 ({len(oil_annual)} years)")
        
        # Integrate all datasets step by step
        print("🚀 Integrating all 4 data sources...")
        
        # Start with oil prices as base
        integrated_df = oil_annual.copy()
        
        # Add fossil fuel consumption
        integrated_df = integrated_df.merge(
            df_fuel[['year', 'fossil_fuel_consumption_percent']], 
            on='year', 
            how='left'
        )
        
        # Add CO2 emissions
        integrated_df = integrated_df.merge(
            df_co2[['year', 'co2_emissions_mt']], 
            on='year', 
            how='left'
        )
        
        # Add weather data
        integrated_df = integrated_df.merge(
            df_weather[['year', 'avg_temp_c', 'heating_degree_days', 'rainfall_mm']], 
            on='year', 
            how='left'
        )
        
        print("✅ 4-way integration complete!")
        print(f"📊 Final dataset shape: {integrated_df.shape}")
        
        print("\n🔍 Complete integrated dataset:")
        print(integrated_df)
        
        # Enhanced correlation analysis with weather
        print("\n📈 COMPREHENSIVE Correlation Matrix:")
        numeric_cols = [
            'oil_price_avg', 'oil_price_volatility', 'fossil_fuel_consumption_percent', 
            'co2_emissions_mt', 'avg_temp_c', 'heating_degree_days', 'rainfall_mm'
        ]
        correlations = integrated_df[numeric_cols].corr()
        print(correlations.round(3))
        
        # Key weather correlations
        print("\n🌤️ Weather-Energy Correlations:")
        temp_oil_corr = correlations.loc['avg_temp_c', 'oil_price_avg']
        temp_co2_corr = correlations.loc['avg_temp_c', 'co2_emissions_mt']
        heating_co2_corr = correlations.loc['heating_degree_days', 'co2_emissions_mt']
        
        print(f"   🌡️ Temperature vs Oil Prices: {temp_oil_corr:.3f}")
        print(f"   🌡️ Temperature vs CO2 Emissions: {temp_co2_corr:.3f}")
        print(f"   🏠 Heating Demand vs CO2 Emissions: {heating_co2_corr:.3f}")
        
        # Save complete integrated dataset
        csv_data = integrated_df.to_csv(index=False)
        integrated_blob = blob_service_client.get_blob_client(
            container="raw-data",
            blob="complete_integrated_analysis_4_sources.csv"
        )
        integrated_blob.upload_blob(csv_data, overwrite=True)
        print(f"\n☁️ Saved complete dataset: complete_integrated_analysis_4_sources.csv")
        
        # Comprehensive business insights
        print("\n🎯 COMPREHENSIVE DataKirk Business Insights:")
        
        oil_volatility = integrated_df['oil_price_volatility'].mean()
        fuel_dependency = integrated_df['fossil_fuel_consumption_percent'].mean()
        co2_reduction = integrated_df['co2_emissions_mt'].iloc[0] - integrated_df['co2_emissions_mt'].iloc[-1]
        temp_increase = integrated_df['avg_temp_c'].iloc[-1] - integrated_df['avg_temp_c'].iloc[0]
        
        print(f"   📊 Oil price volatility: ${oil_volatility:.2f} avg std dev")
        print(f"   ⛽ UK fossil fuel dependency: {fuel_dependency:.1f}% average")
        print(f"   🌱 CO2 emissions reduction: {co2_reduction:.1f} MT")
        print(f"   🌡️ Temperature increase: {temp_increase:.1f}°C over 8 years")
        print(f"   🔗 Oil-CO2 correlation: {correlations.loc['oil_price_avg', 'co2_emissions_mt']:.3f}")
        print(f"   🌤️ Weather-Energy link: {heating_co2_corr:.3f}")
        
        # Weather insights
        hot_years = integrated_df[integrated_df['avg_temp_c'] > 10.5]['year'].tolist()
        cold_years = integrated_df[integrated_df['avg_temp_c'] < 10.0]['year'].tolist()
        
        print(f"\n🌡️ Climate Impact Analysis:")
        print(f"   🔥 Hot years ({hot_years}): Lower heating demand")
        print(f"   ❄️ Cold years ({cold_years}): Higher energy consumption")
        print(f"   ☔ Wettest year: {integrated_df.loc[integrated_df['rainfall_mm'].idxmax(), 'year']} ({integrated_df['rainfall_mm'].max():.0f}mm)")
        print(f"   ☀️ Driest year: {integrated_df.loc[integrated_df['rainfall_mm'].idxmin(), 'year']} ({integrated_df['rainfall_mm'].min():.0f}mm)")
        
        return integrated_df
        
    except Exception as e:
        print(f"❌ Integration failed: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    df = integrate_all_four_datasets()
    if df is not None:
        print("\n🎉 COMPLETE 4-SOURCE INTEGRATION SUCCESS!")
        print("📊 Your analysis now includes:")
        print("   🛢️ Oil prices (Kaggle API)")
        print("   ⛽ Fossil fuel consumption (World Bank API)")
        print("   🏭 CO2 emissions (UK Government)")
        print("   🌤️ Weather patterns (Enhanced sample)")
        print("\n🚀 Ready for comprehensive executive presentation!")
    else:
        print("❌ Integration failed")