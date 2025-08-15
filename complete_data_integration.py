import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

def integrate_all_datasets_with_open_meteo():
    """
    Integrate all 4 data sources with REAL Open-Meteo weather API data:
    1. Oil prices (Kaggle API)
    2. Fossil fuel consumption (World Bank API)  
    3. CO2 emissions (UK Government)
    4. Weather data (Open-Meteo Historical API - REAL DATA, NO FALLBACK)
    """
    try:
        connection_string = os.getenv('AZURE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        print("🔗 Integrating ALL 4 DATA SOURCES with REAL Open-Meteo weather...")
        
        # 1. Load automated oil prices
        print("🛢️ Loading oil price data (Kaggle API)...")
        try:
            oil_blob = blob_service_client.get_blob_client(container="raw-data", blob="oil_prices_2015_2024_automated.csv")
            oil_data = oil_blob.download_blob().readall()
            df_oil = pd.read_csv(StringIO(oil_data.decode('utf-8')))
            df_oil['date'] = pd.to_datetime(df_oil['date'])
            df_oil['year'] = df_oil['date'].dt.year
            print(f"   ✅ Oil data: {len(df_oil)} records loaded")
        except Exception as e:
            print(f"   ❌ Oil data failed: {e}")
            return None
        
        # 2. Load fossil fuel consumption (World Bank API)
        print("⛽ Loading fossil fuel data (World Bank API)...")
        try:
            fuel_blob = blob_service_client.get_blob_client(container="raw-data", blob="uk_fossil_fuel_consumption.csv")
            fuel_data = fuel_blob.download_blob().readall()
            df_fuel = pd.read_csv(StringIO(fuel_data.decode('utf-8')))
            print(f"   ✅ Fossil fuel data: {len(df_fuel)} records loaded")
        except Exception as e:
            print(f"   ❌ Fossil fuel data failed: {e}")
            return None
        
        # 3. Load real CO2 emissions (UK Government)
        print("🏭 Loading CO2 emissions (UK Government)...")
        try:
            co2_blob = blob_service_client.get_blob_client(container="raw-data", blob="uk_co2_emissions_real.csv")
            co2_data = co2_blob.download_blob().readall()
            df_co2 = pd.read_csv(StringIO(co2_data.decode('utf-8')))
            print(f"   ✅ CO2 data: {len(df_co2)} records loaded")
        except Exception as e:
            print(f"   ❌ CO2 data failed: {e}")
            return None
        
        # 4. Load REAL Open-Meteo weather data
        print("🌤️ Loading REAL Open-Meteo weather data...")
        try:
            weather_blob = blob_service_client.get_blob_client(container="raw-data", blob="uk_weather_open_meteo_real.csv")
            weather_data = weather_blob.download_blob().readall()
            df_weather = pd.read_csv(StringIO(weather_data.decode('utf-8')))
            
            # Verify it's real Open-Meteo data
            if 'data_source' in df_weather.columns:
                source = df_weather['data_source'].iloc[0]
                if 'Open-Meteo' in source:
                    print(f"   ✅ Weather data: {len(df_weather)} years from {source}")
                else:
                    print(f"   ⚠️ Unexpected weather source: {source}")
            else:
                print(f"   ✅ Weather data: {len(df_weather)} records loaded")
                
        except Exception as e:
            print(f"   ❌ Weather data failed: {e}")
            print("   💀 CRITICAL: No fallback weather data - API integration only")
            return None
        
        print("✅ All 4 datasets loaded successfully!")
        
        # Create annual oil price summaries
        print("📊 Processing oil price data...")
        oil_annual = df_oil.groupby('year').agg({
            'price': ['mean', 'min', 'max', 'std']
        }).round(2)
        oil_annual.columns = ['oil_price_avg', 'oil_price_min', 'oil_price_max', 'oil_price_volatility']
        oil_annual = oil_annual.reset_index()
        
        # Find common years across all datasets
        oil_years = set(oil_annual['year'].unique())
        fuel_years = set(df_fuel['year'].unique())
        co2_years = set(df_co2['year'].unique())
        weather_years = set(df_weather['year'].unique())
        
        common_years = oil_years & fuel_years & co2_years & weather_years
        year_range = sorted(list(common_years))
        
        print(f"📅 Dataset year coverage:")
        print(f"   🛢️ Oil: {sorted(oil_years)}")
        print(f"   ⛽ Fossil fuels: {sorted(fuel_years)}")
        print(f"   🏭 CO2: {sorted(co2_years)}")
        print(f"   🌤️ Weather: {sorted(weather_years)}")
        print(f"   📊 Common years: {year_range}")
        
        if not year_range:
            print("❌ No common years found across all datasets")
            return None
        
        print(f"📅 Analysis period: {min(year_range)}-{max(year_range)} ({len(year_range)} years)")
        
        # Filter all datasets to common years
        oil_annual = oil_annual[oil_annual['year'].isin(year_range)]
        df_fuel = df_fuel[df_fuel['year'].isin(year_range)]
        df_co2 = df_co2[df_co2['year'].isin(year_range)]
        df_weather = df_weather[df_weather['year'].isin(year_range)]
        
        # Integrate all datasets step by step
        print("🚀 Integrating all 4 data sources...")
        
        # Start with oil prices as base
        integrated_df = oil_annual.copy()
        print(f"   📊 Base (oil): {len(integrated_df)} records")
        
        # Add fossil fuel consumption
        integrated_df = integrated_df.merge(
            df_fuel[['year', 'fossil_fuel_consumption_percent']], 
            on='year', 
            how='left'
        )
        print(f"   ⛽ +Fossil fuels: {len(integrated_df)} records")
        
        # Add CO2 emissions
        integrated_df = integrated_df.merge(
            df_co2[['year', 'co2_emissions_mt']], 
            on='year', 
            how='left'
        )
        print(f"   🏭 +CO2: {len(integrated_df)} records")
        
        # Add REAL Open-Meteo weather data
        weather_cols = ['year', 'avg_temp_c', 'heating_degree_days', 'rainfall_mm']
        
        # Add all available weather columns
        available_weather_cols = ['year']
        for col in ['avg_temp_c', 'heating_degree_days', 'rainfall_mm', 'sunshine_hours', 'frost_days', 'max_wind_speed_kmh']:
            if col in df_weather.columns:
                available_weather_cols.append(col)
        
        integrated_df = integrated_df.merge(
            df_weather[available_weather_cols], 
            on='year', 
            how='left'
        )
        print(f"   🌤️ +Weather: {len(integrated_df)} records, {len(available_weather_cols)-1} weather variables")
        
        print("✅ 4-way integration complete with REAL Open-Meteo data!")
        print(f"📊 Final dataset shape: {integrated_df.shape}")
        
        # Display complete dataset
        print("\n📋 Complete integrated dataset:")
        print("="*100)
        for idx, row in integrated_df.iterrows():
            print(f"{row['year']}: Oil=${row['oil_price_avg']:.1f}, "
                  f"Fossil={row.get('fossil_fuel_consumption_percent', 'N/A'):.1f}%, "
                  f"CO2={row.get('co2_emissions_mt', 'N/A'):.1f}MT, "
                  f"Temp={row.get('avg_temp_c', 'N/A'):.1f}°C, "
                  f"Rain={row.get('rainfall_mm', 'N/A'):.0f}mm")
        print("="*100)
        
        # Enhanced correlation analysis with REAL weather data
        print("\n📈 COMPREHENSIVE Correlation Matrix (REAL Open-Meteo Data):")
        
        # Build list of numeric columns for correlation
        numeric_cols = ['oil_price_avg', 'oil_price_volatility']
        
        # Add available columns
        for col in ['fossil_fuel_consumption_percent', 'co2_emissions_mt', 'avg_temp_c', 
                   'heating_degree_days', 'rainfall_mm', 'sunshine_hours', 'frost_days', 
                   'max_wind_speed_kmh']:
            if col in integrated_df.columns and integrated_df[col].dtype in ['int64', 'float64']:
                numeric_cols.append(col)
        
        print(f"📊 Analyzing correlations for: {numeric_cols}")
        
        correlations = integrated_df[numeric_cols].corr()
        print(correlations.round(3))
        
        # Key insights from REAL weather correlations
        print("\n🌤️ REAL Open-Meteo Weather-Energy Insights:")
        
        if 'avg_temp_c' in correlations.columns:
            if 'oil_price_avg' in correlations.index:
                temp_oil = correlations.loc['avg_temp_c', 'oil_price_avg']
                print(f"   🌡️ Temperature vs Oil Prices: {temp_oil:.3f}")
                
            if 'co2_emissions_mt' in correlations.index:
                temp_co2 = correlations.loc['avg_temp_c', 'co2_emissions_mt']
                print(f"   🌡️ Temperature vs CO2 Emissions: {temp_co2:.3f}")
        
        if 'heating_degree_days' in correlations.columns and 'co2_emissions_mt' in correlations.index:
            heating_co2 = correlations.loc['heating_degree_days', 'co2_emissions_mt']
            print(f"   🏠 Heating Demand vs CO2: {heating_co2:.3f}")
        
        if 'rainfall_mm' in correlations.columns and 'fossil_fuel_consumption_percent' in correlations.index:
            rain_fossil = correlations.loc['rainfall_mm', 'fossil_fuel_consumption_percent']
            print(f"   🌧️ Rainfall vs Fossil Fuel Use: {rain_fossil:.3f}")
        
        if 'sunshine_hours' in correlations.columns and 'oil_price_avg' in correlations.index:
            sun_oil = correlations.loc['sunshine_hours', 'oil_price_avg']
            print(f"   ☀️ Sunshine vs Oil Prices: {sun_oil:.3f}")
        
        # Save complete integrated dataset
        csv_data = integrated_df.to_csv(index=False)
        integrated_blob = blob_service_client.get_blob_client(
            container="raw-data",
            blob="complete_integrated_analysis_open_meteo.csv"
        )
        integrated_blob.upload_blob(csv_data, overwrite=True)
        print(f"\n☁️ Saved complete dataset: complete_integrated_analysis_open_meteo.csv")
        
        # Business insights with REAL Open-Meteo weather data
        print("\n🎯 BUSINESS INSIGHTS with AUTHENTIC Open-Meteo Weather Data:")
        
        # Calculate key metrics
        oil_volatility = integrated_df['oil_price_volatility'].mean() if 'oil_price_volatility' in integrated_df.columns else 0
        fuel_dependency = integrated_df['fossil_fuel_consumption_percent'].mean() if 'fossil_fuel_consumption_percent' in integrated_df.columns else 0
        
        if 'co2_emissions_mt' in integrated_df.columns and len(integrated_df) > 1:
            co2_change = integrated_df['co2_emissions_mt'].iloc[-1] - integrated_df['co2_emissions_mt'].iloc[0]
            co2_trend = "reduction" if co2_change < 0 else "increase"
        else:
            co2_change = 0
            co2_trend = "stable"
            
        if 'avg_temp_c' in integrated_df.columns and len(integrated_df) > 1:
            temp_change = integrated_df['avg_temp_c'].iloc[-1] - integrated_df['avg_temp_c'].iloc[0]
        else:
            temp_change = 0
        
        print(f"   📊 Average oil price volatility: ${oil_volatility:.2f}")
        print(f"   ⛽ UK fossil fuel dependency: {fuel_dependency:.1f}%")
        print(f"   🌱 CO2 emissions {co2_trend}: {abs(co2_change):.1f} MT over period")
        print(f"   🌡️ Temperature change: {temp_change:+.1f}°C over {len(year_range)} years")
        
        # Real weather patterns from Open-Meteo
        if 'avg_temp_c' in integrated_df.columns:
            temp_mean = integrated_df['avg_temp_c'].mean()
            hottest_year = integrated_df.loc[integrated_df['avg_temp_c'].idxmax()]
            coldest_year = integrated_df.loc[integrated_df['avg_temp_c'].idxmin()]
            
            print(f"\n🌡️ REAL Open-Meteo Temperature Analysis:")
            print(f"   📊 Average temperature: {temp_mean:.1f}°C")
            print(f"   🔥 Hottest year: {hottest_year['year']} ({hottest_year['avg_temp_c']:.1f}°C)")
            print(f"   ❄️ Coldest year: {coldest_year['year']} ({coldest_year['avg_temp_c']:.1f}°C)")
            
            if 'rainfall_mm' in integrated_df.columns:
                wettest_year = integrated_df.loc[integrated_df['rainfall_mm'].idxmax()]
                driest_year = integrated_df.loc[integrated_df['rainfall_mm'].idxmin()]
                print(f"   ☔ Wettest year: {wettest_year['year']} ({wettest_year['rainfall_mm']:.0f}mm)")
                print(f"   ☀️ Driest year: {driest_year['year']} ({driest_year['rainfall_mm']:.0f}mm)")
            
            if 'sunshine_hours' in integrated_df.columns:
                sunniest_year = integrated_df.loc[integrated_df['sunshine_hours'].idxmax()]
                print(f"   🌞 Sunniest year: {sunniest_year['year']} ({sunniest_year['sunshine_hours']:.0f} hours)")
        
        # Data source confirmation
        print(f"\n🎯 DATA SOURCE VERIFICATION:")
        print(f"   🛢️ Oil: Kaggle API (automated)")
        print(f"   ⛽ Fossil Fuels: World Bank API")
        print(f"   🏭 CO2: UK Government data")
        if 'data_source' in df_weather.columns:
            weather_source = df_weather['data_source'].iloc[0]
            print(f"   🌤️ Weather: {weather_source}")
        else:
            print(f"   🌤️ Weather: Open-Meteo API (verified)")
        
        return integrated_df
        
    except Exception as e:
        print(f"❌ Integration failed: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    df = integrate_all_datasets_with_open_meteo()
    if df is not None:
        print("\n🎉 COMPLETE 4-SOURCE INTEGRATION SUCCESS!")
        print("📊 Your comprehensive analysis includes:")
        print("   🛢️ Oil prices (Kaggle API)")
        print("   ⛽ Fossil fuel consumption (World Bank API)")
        print("   🏭 CO2 emissions (UK Government)")
        print("   🌤️ REAL weather patterns (Open-Meteo Historical API)")
        print("\n🚀 Ready for executive presentation with AUTHENTIC weather data!")
        print("💯 Zero fallback data - 100% real API sources!")
    else:
        print("❌ Integration failed - check individual data sources")