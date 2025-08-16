import os
import sys
import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from io import StringIO
from datetime import datetime

# Setup logging for GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

class DataIntegrationPipeline:
    def __init__(self):
        self.connection_string = os.getenv('AZURE_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("❌ AZURE_CONNECTION_STRING not found in environment")
        
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_name = "raw-data"
        
    def check_blob_exists(self, blob_name):
        """Check if a blob exists in Azure storage"""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            blob_client.get_blob_properties()
            return True
        except Exception:
            return False
    
    def load_dataset(self, blob_name, required=True):
        """Load a dataset from Azure blob storage with error handling"""
        try:
            logger.info(f"📥 Loading {blob_name}...")
            
            if not self.check_blob_exists(blob_name):
                if required:
                    logger.error(f"❌ Required dataset {blob_name} not found in Azure storage")
                    return None
                else:
                    logger.warning(f"⚠️ Optional dataset {blob_name} not found, skipping")
                    return None
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            data = blob_client.download_blob().readall()
            df = pd.read_csv(StringIO(data.decode('utf-8')))
            
            logger.info(f"✅ Loaded {blob_name}: {len(df)} records")
            return df
            
        except Exception as e:
            if required:
                logger.error(f"❌ Failed to load required dataset {blob_name}: {e}")
                return None
            else:
                logger.warning(f"⚠️ Failed to load optional dataset {blob_name}: {e}")
                return None
    
    def save_dataset(self, df, blob_name):
        """Save dataset to Azure blob storage"""
        try:
            csv_data = df.to_csv(index=False)
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            blob_client.upload_blob(csv_data, overwrite=True)
            logger.info(f"☁️ Saved {blob_name} to Azure storage")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to save {blob_name}: {e}")
            return False
    
    def integrate_all_datasets(self):
        """Main integration method with comprehensive error handling"""
        try:
            logger.info("🔗 Starting comprehensive data integration...")
            
            # Define dataset configurations
            datasets = {
                'oil_prices': {
                    'blob': 'oil_prices_2015_2024_automated.csv',
                    'required': True,
                    'description': 'Oil prices (Kaggle API)'
                },
                'fossil_fuel': {
                    'blob': 'uk_fossil_fuel_consumption.csv',
                    'required': True,
                    'description': 'Fossil fuel consumption (World Bank API)'
                },
                'co2_emissions': {
                    'blob': 'uk_co2_emissions_real.csv',
                    'required': False,  # Made optional for testing
                    'description': 'CO2 emissions (UK Government)'
                },
                'weather': {
                    'blob': 'uk_weather_open_meteo_real.csv',
                    'required': True,
                    'description': 'Weather data (Open-Meteo API)'
                }
            }
            
            # Load all datasets
            loaded_data = {}
            for key, config in datasets.items():
                df = self.load_dataset(config['blob'], config['required'])
                if df is not None:
                    loaded_data[key] = df
                    logger.info(f"📊 {config['description']}: ✅ Ready")
                elif config['required']:
                    logger.error(f"💀 Critical dataset missing: {config['description']}")
                    return None
                else:
                    logger.warning(f"⚠️ Optional dataset missing: {config['description']}")
            
            # Check minimum requirements
            if len(loaded_data) < 2:
                logger.error("❌ Insufficient datasets for integration (need at least 2)")
                return None
            
            logger.info(f"✅ Successfully loaded {len(loaded_data)} datasets")
            
            # Process oil prices if available
            if 'oil_prices' in loaded_data:
                logger.info("🛢️ Processing oil price data...")
                df_oil = loaded_data['oil_prices'].copy()
                
                # Ensure date column exists and is properly formatted
                if 'date' in df_oil.columns:
                    df_oil['date'] = pd.to_datetime(df_oil['date'])
                    df_oil['year'] = df_oil['date'].dt.year
                elif 'year' not in df_oil.columns:
                    logger.error("❌ Oil data missing date/year information")
                    return None
                
                # Create annual summaries
                oil_annual = df_oil.groupby('year').agg({
                    'price': ['mean', 'min', 'max', 'std']
                }).round(2)
                oil_annual.columns = ['oil_price_avg', 'oil_price_min', 'oil_price_max', 'oil_price_volatility']
                oil_annual = oil_annual.reset_index()
                
                integrated_df = oil_annual.copy()
                logger.info(f"📊 Oil price base: {len(integrated_df)} years")
            else:
                logger.error("❌ Oil price data required as base dataset")
                return None
            
            # Add other datasets progressively
            for key, df in loaded_data.items():
                if key == 'oil_prices':
                    continue  # Already used as base
                
                logger.info(f"🔄 Integrating {key} data...")
                
                # Ensure year column exists
                if 'year' not in df.columns:
                    logger.warning(f"⚠️ {key} data missing year column, skipping")
                    continue
                
                # Define merge columns for each dataset type
                merge_columns = {
                    'fossil_fuel': ['year', 'fossil_fuel_consumption_percent'],
                    'co2_emissions': ['year', 'co2_emissions_mt'],
                    'weather': ['year', 'avg_temp_c', 'heating_degree_days', 'rainfall_mm']
                }
                
                # Get columns to merge (use all available if not predefined)
                if key in merge_columns:
                    cols_to_merge = [col for col in merge_columns[key] if col in df.columns]
                else:
                    # For unknown datasets, use year + all numeric columns
                    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                    cols_to_merge = ['year'] + [col for col in numeric_cols if col != 'year']
                
                if len(cols_to_merge) <= 1:
                    logger.warning(f"⚠️ No data columns found for {key}, skipping")
                    continue
                
                # Perform merge
                before_count = len(integrated_df)
                integrated_df = integrated_df.merge(
                    df[cols_to_merge], 
                    on='year', 
                    how='left'
                )
                
                logger.info(f"✅ Added {key}: {len(cols_to_merge)-1} columns, {len(integrated_df)} records")
            
            # Find analysis period
            year_range = sorted(integrated_df['year'].unique())
            logger.info(f"📅 Analysis period: {min(year_range)}-{max(year_range)} ({len(year_range)} years)")
            
            # Data quality check
            total_cells = integrated_df.shape[0] * integrated_df.shape[1]
            missing_cells = integrated_df.isnull().sum().sum()
            completeness = ((total_cells - missing_cells) / total_cells) * 100
            
            logger.info(f"📊 Dataset completeness: {completeness:.1f}%")
            
            if completeness < 50:
                logger.warning("⚠️ Low data completeness - check data sources")
            
            # Display integration summary
            logger.info("📋 Integration Summary:")
            logger.info("="*60)
            for _, row in integrated_df.iterrows():
                oil_price = f"${row['oil_price_avg']:.1f}" if pd.notna(row['oil_price_avg']) else "N/A"
                
                summary_parts = [f"{int(row['year'])}: Oil={oil_price}"]
                
                if 'fossil_fuel_consumption_percent' in row and pd.notna(row['fossil_fuel_consumption_percent']):
                    summary_parts.append(f"Fossil={row['fossil_fuel_consumption_percent']:.1f}%")
                
                if 'co2_emissions_mt' in row and pd.notna(row['co2_emissions_mt']):
                    summary_parts.append(f"CO2={row['co2_emissions_mt']:.1f}MT")
                
                if 'avg_temp_c' in row and pd.notna(row['avg_temp_c']):
                    summary_parts.append(f"Temp={row['avg_temp_c']:.1f}°C")
                
                logger.info("   " + ", ".join(summary_parts))
            
            # Save integrated dataset
            output_filename = f"complete_integrated_analysis_{datetime.now().strftime('%Y%m%d')}.csv"
            success = self.save_dataset(integrated_df, output_filename)
            
            if success:
                # Also save with standard name for consistency
                self.save_dataset(integrated_df, "complete_integrated_analysis_open_meteo.csv")
                logger.info("✅ Integration completed successfully!")
                return integrated_df
            else:
                logger.error("❌ Failed to save integrated dataset")
                return None
                
        except Exception as e:
            logger.error(f"❌ Integration failed: {e}")
            import traceback
            traceback.print_exc()
            return None

def main():
    """Main entry point for GitHub Actions"""
    try:
        logger.info("🚀 Starting automated data integration pipeline...")
        
        pipeline = DataIntegrationPipeline()
        result = pipeline.integrate_all_datasets()
        
        if result is not None:
            logger.info("🎉 DATA INTEGRATION PIPELINE SUCCESS!")
            logger.info(f"📊 Final dataset: {result.shape[0]} years, {result.shape[1]} variables")
            logger.info("☁️ Results saved to Azure Blob Storage")
            sys.exit(0)  # Success
        else:
            logger.error("💀 DATA INTEGRATION PIPELINE FAILED!")
            sys.exit(1)  # Failure
            
    except Exception as e:
        logger.error(f"💥 Pipeline crashed: {e}")
        sys.exit(1)  # Failure

if __name__ == "__main__":
    main()
