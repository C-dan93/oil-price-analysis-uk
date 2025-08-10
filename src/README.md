# Production Data Pipeline Scripts

## 🚀 Main Pipeline (Run in Order)

### 1. Setup & Testing
- **azure_connection.py** - Test Azure storage connection

### 2. Automated Data Collection  
- **kaggle_oil_api.py** - Download oil prices from Kaggle API
- **worldbank_api.py** - Fetch UK fossil fuel data from World Bank API  
- **uk_weather_api.py** - Integrate UK weather data

### 3. Final Integration
- **complete_data_integration.py** - Integrate all 4 data sources

## 📊 Data Flow
## 🔧 Usage
1. Ensure Azure credentials in .env file
2. Run: python kaggle_oil_api.py
3. Run: python worldbank_api.py  
4. Run: python uk_weather_api.py
5. Process CO2 data manually
6. Run: python complete_data_integration.py
7. Analyze results in Azure storage

## 📈 Output
Final integrated dataset with oil prices, fossil fuel consumption, CO2 emissions, and weather correlations.
