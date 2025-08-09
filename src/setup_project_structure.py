import os
import shutil

def setup_project_structure():
    """Create proper data engineering project structure"""
    
    # Define project structure
    directories = [
        'src',           # Source code
        'data',          # Local data samples
        'notebooks',     # Jupyter notebooks
        'docs',          # Documentation
        'config',        # Configuration files
        'tests'          # Unit tests
    ]
    
    # Create directories
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created: {directory}/")
    
    # Create initial files
    files_to_create = {
        'src/__init__.py': '# Source code package',
        'src/azure_connection.py': '# Azure connection utilities',
        'src/data_processing.py': '# Data processing functions',
        'src/api_connectors.py': '# API connection modules',
        'README.md': '''# Oil Price Analysis - UK Impact Study

## Project Overview
Data engineering pipeline analyzing oil price impact on UK consumption, behavior, and CO2 emissions (2015-2024).

## Data Sources
- Crude Oil Prices (Kaggle)
- UK Air Pollution Data (Government API)
- UK Fossil Fuel Consumption (World Bank API)

## Technology Stack
- **Cloud Platform**: Microsoft Azure
- **Programming**: Python 3.13
- **Storage**: Azure Blob Storage
- **Visualization**: Power BI
- **Version Control**: Git/GitHub

## Team Structure
- Project Leads (2)
- Data Engineers (2) 
- Data Cleaners (2)
- Documentation Handlers (2)

## Project Status
Phase 1 Complete: Data collection and integration
Next: Power BI dashboard and analysis
''',
        '.env.example': '''# Copy this to .env and add your actual credentials
AZURE_CONNECTION_STRING=your_connection_string_here
''',
        'requirements.txt': '''azure-storage-blob==12.19.0
pandas==2.3.1
requests==2.31.0
python-dotenv==1.0.0
jupyter==1.0.0
matplotlib==3.8.0
'''
    }
    
    for file_path, content in files_to_create.items():
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Created: {file_path}")
    
    print("\nProject structure setup complete!")
    print("Your repository is now properly organized for team collaboration.")

if __name__ == "__main__":
    setup_project_structure()