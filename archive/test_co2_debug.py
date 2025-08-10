print("🔍 Script starting...")

try:
    import os
    print("✅ os imported")
    
    import pandas as pd
    print("✅ pandas imported")
    
    from azure.storage.blob import BlobServiceClient
    print("✅ Azure blob imported")
    
    from dotenv import load_dotenv
    print("✅ dotenv imported")
    
    load_dotenv()
    print("✅ Environment loaded")
    
    # Check if CSV file exists
    csv_filename = 'carbon_dioxide_as_carbon_emission_summary_10-08-2025.csv'
    if os.path.exists(csv_filename):
        print(f"✅ CSV file found: {csv_filename}")
    else:
        print(f"❌ CSV file NOT found: {csv_filename}")
        print("Files in current directory:")
        for file in os.listdir('.'):
            if file.endswith('.csv'):
                print(f"   - {file}")
    
    print("🎉 Debug test complete!")
    
except Exception as e:
    print(f"❌ Error in debug: {e}")
    import traceback
    traceback.print_exc()