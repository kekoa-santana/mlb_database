import sys
import os

# Add the parent directory to Python path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pybaseball as pb
import pandas as pd
from datetime import date
from dotenv import load_dotenv
load_dotenv('config/.env')

def test_pybaseball_connection():
    """Test if pybaseball can fetch data successfully"""
    try:
        print("Testing pybaseball connection...")
        
        # Small test - just one day
        df = pb.statcast('2024-04-01', '2024-04-01')
        
        print(f"Successfully fetched {len(df)} rows")
        print(f"Columns available: {len(df.columns)}")
        print(f"Sample columns: {df.columns[:10].tolist()}")
        
        # Test essential columns exist
        required_cols = ['game_pk', 'pitcher', 'batter', 'pitch_number', 'at_bat_number']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"Missing required columns: {missing_cols}")
            return False
        else:
            print("All required columns present")
            
        return True
        
    except Exception as e:
        print(f"pybaseball test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_pybaseball_connection()
    if success:
        print("\npybaseball test PASSED")
    else:
        print("\npybaseball test FAILED")
        sys.exit(1)