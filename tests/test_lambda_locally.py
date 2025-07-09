import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

sys.path.append(str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv('config/.env')

import json
import pytest
from lambda_function import lambda_handler
from .utils import has_network

class MockContext:
    #Mock AWS Lambda context for local testing
    def get_remaining_time_in_millis(self):
        return 900000  # 15 minutes

def test_daily_update():
    # Test daily update mode with a unique date each time
    
    clean_test_data()
    
    test_event = {
        "mode": "daily_update",
        "date": "2024-06-11"
    }
    
    
def test_table_creation_mode():
    """Test just the table creation without data fetching"""
    print("🧪 Testing table creation...")
    
    try:
        from lambda_utils import create_database_tables
        create_database_tables()
        print("✅ Table creation test PASSED")
        return True
        
    except Exception as e:
        print(f"❌ Table creation test failed: {e}")
        return False

def clean_test_data():
    """Clean up test data from previous runs"""
    print("🧹 Cleaning up previous test data...")
    
    try:
        from lambda_utils import RDSConnection
        
        rds = RDSConnection()
        with rds.get_connection() as conn:
            cursor = conn.cursor()
            
            # Delete test data from previous runs
            test_dates = ['2024-04-01', '2023-03-15', '2023-04-01']
            
            for table in ['statcast_pitchers', 'statcast_batters']:
                for test_date in test_dates:
                    cursor.execute(f"DELETE FROM {table} WHERE game_date = %s", (test_date,))
                    
            conn.commit()
            print("✅ Test data cleanup completed")
            return True
            
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Starting Lambda local tests...\n")
    
    # Clean up previous test data first
    clean_test_data()
    
    # Test table creation first
    if not test_table_creation_mode():
        print("💥 Table creation failed, stopping tests")
        sys.exit(1)
    
    # Test daily update
    if not test_daily_update():
        print("💥 Daily update test failed")
        sys.exit(1)
        
    print("\n🎉 All Lambda tests PASSED!")
    print("\n💡 Pro tip: Test data has been cleaned up for next run")