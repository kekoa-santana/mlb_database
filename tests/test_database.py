import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from lambda_utils import RDSConnection, create_database_tables
from dotenv import load_dotenv
load_dotenv('config/.env')

def test_database_connection():
    """Test RDS PostgreSQL connection"""
    try:
        print("Testing RDS connection...")
        
        # Test basic connection
        rds = RDSConnection()
        with rds.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()
            print(f"Connected to PostgreSQL: {version[0]}")
            
        return True
        
    except Exception as e:
        print(f"Database connection failed: {e}")
        print("Make sure your environment variables are set:")
        print("   - RDS_HOST")
        print("   - RDS_DATABASE") 
        print("   - RDS_USERNAME")
        print("   - RDS_PASSWORD")
        return False

def test_table_creation():
    """Test creating tables"""
    try:
        print("Testing table creation...")
        
        create_database_tables()
        print("All tables created successfully")
        
        # Test that tables exist
        rds = RDSConnection()
        with rds.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
        expected_tables = ['statcast_pitchers', 'statcast_batters', 'mlb_boxscores']
        missing_tables = [t for t in expected_tables if t not in tables]
        
        if missing_tables:
            print(f"Missing tables: {missing_tables}")
            return False
        else:
            print(f"Found tables: {tables}")
            
        return True
        
    except Exception as e:
        print(f"Table creation failed: {e}")
        return False

if __name__ == "__main__":
    # Test connection first
    if not test_database_connection():
        sys.exit(1)
        
    # Then test table creation
    if not test_table_creation():
        sys.exit(1)
        
    print("\nDatabase tests PASSED")