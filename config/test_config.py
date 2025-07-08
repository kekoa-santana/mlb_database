"""
Test-specific configuration settings
Overrides production settings for safe testing
"""

from datetime import date

# Test date ranges (small, safe datasets)
TEST_DATE_RANGES = {
    2024: (date(2024, 4, 1), date(2024, 4, 3)),  # Just 3 days for testing
    2023: (date(2023, 4, 1), date(2023, 4, 7)),  # One week for larger tests
}

# Test database settings
TEST_DB_CONFIG = {
    'max_rows_per_test': 1000,  # Limit data size during tests
    'test_table_prefix': 'test_',  # Use test_ prefix for tables
    'cleanup_after_test': True,  # Whether to clean up test data
}

# Test Lambda settings
TEST_LAMBDA_CONFIG = {
    'timeout_seconds': 60,  # Shorter timeout for tests
    'memory_mb': 512,       # Less memory needed for tests
    'max_retries': 1,       # Fewer retries during testing
}

# Safe test pitcher IDs (known to have data)
TEST_PITCHER_IDS = [
    594798,  # Gerrit Cole
    545333,  # Jacob deGrom  
    592789,  # Shane Bieber
]

# Test game PKs (known good games)
TEST_GAME_PKS = [
    717614,  # Random 2024 game
    716374,  # Another 2024 game
]

# Mock data for when APIs are unavailable
MOCK_STATCAST_DATA = {
    'game_pk': [717614, 717614],
    'pitcher': [594798, 594798], 
    'batter': [545361, 643376],
    'pitch_type': ['FF', 'SL'],
    'release_speed': [98.5, 86.2],
    'events': [None, 'strikeout'],
    'description': ['ball', 'swinging_strike'],
    'game_date': ['2025-07-06', '2025-07-06'],
    'inning': [1, 1],
    'at_bat_number': [1, 2],
    'pitch_number': [1, 3],
}

# Environment-specific settings
class TestConfig:
    """Test configuration class"""
    
    # Use test database
    USE_TEST_DATABASE = True
    
    # Smaller batch sizes for testing
    BATCH_SIZE = 10
    
    # Skip expensive operations during tests
    SKIP_HEAVY_PROCESSING = True
    
    # Test mode flags
    MOCK_EXTERNAL_APIS = False  # Set to True to use mock data
    VALIDATE_DATA_QUALITY = True
    LOG_LEVEL = 'DEBUG'
    
    # Cost safety limits
    MAX_API_CALLS_PER_TEST = 5
    MAX_DATA_POINTS_PER_TEST = 1000
    
    @classmethod
    def get_test_date_range(cls, year: int = 2024):
        """Get safe date range for testing"""
        return TEST_DATE_RANGES.get(year, (date(2024, 4, 1), date(2024, 4, 1)))
    
    @classmethod
    def is_safe_to_test(cls, num_rows: int) -> bool:
        """Check if test is within safe limits"""
        return num_rows <= cls.MAX_DATA_POINTS_PER_TEST