#!/usr/bin/env python3
"""
Test script to validate real parquet file reading.
"""

import sys
from pathlib import Path

# Add parent directory to path for importing rugo when running from tests/
sys.path.insert(0, str(Path(__file__).parent.parent))

from rugo.decoders.parquet_decoder import ParquetDecoder, get_parquet_info

def test_real_parquet_files():
    """Test reading real parquet files from Apache parquet-testing."""
    
    test_files = [
        'tests/data/alltypes_plain.parquet',
        'tests/data/data_index_bloom_encoding_stats.parquet'
    ]
    
    for file_path in test_files:
        if not Path(file_path).exists():
            print(f"Skipping {file_path} - file not found")
            continue
            
        print(f"\n=== Testing {file_path} ===")
        
        try:
            # Test with convenience function
            info = get_parquet_info(file_path)
            print(f"File info: {info}")
            
            # Test with decoder class
            decoder = ParquetDecoder(file_path)
            metadata = decoder.get_metadata()
            print(f"Detailed metadata: {metadata}")
            
            # Test wrapper methods
            all_stats = decoder.get_all_statistics()
            print(f"All statistics: {all_stats}")
            
            all_bloom_filters = decoder.get_all_bloom_filters()
            print(f"All bloom filters: {all_bloom_filters}")
            
            decoder.close()
            print("✅ Successfully parsed real parquet file!")
            
        except Exception as e:
            print(f"❌ Error parsing {file_path}: {e}")

if __name__ == "__main__":
    test_real_parquet_files()