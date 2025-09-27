#!/usr/bin/env python3
"""
Test script for new rugo parquet features:
1. Logical type extraction
2. Bloom filter testing
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import rugo.parquet as parquet_meta

def test_logical_types():
    """Test logical type extraction"""
    print("=== Testing Logical Types ===")
    
    files_to_test = [
        'tests/data/satellites.parquet',
        'tests/data/astronauts.parquet',
        'tests/data/tweets.parquet'
    ]
    
    for file_path in files_to_test:
        if not Path(file_path).exists():
            print(f"Skipping {file_path} - file not found")
            continue
            
        print(f"\nFile: {file_path}")
        try:
            meta = parquet_meta.read_metadata(file_path)
            
            for rg_idx, rg in enumerate(meta['row_groups']):
                print(f"  Row Group {rg_idx}:")
                for col in rg['columns']:
                    logical = col.get('logical_type', '')
                    print(f"    {col['name']:15} | physical={col['type']:12} | logical={logical or '(none)'}")
                break  # Only show first row group
                
        except Exception as e:
            print(f"  Error: {e}")

def test_bloom_filters():
    """Test bloom filter functionality"""
    print("\n=== Testing Bloom Filters ===")
    
    files_to_test = [
        'tests/data/data_index_bloom_encoding_stats.parquet',
        'tests/data/satellites.parquet'
    ]
    
    for file_path in files_to_test:
        if not Path(file_path).exists():
            print(f"Skipping {file_path} - file not found")
            continue
            
        print(f"\nFile: {file_path}")
        try:
            meta = parquet_meta.read_metadata(file_path)
            
            bloom_found = False
            for rg_idx, rg in enumerate(meta['row_groups']):
                print(f"  Row Group {rg_idx}:")
                for col in rg['columns']:
                    if col['bloom_offset'] >= 0:
                        bloom_found = True
                        print(f"    {col['name']:15} | has bloom filter at offset {col['bloom_offset']}, length {col['bloom_length']}")
                        
                        # Test the bloom filter function (even if it doesn't work perfectly yet)
                        test_values = ['test', col.get('min', ''), col.get('max', '')]
                        for val in test_values:
                            if val and isinstance(val, (str, bytes)):
                                try:
                                    result = parquet_meta.test_bloom_filter(file_path, col['bloom_offset'], col['bloom_length'], str(val))
                                    print(f"      test_bloom_filter({val!r}) -> {result}")
                                except Exception as e:
                                    print(f"      test_bloom_filter({val!r}) -> Error: {e}")
                    else:
                        print(f"    {col['name']:15} | no bloom filter")
                break  # Only show first row group
                
            if not bloom_found:
                print("  No bloom filters found in this file")
                
        except Exception as e:
            print(f"  Error: {e}")

def test_comparison_with_pyarrow():
    """Compare our logical types with PyArrow's interpretation"""
    print("\n=== Comparison with PyArrow ===")
    
    try:
        import pyarrow.parquet as pq
        
        file_path = 'tests/data/satellites.parquet'
        if not Path(file_path).exists():
            print(f"Skipping comparison - {file_path} not found")
            return
            
        print(f"File: {file_path}")
        
        # PyArrow interpretation
        pf = pq.ParquetFile(file_path)
        schema = pf.schema.to_arrow_schema()
        print("  PyArrow schema:")
        for field in schema:
            print(f"    {field.name:15} | {field.type}")
        
        # Our interpretation
        meta = parquet_meta.read_metadata(file_path)
        print("  Rugo interpretation:")
        for col in meta['row_groups'][0]['columns']:
            logical = col.get('logical_type', '')
            print(f"    {col['name']:15} | physical={col['type']:12} | logical={logical or '(none)'}")
            
    except ImportError:
        print("  PyArrow not available for comparison")
    except Exception as e:
        print(f"  Error: {e}")

if __name__ == '__main__':
    test_logical_types()
    test_bloom_filters()
    test_comparison_with_pyarrow()