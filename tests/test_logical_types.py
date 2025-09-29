#!/usr/bin/env python3
"""
Test script for rugo parquet features:
1. Logical type extraction
"""

import glob
import sys
from pathlib import Path
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent))

import rugo.parquet as parquet_meta


def test_logical_types():
    """Test logical type extraction"""
    print("=== Testing Logical Types ===")
    
    files_to_test = glob.glob("tests/data/*.parquet")
    
    for file_path in files_to_test:
        if not Path(file_path).exists():
            print(f"Skipping {file_path} - file not found")
            continue
            
        print(f"\nFile: {file_path}")

        meta = parquet_meta.read_metadata(file_path)
        
        for rg_idx, rg in enumerate(meta['row_groups']):
            print(f"  Row Group {rg_idx}:")
            for col in rg['columns']:
                logical = col.get('logical_type', '')
                print(f"    {col['name']:20} | physical={col['type']:12} | logical={logical or '(none)'}")
            break  # Only show first row group
            

def test_comparison_with_pyarrow():
    """Compare our logical types with PyArrow's interpretation"""
    print("\n=== Comparison with PyArrow ===")
    
    files_to_test = glob.glob("tests/data/*.parquet")
    
    for file_path in files_to_test:

        if not Path(file_path).exists():
            print(f"Skipping comparison - {file_path} not found")
            return
            
        print(f"File: {file_path}")
        
        # PyArrow interpretation
        pf = pq.ParquetFile(file_path)
        schema = pf.schema.to_arrow_schema()
        print("  PyArrow schema:")
        for field in schema:
            print(f"    {field.name:20} | {field.type}")
        
        # Our interpretation
        meta = parquet_meta.read_metadata(file_path)
        print("  Rugo interpretation:")
        for col in meta['row_groups'][0]['columns']:
            logical = col.get('logical_type', '')
            print(f"    {col['name']:20} | physical={col['type']:12} | logical={logical or '(none)'}")


if __name__ == '__main__':
    test_logical_types()
    test_comparison_with_pyarrow()
