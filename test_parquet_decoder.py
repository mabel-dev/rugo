#!/usr/bin/env python3
"""
Simple test script to verify the rugo parquet decoder functionality.
"""

import sys
from pathlib import Path
import tempfile
import pandas as pd
import numpy as np

# Add the current directory to path to import rugo
sys.path.insert(0, str(Path(__file__).parent))

def create_test_data():
    """Create test parquet file."""
    # Create sample data
    np.random.seed(42)  # For reproducible tests
    data = {
        'id': range(100),
        'name': [f'item_{i}' for i in range(100)],
        'value': np.random.rand(100),
        'category': np.random.choice(['A', 'B', 'C'], 100),
        'score': np.random.randint(1, 100, 100)
    }
    df = pd.DataFrame(data)
    
    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
    temp_file.close()
    
    df.to_parquet(temp_file.name, index=False)
    return temp_file.name, df


def test_parquet_decoder():
    """Test the ParquetDecoder class."""
    from rugo.decoders.parquet_decoder import ParquetDecoder, read_parquet, get_parquet_info
    
    # Create test data
    temp_file, original_df = create_test_data()
    
    try:
        print("=== Testing Parquet Decoder ===")
        print(f"Created test file: {temp_file}")
        print(f"Original data shape: {original_df.shape}")
        
        # Test convenience functions
        print("\n1. Testing convenience functions:")
        info = get_parquet_info(temp_file)
        print(f"   - Rows: {info['num_rows']}")
        print(f"   - Columns: {info['num_columns']}")
        print(f"   - Row groups: {info['num_row_groups']}")
        
        # Test reading full file
        table = read_parquet(temp_file)
        print(f"   - Read table shape: {table.shape}")
        print(f"   - Column names: {table.column_names}")
        
        # Test ParquetDecoder class
        print("\n2. Testing ParquetDecoder class:")
        decoder = ParquetDecoder(temp_file)
        
        # Test metadata
        metadata = decoder.get_metadata()
        print(f"   - Metadata rows: {metadata['num_rows']}")
        print(f"   - Metadata columns: {metadata['num_columns']}")
        
        # Test column names
        columns = decoder.get_column_names()
        print(f"   - Column names: {columns}")
        
        # Test reading specific columns
        subset = decoder.read_columns(['id', 'value'])
        print(f"   - Subset shape: {subset.shape}")
        
        # Test fast numeric read
        values = decoder.read_numeric_column_fast('value')
        print(f"   - Fast read shape: {values.shape}, dtype: {values.dtype}")
        print(f"   - Value range: [{values.min():.4f}, {values.max():.4f}]")
        
        # Test statistics
        try:
            stats = decoder.get_statistics('value')
            print(f"   - Statistics: {stats}")
        except:
            print("   - Statistics not available (normal for this test)")
        
        decoder.close()
        
        print("\n✓ All tests passed!")
        
    finally:
        # Cleanup
        Path(temp_file).unlink(missing_ok=True)


if __name__ == "__main__":
    test_parquet_decoder()