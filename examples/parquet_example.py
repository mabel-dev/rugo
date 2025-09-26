#!/usr/bin/env python3
"""
Example usage of the rugo parquet decoder.

This script demonstrates how to use the rugo library to decode parquet files
efficiently using the Cython-based implementation.
"""

import tempfile
import pandas as pd
import numpy as np
from pathlib import Path

def create_sample_data():
    """Create sample parquet file for demonstration."""
    # Generate sample data
    np.random.seed(42)
    data = {
        'id': range(1000),
        'name': [f'product_{i}' for i in range(1000)],
        'price': np.random.uniform(10, 1000, 1000),
        'category': np.random.choice(['Electronics', 'Clothing', 'Books'], 1000),
        'rating': np.random.uniform(1, 5, 1000),
        'sales': np.random.randint(0, 500, 1000)
    }
    
    df = pd.DataFrame(data)
    
    # Create temporary parquet file
    temp_file = Path(tempfile.gettempdir()) / 'sample_products.parquet'
    df.to_parquet(temp_file, index=False)
    
    return temp_file, df

def demonstrate_basic_usage():
    """Demonstrate basic parquet reading functionality."""
    from rugo.decoders.parquet_decoder import read_parquet, get_parquet_info
    
    print("=== Basic Usage Example ===")
    
    # Create sample data
    parquet_file, original_df = create_sample_data()
    print(f"Created sample file: {parquet_file}")
    print(f"Original data: {original_df.shape[0]} rows, {original_df.shape[1]} columns")
    
    # Get file information
    info = get_parquet_info(parquet_file)
    print(f"File info - Rows: {info['num_rows']}, Columns: {info['num_columns']}")
    
    # Read entire file
    table = read_parquet(parquet_file)
    print(f"Read complete table: {table.shape}")
    print(f"Column names: {table.column_names}")
    
    # Read specific columns
    price_data = read_parquet(parquet_file, columns=['name', 'price', 'category'])
    print(f"Price data shape: {price_data.shape}")
    
    # Cleanup
    parquet_file.unlink()

def demonstrate_advanced_usage():
    """Demonstrate advanced ParquetDecoder functionality."""
    from rugo.decoders.parquet_decoder import ParquetDecoder
    
    print("\n=== Advanced Usage Example ===")
    
    # Create sample data
    parquet_file, original_df = create_sample_data()
    print(f"Using sample file: {parquet_file}")
    
    # Create decoder instance
    decoder = ParquetDecoder(parquet_file)
    
    try:
        # Get detailed metadata
        metadata = decoder.get_metadata()
        print(f"Detailed metadata:")
        print(f"  - Rows: {metadata['num_rows']}")
        print(f"  - Columns: {metadata['num_columns']}")
        print(f"  - Row groups: {metadata['num_row_groups']}")
        print(f"  - Serialized size: {metadata['serialized_size']} bytes")
        
        # Get column names
        columns = decoder.get_column_names()
        print(f"Available columns: {columns}")
        
        # Fast numeric column reading
        prices = decoder.read_numeric_column_fast('price')
        print(f"Fast price read: shape={prices.shape}, dtype={prices.dtype}")
        print(f"Price statistics: min={prices.min():.2f}, max={prices.max():.2f}, mean={prices.mean():.2f}")
        
        # Get column statistics from metadata
        try:
            price_stats = decoder.get_statistics('price')
            print(f"Metadata statistics: {price_stats}")
        except:
            print("Column statistics not available in metadata")
        
        # Read specific row groups (if multiple exist)
        if metadata['num_row_groups'] > 1:
            subset = decoder.read_row_groups([0], columns=['id', 'name', 'price'])
            print(f"Row group 0 data: {subset.shape}")
        else:
            print("Only one row group available")
        
        # Read high-value products
        high_value_data = decoder.read_columns(['name', 'price', 'category'])
        # Convert to pandas for filtering demonstration
        high_value_df = high_value_data.to_pandas()
        expensive_items = high_value_df[high_value_df['price'] > 800]
        print(f"High-value items (>$800): {len(expensive_items)} items")
        if len(expensive_items) > 0:
            print(f"Most expensive: {expensive_items.loc[expensive_items['price'].idxmax(), 'name']} - ${expensive_items['price'].max():.2f}")
    
    finally:
        # Always close the decoder
        decoder.close()
        parquet_file.unlink()

def main():
    """Run all examples."""
    try:
        # Add parent directory to path for importing rugo when running from examples/
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent))
        
        # Check if rugo is available
        from rugo.decoders import parquet_decoder
        print("Rugo parquet decoder loaded successfully!")
        print(f"Available functions: {[x for x in dir(parquet_decoder) if not x.startswith('_')]}")
        
        # Run examples
        demonstrate_basic_usage()
        demonstrate_advanced_usage()
        
        print("\n=== Example completed successfully! ===")
        
    except ImportError as e:
        print(f"Error: Could not import rugo library: {e}")
        print("Make sure to build the Cython extensions first:")
        print("  python setup.py build_ext --inplace")
    except Exception as e:
        print(f"Error running examples: {e}")

if __name__ == "__main__":
    main()