#!/usr/bin/env python3
"""
Example usage of the rugo parquet decoder.

This script demonstrates how to use the rugo library to decode parquet files
efficiently using the Cython-based implementation.
"""

import tempfile
import random
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

def create_sample_data():
    """Create sample parquet file for demonstration without pandas/numpy."""
    # Generate sample data using Python's random module
    random.seed(42)
    
    # Create data directly as PyArrow arrays
    num_rows = 1000
    
    data = {
        'id': pa.array(list(range(num_rows))),
        'name': pa.array([f'product_{i}' for i in range(num_rows)]),
        'price': pa.array([random.uniform(10, 1000) for _ in range(num_rows)]),
        'category': pa.array([random.choice(['Electronics', 'Clothing', 'Books']) for _ in range(num_rows)]),
        'rating': pa.array([random.uniform(1, 5) for _ in range(num_rows)]),
        'sales': pa.array([random.randint(0, 500) for _ in range(num_rows)])
    }
    
    # Create PyArrow table
    table = pa.table(data)
    
    # Create temporary parquet file
    temp_file = Path(tempfile.gettempdir()) / 'sample_products.parquet'
    pq.write_table(table, temp_file)
    
    return temp_file, table

def demonstrate_basic_usage():
    """Demonstrate basic parquet reading functionality."""
    from rugo.decoders.parquet_decoder import read_parquet, get_parquet_info
    
    print("=== Basic Usage Example ===")
    
    # Create sample data
    parquet_file, original_table = create_sample_data()
    print(f"Created sample file: {parquet_file}")
    print(f"Original data: {original_table.num_rows} rows, {original_table.num_columns} columns")
    
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
    parquet_file, original_table = create_sample_data()
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
        
        # Fast numeric column reading (now returns array.array)
        prices = decoder.read_numeric_column_fast('price')
        print(f"Fast price read: length={len(prices)}, type={type(prices)}")
        if len(prices) > 0:
            print(f"Price statistics: min={min(prices):.2f}, max={max(prices):.2f}, mean={sum(prices)/len(prices):.2f}")
        
        # Get column statistics from metadata
        try:
            price_stats = decoder.get_statistics('price')
            print(f"Metadata statistics: {price_stats}")
        except:
            print("Column statistics not available in metadata")
        
        # Test bloom filter functionality
        print("\n=== Bloom Filter Demo ===")
        bloom_filters = decoder.get_bloom_filters('category')
        print(f"Bloom filters for 'category': {list(bloom_filters.keys())}")
        for rg_key, bloom_info in bloom_filters.items():
            print(f"  {rg_key}: available={bloom_info['available']}")
        
        # Test bloom filter check
        test_value = 'Electronics'
        might_exist = decoder.check_bloom_filter('category', test_value)
        print(f"Value '{test_value}' might exist in category column: {might_exist}")
        
        # Read specific row groups (if multiple exist)
        if metadata['num_row_groups'] > 1:
            subset = decoder.read_row_groups([0], columns=['id', 'name', 'price'])
            print(f"Row group 0 data: {subset.shape}")
        else:
            print("Only one row group available")
        
        # Read high-value products
        high_value_data = decoder.read_columns(['name', 'price', 'category'])
        # Convert to python lists for filtering demonstration
        names = high_value_data.column('name').to_pylist()
        prices_list = high_value_data.column('price').to_pylist()
        categories = high_value_data.column('category').to_pylist()
        
        expensive_items = [(name, price, cat) for name, price, cat in zip(names, prices_list, categories) if price > 800]
        print(f"High-value items (>$800): {len(expensive_items)} items")
        if expensive_items:
            most_expensive = max(expensive_items, key=lambda x: x[1])
            print(f"Most expensive: {most_expensive[0]} - ${most_expensive[1]:.2f}")
    
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