#!/usr/bin/env python3
"""
Example usage of the rugo parquet decoder.

This script demonstrates how to use the rugo library to decode parquet data
efficiently using the Cython-based implementation with stream-based input.
"""

import tempfile
import random
import pyarrow as pa
import pyarrow.parquet as pq
import io
from pathlib import Path

def create_sample_data():
    """Create sample parquet data for demonstration without pandas/numpy."""
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
    
    # Create in-memory parquet stream
    stream = io.BytesIO()
    pq.write_table(table, stream)
    stream.seek(0)
    
    return stream, table

def demonstrate_basic_usage():
    """Demonstrate basic parquet reading functionality with streams."""
    from rugo.decoders.parquet_decoder import read_parquet, get_parquet_info
    
    print("=== Basic Usage Example (Stream-based) ===")
    
    # Create sample data as stream
    parquet_stream, original_table = create_sample_data()
    print(f"Created parquet stream with {original_table.num_rows} rows, {original_table.num_columns} columns")
    
    # Get file information from stream
    info = get_parquet_info(parquet_stream)
    print(f"Stream info - Rows: {info['num_rows']}, Columns: {info['num_columns']}")
    
    # Reset stream position for next read
    parquet_stream.seek(0)
    
    # Read entire data from stream
    table = read_parquet(parquet_stream)
    print(f"Read complete table: {table.shape}")
    print(f"Column names: {table.column_names}")
    
    # Reset stream position for selective read
    parquet_stream.seek(0)
    
    # Read specific columns from stream
    price_data = read_parquet(parquet_stream, columns=['name', 'price', 'category'])
    print(f"Price data shape: {price_data.shape}")
    
    # Also demonstrate bytes input
    parquet_bytes = parquet_stream.getvalue()
    table_from_bytes = read_parquet(parquet_bytes)
    print(f"Read from bytes: {table_from_bytes.shape}")
    
    print("Stream-based reading completed successfully!")

def demonstrate_advanced_usage():
    """Demonstrate advanced ParquetDecoder functionality with wrapped row group calls."""
    from rugo.decoders.parquet_decoder import ParquetDecoder
    
    print("\n=== Advanced Usage Example (Stream + Row Group Wrappers) ===")
    
    # Create sample data as stream
    parquet_stream, original_table = create_sample_data()
    print(f"Using parquet stream with {original_table.num_rows} rows")
    
    # Create decoder instance with stream
    decoder = ParquetDecoder(parquet_stream)
    
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
        
        # WRAPPED ROW GROUP CALLS - no manual iteration needed!
        print("\n=== Wrapped Row Group Statistics (No Manual Iteration!) ===")
        
        # Get all statistics in one call
        all_stats = decoder.get_all_statistics()
        print(f"All column statistics: {list(all_stats.keys())}")
        for col, stats in all_stats.items():
            if 'min' in stats and stats['min'] is not None:
                print(f"  {col}: min={stats['min']}, max={stats['max']}, nulls={stats['null_count']}")
        
        # Get all bloom filters in one call
        all_bloom_filters = decoder.get_all_bloom_filters()
        print(f"\nAll bloom filters: {list(all_bloom_filters.keys())}")
        for col, bloom_info in all_bloom_filters.items():
            available_count = sum(1 for rg_info in bloom_info.values() if rg_info.get('available', False))
            print(f"  {col}: {available_count}/{len(bloom_info)} row groups have bloom filters")
        
        # Check value across all row groups in one call
        category_results = decoder.check_bloom_filter_all_row_groups('category', 'Electronics')
        print(f"\nBloom filter check for 'Electronics' across all row groups: {category_results}")
        
        # Get row group statistics wrapped in one call
        row_group_stats = decoder.get_row_group_statistics()
        print(f"\nRow group statistics:")
        for i, stats in enumerate(row_group_stats):
            print(f"  Row group {i}: {stats['num_rows']} rows, {stats['num_columns']} cols, {stats['total_byte_size']} bytes")
        
        # Read high-value products
        parquet_stream.seek(0)  # Reset stream for new read
        high_value_data = decoder.read_columns(['name', 'price', 'category'])
        # Convert to python lists for filtering demonstration
        names = high_value_data.column('name').to_pylist()
        prices_list = high_value_data.column('price').to_pylist()
        categories = high_value_data.column('category').to_pylist()
        
        expensive_items = [(name, price, cat) for name, price, cat in zip(names, prices_list, categories) if price > 800]
        print(f"\nHigh-value items (>$800): {len(expensive_items)} items")
        if expensive_items:
            most_expensive = max(expensive_items, key=lambda x: x[1])
            print(f"Most expensive: {most_expensive[0]} - ${most_expensive[1]:.2f}")
    
    finally:
        # Always close the decoder
        decoder.close()
        print("Decoder closed successfully!")

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