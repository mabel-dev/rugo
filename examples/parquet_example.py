#!/usr/bin/env python3
"""
Example usage of the rugo parquet metadata decoder.

This script demonstrates how to use the rugo library to decode parquet metadata
efficiently using the Cython-based implementation without PyArrow dependency.
"""

import tempfile
import io
import struct
from pathlib import Path

def create_minimal_parquet_data():
    """Create minimal parquet data for demonstration."""
    # Create a minimal parquet file structure for testing
    # This is a very basic structure just to demonstrate the parser
    
    # Parquet magic number at the start
    header = b"PAR1"
    
    # Create fake metadata (in real parquet, this would be Thrift-encoded)
    fake_metadata = b'{"version": 1, "num_rows": 1000, "created_by": "rugo_example"}'
    
    # Footer: metadata_length (4 bytes, little-endian) + magic number (4 bytes)
    footer_length = len(fake_metadata)
    footer = struct.pack('<I', footer_length) + b"PAR1"
    
    # Combine: header + fake_metadata + footer
    parquet_bytes = header + fake_metadata + footer
    
    # Create in-memory stream
    stream = io.BytesIO(parquet_bytes)
    stream.seek(0)
    
    return stream, len(parquet_bytes)

def demonstrate_basic_usage():
    """Demonstrate basic parquet metadata reading functionality with streams."""
    from rugo.decoders.parquet_decoder import get_parquet_info, get_parquet_statistics, get_parquet_bloom_filters
    
    print("=== Basic Usage Example (Metadata Only) ===")
    
    # Create minimal parquet data
    parquet_stream, data_size = create_minimal_parquet_data()
    print(f"Created minimal parquet stream with {data_size} bytes")
    
    # Get file information from stream
    info = get_parquet_info(parquet_stream)
    print(f"Stream info - File size: {info['file_size']}, Version: {info['version']}")
    print(f"Created by: {info['created_by']}")
    
    # Reset stream position for next operations
    parquet_stream.seek(0)
    
    # Get statistics for a column (placeholder implementation)
    stats = get_parquet_statistics(parquet_stream, 'example_column')
    print(f"Column statistics: {stats}")
    
    # Reset stream position
    parquet_stream.seek(0)
    
    # Get bloom filters for a column (placeholder implementation)
    bloom_filters = get_parquet_bloom_filters(parquet_stream, 'example_column')
    print(f"Bloom filters: {bloom_filters}")
    
    # Also demonstrate bytes input
    parquet_bytes = parquet_stream.getvalue()
    info_from_bytes = get_parquet_info(parquet_bytes)
    print(f"Info from bytes - File size: {info_from_bytes['file_size']}")
    
    print("Stream-based metadata reading completed successfully!")

def demonstrate_advanced_usage():
    """Demonstrate advanced ParquetDecoder metadata functionality with wrapped row group calls."""
    from rugo.decoders.parquet_decoder import ParquetDecoder
    
    print("\n=== Advanced Usage Example (Metadata + Row Group Wrappers) ===")
    
    # Create minimal parquet data
    parquet_stream, data_size = create_minimal_parquet_data()
    print(f"Using parquet stream with {data_size} bytes")
    
    # Create decoder instance with stream
    decoder = ParquetDecoder(parquet_stream)
    
    try:
        # Get detailed metadata
        metadata = decoder.get_metadata()
        print(f"Detailed metadata:")
        print(f"  - File size: {metadata['file_size']} bytes")
        print(f"  - Version: {metadata['version']}")
        print(f"  - Created by: {metadata['created_by']}")
        print(f"  - Has schema: {metadata['has_schema']}")
        
        # Get column names (placeholder implementation)
        columns = decoder.get_column_names()
        print(f"Available columns: {columns} (empty without full Thrift parsing)")
        
        # WRAPPED ROW GROUP CALLS - no manual iteration needed!
        print("\n=== Wrapped Row Group Operations (No Manual Iteration!) ===")
        
        # Get all statistics in one call (placeholder implementation)
        all_stats = decoder.get_all_statistics()
        print(f"All column statistics: {all_stats} (empty without full parsing)")
        
        # Get all bloom filters in one call (placeholder implementation)
        all_bloom_filters = decoder.get_all_bloom_filters()
        print(f"All bloom filters: {all_bloom_filters} (empty without full parsing)")
        
        # Check value across all row groups in one call
        category_results = decoder.check_bloom_filter_all_row_groups('category', 'Electronics')
        print(f"Bloom filter check for 'Electronics' across all row groups: {category_results}")
        
        # Get row group statistics wrapped in one call
        row_group_stats = decoder.get_row_group_statistics()
        print(f"Row group statistics:")
        for i, stats in enumerate(row_group_stats):
            print(f"  Row group {i}: {stats['num_rows']} rows, note: {stats.get('note', 'N/A')}")
        
        # Test individual methods
        print("\n=== Individual Column Operations ===")
        test_stats = decoder.get_statistics('test_column')
        print(f"Individual column stats: {test_stats}")
        
        test_bloom = decoder.get_bloom_filters('test_column')
        print(f"Individual bloom filter: {test_bloom}")
        
        test_check = decoder.check_bloom_filter('test_column', 'test_value')
        print(f"Bloom filter check result: {test_check}")
    
    finally:
        # Always close the decoder
        decoder.close()
        print("Decoder closed successfully!")
    
    print("\nNote: This implementation focuses on metadata extraction.")
    print("Full Thrift parsing would be needed for complete parquet metadata support.")

def main():
    """Run all examples."""
    try:
        # Add parent directory to path for importing rugo when running from examples/
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent))
        
        # Check if rugo is available
        from rugo.decoders import parquet_decoder
        print("Rugo parquet metadata decoder loaded successfully!")
        print(f"Available functions: {[x for x in dir(parquet_decoder) if not x.startswith('_')]}")
        
        # Run examples
        demonstrate_basic_usage()
        demonstrate_advanced_usage()
        
        print("\n=== Example completed successfully! ===")
        print("Note: This version focuses on metadata extraction without PyArrow dependency.")
        
    except ImportError as e:
        print(f"Error: Could not import rugo library: {e}")
        print("Make sure to build the Cython extensions first:")
        print("  python setup.py build_ext --inplace")
    except Exception as e:
        print(f"Error running examples: {e}")

if __name__ == "__main__":
    main()