#!/usr/bin/env python3
"""
Example demonstrating the new rugo parquet features:
1. Logical type extraction
2. Bloom filter testing
"""

import rugo.parquet as parquet_meta

def example_logical_types():
    """Demonstrate logical type extraction"""
    print("🔍 Logical Type Extraction Example")
    print("=" * 50)
    
    file_path = "tests/data/astronauts.parquet"
    metadata = parquet_meta.read_metadata(file_path)
    
    print(f"File: {file_path}")
    print(f"Total rows: {metadata['num_rows']}")
    print(f"Row groups: {len(metadata['row_groups'])}")
    print()
    
    # Show column information with both physical and logical types
    print("Column Information:")
    print(f"{'Column Name':<25} {'Physical Type':<15} {'Logical Type':<15}")
    print("-" * 55)
    
    for col in metadata['row_groups'][0]['columns']:
        physical_type = col['type']
        logical_type = col.get('logical_type', '') or '(none)'
        
        print(f"{col['name']:<25} {physical_type:<15} {logical_type:<15}")
    
    print()

def example_bloom_filters():
    """Demonstrate bloom filter functionality"""
    print("🌸 Bloom Filter Testing Example") 
    print("=" * 50)
    
    file_path = "tests/data/data_index_bloom_encoding_stats.parquet"
    metadata = parquet_meta.read_metadata(file_path)
    
    print(f"File: {file_path}")
    print()
    
    bloom_columns = []
    
    # Find columns with bloom filters
    for col in metadata['row_groups'][0]['columns']:
        if parquet_meta.has_bloom_filter(col):
            bloom_columns.append(col)
            print(f"✓ Column '{col['name']}' has bloom filter")
            print(f"  Physical type: {col['type']}")
            print(f"  Logical type: {col.get('logical_type', '(none)')}")
            print(f"  Bloom filter offset: {col['bloom_offset']}")
            print(f"  Bloom filter length: {col['bloom_length']}")
            print()
    
    if not bloom_columns:
        print("❌ No bloom filters found in this file")
        return
    
    # Test bloom filter functionality
    print("Bloom Filter Tests:")
    print("-" * 30)
    
    for col in bloom_columns:
        print(f"Testing column: {col['name']}")
        
        # Test with values from the column's min/max (likely to be present)
        test_values = [
            col.get('min'),
            col.get('max'), 
            'test_value',  # Unlikely to be present
            'Hello',       # Based on what we saw in the data
            'today'        # Based on what we saw in the data
        ]
        
        for value in test_values:
            if value is not None:
                # Convert bytes to string if needed
                if isinstance(value, bytes):
                    value = value.decode('utf-8', errors='ignore')
                
                if value:
                    try:
                        result = parquet_meta.test_bloom_filter(
                            file_path,
                            col['bloom_offset'], 
                            col['bloom_length'],
                            str(value)
                        )
                        status = "might be present" if result else "definitely not present"
                        print(f"  '{value}' -> {status}")
                        
                    except Exception as e:
                        print(f"  '{value}' -> Error: {e}")
        print()

def example_comparison():
    """Compare physical vs logical types"""
    print("🔄 Physical vs Logical Type Comparison")
    print("=" * 50)
    
    files = [
        ("tests/data/satellites.parquet", "Satellites data"),
        ("tests/data/tweets.parquet", "Twitter data")
    ]
    
    for file_path, description in files:
        print(f"{description} ({file_path}):")
        
        try:
            metadata = parquet_meta.read_metadata(file_path)
            
            # Show examples where logical type adds information
            for col in metadata['row_groups'][0]['columns'][:5]:  # First 5 columns
                physical = col['type']
                logical = col.get('logical_type', '')
                
                if logical:
                    print(f"  {col['name']}: {physical} → {logical}")
                else:
                    print(f"  {col['name']}: {physical}")
                    
        except Exception as e:
            print(f"  Error reading {file_path}: {e}")
        
        print()

if __name__ == '__main__':
    example_logical_types()
    example_bloom_filters() 
    example_comparison()
    
    print("🎉 New Features Summary:")
    print("  ✓ Logical type extraction - Working!")
    print("  ⚠️  Bloom filter testing - Basic framework implemented")
    print("     (Bloom filter format parsing needs refinement)")