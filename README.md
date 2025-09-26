# rugo
File Decoders

A high-performance Cython-based library for decoding various file formats, starting with Parquet files.

## Features

- **Stream-based Processing**: Decode parquet data from streams, bytes, or file paths
- **Fast Parquet Decoding**: High-performance parquet data reading using Cython and PyArrow
- **Memory Efficient**: Optimized for minimal memory overhead
- **Simple API**: Easy-to-use interface for data operations
- **Extensible**: Designed to support additional file formats
- **Bloom Filter Support**: Extract and query bloom filters from parquet columns
- **Array-based Results**: Returns standard Python array.array objects (no numpy dependency)
- **Row Group Wrappers**: Simplified API that handles row group iteration automatically
- **Comprehensive Testing**: Full test suite with GitHub Actions CI/CD

## Installation

### From Source

```bash
git clone https://github.com/mabel-dev/rugo.git
cd rugo
pip install -e .
```

### Development Setup

```bash
# Install dependencies
pip install Cython pyarrow setuptools wheel pytest

# Build the Cython extensions
python setup.py build_ext --inplace

# Run tests
pytest tests/
```

## Usage

### Basic Stream-based Reading

```python
from rugo.decoders.parquet_decoder import read_parquet, get_parquet_info
import io

# From bytes
parquet_bytes = b"..."  # your parquet data
table = read_parquet(parquet_bytes)

# From stream
stream = io.BytesIO(parquet_bytes)
info = get_parquet_info(stream)
print(f"Rows: {info['num_rows']}, Columns: {info['num_columns']}")

# From file (legacy support)
table = read_parquet('data.parquet', columns=['col1', 'col2'])
```

### Advanced Usage with ParquetDecoder

```python
from rugo.decoders.parquet_decoder import ParquetDecoder
import io

# Create decoder with stream/bytes/file
decoder = ParquetDecoder(stream_or_bytes_or_path)

# Get metadata
metadata = decoder.get_metadata()
print(f"Schema: {metadata['schema']}")

# Get column names
columns = decoder.get_column_names()

# Fast numeric column reading (returns array.array)
values = decoder.read_numeric_column_fast('numeric_column')
print(f"Values length: {len(values)}, type: {type(values)}")

# Wrapped row group operations (no manual iteration needed!)
all_stats = decoder.get_all_statistics()  # All columns, all row groups
all_bloom_filters = decoder.get_all_bloom_filters()  # All bloom filters
row_group_stats = decoder.get_row_group_statistics()  # Detailed row group info

# Check bloom filters across all row groups
results = decoder.check_bloom_filter_all_row_groups('category_col', 'some_value')
print(f"Value might exist in row groups: {results}")

# Always close when done
decoder.close()
```

### Row Group Wrapper Examples

The library provides convenient wrappers that handle row group iteration automatically:

```python
# Instead of manually iterating over row groups, use wrapper methods:

# Get statistics for ALL columns across ALL row groups
all_stats = decoder.get_all_statistics()
for column, stats in all_stats.items():
    print(f"{column}: min={stats.get('min')}, max={stats.get('max')}")

# Get bloom filters for ALL columns across ALL row groups  
all_bloom_filters = decoder.get_all_bloom_filters()

# Check a value against bloom filters in ALL row groups
existence_results = decoder.check_bloom_filter_all_row_groups('category', 'Electronics')

# Get detailed statistics for each row group
row_group_stats = decoder.get_row_group_statistics()
for i, rg_stats in enumerate(row_group_stats):
    print(f"Row group {i}: {rg_stats['num_rows']} rows, {rg_stats['total_byte_size']} bytes")
```

## API Reference

### Functions

- `read_parquet(source, columns=None)` - Read parquet data from stream/bytes/file into PyArrow Table
- `get_parquet_info(source)` - Get basic information about parquet data

### ParquetDecoder Class

#### Core Methods

- `__init__(source)` - Initialize with stream, bytes, or file path
- `load_file()` - Load and cache metadata 
- `close()` - Close and free resources
- `get_metadata()` - Get comprehensive metadata
- `get_column_names()` - Get list of column names
- `read_columns(columns=None, use_threads=True)` - Read specific columns
- `read_row_groups(row_groups, columns=None, use_threads=True)` - Read specific row groups
- `read_numeric_column_fast(column_name)` - Fast read of numeric column into array.array

#### Row Group Wrapper Methods (New!)

- `get_all_statistics()` - Get statistics for all columns across all row groups
- `get_all_bloom_filters()` - Get bloom filters for all columns across all row groups
- `check_bloom_filter_all_row_groups(column_name, value)` - Check value against bloom filters in all row groups
- `get_row_group_statistics()` - Get detailed statistics for each row group

#### Individual Row Group Methods

- `get_statistics(column_name)` - Get column statistics from specific row group
- `get_bloom_filters(column_name)` - Get bloom filter information for specific column
- `check_bloom_filter(column_name, value, row_group_idx=0)` - Check value against specific row group's bloom filter

## Performance

The Cython implementation provides significant performance improvements over pure Python:

- **Stream Processing**: Efficient handling of in-memory data without file I/O overhead
- **Fast I/O**: Leverages PyArrow's optimized reading
- **Memory Efficiency**: Direct array.array access for numeric data
- **Zero-copy Operations**: Where possible, avoids data copying
- **Bloom Filter Querying**: Fast membership testing using parquet bloom filters
- **Batch Operations**: Wrapper methods process multiple row groups efficiently

## Testing

Run the comprehensive test suite:

```bash
# Install test dependencies
pip install pytest

# Run tests
pytest tests/ -v

# Run tests with coverage
pip install pytest-cov
pytest tests/ --cov=rugo --cov-report=html
```

## Requirements

- Python 3.8+
- PyArrow 10.0+
- Cython (for building from source)

## License

Apache License 2.0
