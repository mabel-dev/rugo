# rugo
File Decoders

A high-performance Cython-based library for decoding file formats, starting with Parquet metadata extraction.

## Features

- **Stream-based Processing**: Decode parquet metadata from streams, bytes, or file paths
- **Direct Binary Parsing**: Pure Cython implementation without external dependencies
- **Memory Efficient**: Optimized for minimal memory overhead
- **Simple API**: Easy-to-use interface for metadata operations
- **Extensible**: Designed to support additional file formats
- **Metadata Focus**: Extracts file metadata, statistics, and bloom filter information
- **Row Group Wrappers**: Simplified API that handles row group iteration automatically
- **Comprehensive Testing**: Full test suite with GitHub Actions CI/CD
- **Zero Dependencies**: No PyArrow, numpy, or pandas requirements

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
pip install Cython setuptools wheel pytest

# Build the Cython extensions
python setup.py build_ext --inplace

# Run tests
pytest tests/
```

## Usage

### Basic Stream-based Metadata Reading

```python
from rugo.decoders.parquet_decoder import get_parquet_info, get_parquet_statistics, get_parquet_bloom_filters
import io

# From bytes
parquet_bytes = b"..."  # your parquet data
info = get_parquet_info(parquet_bytes)
print(f"File size: {info['file_size']}, Version: {info['version']}")

# From stream
stream = io.BytesIO(parquet_bytes)
stats = get_parquet_statistics(stream, 'column_name')

# From file (legacy support)
bloom_filters = get_parquet_bloom_filters('data.parquet', 'column_name')
```

### Advanced Usage with ParquetDecoder

```python
from rugo.decoders.parquet_decoder import ParquetDecoder
import io

# Create decoder with stream/bytes/file
decoder = ParquetDecoder(stream_or_bytes_or_path)

# Get basic metadata
metadata = decoder.get_metadata()
print(f"File size: {metadata['file_size']}")
print(f"Version: {metadata['version']}")
print(f"Created by: {metadata['created_by']}")

# Get column names (requires full Thrift parsing - placeholder for now)
columns = decoder.get_column_names()

# Wrapped row group operations (no manual iteration needed!)
all_stats = decoder.get_all_statistics()
all_bloom_filters = decoder.get_all_bloom_filters()
row_group_stats = decoder.get_row_group_statistics()

# Check bloom filters across all row groups
results = decoder.check_bloom_filter_all_row_groups('category_col', 'some_value')

# Always close when done
decoder.close()
```

### Row Group Wrapper Examples

The library provides convenient wrappers that handle row group iteration automatically:

```python
# Instead of manually iterating over row groups, use wrapper methods:

# Get statistics for ALL columns across ALL row groups
all_stats = decoder.get_all_statistics()

# Get bloom filters for ALL columns across ALL row groups  
all_bloom_filters = decoder.get_all_bloom_filters()

# Check a value against bloom filters in ALL row groups
existence_results = decoder.check_bloom_filter_all_row_groups('category', 'Electronics')

# Get detailed statistics for each row group
row_group_stats = decoder.get_row_group_statistics()
for i, rg_stats in enumerate(row_group_stats):
    print(f"Row group {i}: note: {rg_stats.get('note', 'N/A')}")
```

## Implementation Status

**Current Implementation (v0.1.0):**
- ✅ Stream-based input handling (BinaryIO, bytes, str, Path)
- ✅ Basic parquet file structure validation (magic number verification)
- ✅ Footer parsing and metadata extraction framework
- ✅ Row group wrapper methods
- ✅ Comprehensive test suite
- ✅ Zero external dependencies

**Limitations:**
- Basic metadata extraction (full Thrift parsing not yet implemented)
- Column names, statistics, and bloom filters return placeholder data
- No data reading capabilities (metadata-focused)

**Future Enhancements:**
- Complete Apache Thrift parsing for full metadata extraction
- Column schema parsing
- Statistics extraction from column chunks
- Bloom filter parsing and querying
- Support for additional file formats

## API Reference

### Functions

- `get_parquet_info(source)` - Get basic parquet file information
- `get_parquet_statistics(source, column_name)` - Get column statistics  
- `get_parquet_bloom_filters(source, column_name)` - Get bloom filter information

### ParquetDecoder Class

#### Core Methods

- `__init__(source)` - Initialize with stream, bytes, or file path
- `load_metadata()` - Load and cache metadata 
- `close()` - Close and free resources
- `get_metadata()` - Get comprehensive metadata
- `get_column_names()` - Get list of column names (placeholder)
- `get_statistics(column_name)` - Get column statistics (placeholder)
- `get_bloom_filters(column_name)` - Get bloom filter info (placeholder)

#### Row Group Wrapper Methods

- `get_all_statistics()` - Get statistics for all columns across all row groups
- `get_all_bloom_filters()` - Get bloom filters for all columns across all row groups
- `check_bloom_filter_all_row_groups(column_name, value)` - Check value against bloom filters in all row groups
- `get_row_group_statistics()` - Get detailed statistics for each row group

#### Individual Row Group Methods

- `check_bloom_filter(column_name, value, row_group_idx=0)` - Check value against specific row group's bloom filter

## Performance

The Cython implementation provides significant performance improvements:

- **Stream Processing**: Efficient handling of in-memory data without file I/O overhead
- **Direct Binary Parsing**: No intermediate PyArrow objects
- **Memory Efficiency**: Minimal memory allocation and copying
- **Zero Dependencies**: No external library overhead
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
- Cython (for building from source)
- No runtime dependencies

## License

Apache License 2.0
