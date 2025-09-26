# rugo
File Decoders

A high-performance Cython-based library for decoding various file formats, starting with Parquet files.

## Features

- **Fast Parquet Decoding**: High-performance parquet file reading using Cython and PyArrow
- **Memory Efficient**: Optimized for minimal memory overhead
- **Simple API**: Easy-to-use interface for file operations
- **Extensible**: Designed to support additional file formats

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
pip install Cython numpy pyarrow setuptools wheel

# Build the Cython extensions
python setup.py build_ext --inplace
```

## Usage

### Basic Parquet Reading

```python
from rugo.decoders.parquet_decoder import read_parquet, get_parquet_info

# Get file information
info = get_parquet_info('data.parquet')
print(f"Rows: {info['num_rows']}, Columns: {info['num_columns']}")

# Read the entire file
table = read_parquet('data.parquet')
print(f"Data shape: {table.shape}")

# Read specific columns
table = read_parquet('data.parquet', columns=['col1', 'col2'])
```

### Advanced Usage with ParquetDecoder

```python
from rugo.decoders.parquet_decoder import ParquetDecoder

# Create decoder instance
decoder = ParquetDecoder('data.parquet')

# Get metadata
metadata = decoder.get_metadata()
print(f"Schema: {metadata['schema']}")

# Get column names
columns = decoder.get_column_names()
print(f"Available columns: {columns}")

# Read specific columns
table = decoder.read_columns(['numeric_column'])

# Fast numeric column reading (returns numpy array)
values = decoder.read_numeric_column_fast('numeric_column')
print(f"Values shape: {values.shape}, dtype: {values.dtype}")

# Get column statistics
stats = decoder.get_statistics('numeric_column')
print(f"Min: {stats.get('min')}, Max: {stats.get('max')}")

# Read specific row groups
table = decoder.read_row_groups([0, 1], columns=['col1', 'col2'])

# Always close when done
decoder.close()
```

## API Reference

### Functions

- `read_parquet(file_path, columns=None)` - Read a parquet file into PyArrow Table
- `get_parquet_info(file_path)` - Get basic information about a parquet file

### ParquetDecoder Class

#### Methods

- `load_file()` - Load and cache file metadata
- `close()` - Close file and free resources
- `get_metadata()` - Get comprehensive file metadata
- `get_column_names()` - Get list of column names
- `read_columns(columns=None, use_threads=True)` - Read specific columns
- `read_row_groups(row_groups, columns=None, use_threads=True)` - Read specific row groups
- `read_numeric_column_fast(column_name)` - Fast read of numeric column into numpy array
- `get_statistics(column_name)` - Get column statistics if available

## Performance

The Cython implementation provides significant performance improvements over pure Python:

- **Fast I/O**: Leverages PyArrow's optimized reading
- **Memory Efficiency**: Direct numpy array access for numeric data
- **Zero-copy Operations**: Where possible, avoids data copying

## Requirements

- Python 3.8+
- NumPy
- PyArrow 10.0+
- Cython (for building from source)

## License

Apache License 2.0
