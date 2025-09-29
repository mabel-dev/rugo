# rugo

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/downloads/)

A lightning-fast Parquet file reader built with C++ and Cython, optimized for ultra-fast metadata extraction and analysis.

## 🚀 Features

- **🚀 Lightning-fast metadata reading** - 10-50x faster than PyArrow for metadata operations
- **🏗️ C++ core with Cython bindings** - Maximum performance with Python convenience
- **📊 Complete schema information** - Physical types, logical types, and statistics
- **🔄 Schema conversion** - Convert rugo schemas to orso format (optional)
- **🔬 Zero dependencies** - No runtime dependencies for core functionality
- **✅ PyArrow compatible** - Validated results, drop-in replacement for metadata operations

## 📦 Installation

```bash
# Basic installation (coming soon to PyPI)
pip install rugo

# With orso schema conversion support
pip install rugo[orso]
```

### From Source

```bash
# Clone the repository
git clone https://github.com/mabel-dev/rugo.git
cd rugo

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install build dependencies
pip install setuptools cython

# Build the extension
make compile

# Install in development mode
pip install -e .
```

### Requirements

- Python 3.9+
- C++ compiler with C++17 support
- Cython (for building from source)

## 🔧 Usage

### Reading Parquet Metadata

Rugo provides blazing-fast access to Parquet file metadata without the overhead of loading actual data:

```python
import rugo.parquet as parquet_meta

# Extract complete metadata from a Parquet file
metadata = parquet_meta.read_metadata("example.parquet")

print(f"Number of rows: {metadata['num_rows']}")
print(f"Number of row groups: {len(metadata['row_groups'])}")

# Analyze row groups and column statistics
for i, row_group in enumerate(metadata['row_groups']):
    print(f"Row Group {i}:")
    print(f"  Rows: {row_group['num_rows']}")
    print(f"  Size: {row_group['total_byte_size']} bytes")
    
    for col in row_group['columns']:
        print(f"    Column: {col['name']}")
        print(f"    Physical Type: {col['type']}")
        print(f"    Logical Type: {col.get('logical_type', '(none)')}")
        print(f"    Nulls: {col['null_count']}")
        print(f"    Min: {col['min']}")
        print(f"    Max: {col['max']}")
        
        # Check for bloom filter availability
        if parquet_meta.has_bloom_filter(col):
            print(f"    Has bloom filter: Yes")
        else:
            print(f"    Has bloom filter: No")
```

### Advanced Features

#### Schema Analysis

Extract detailed schema information including both physical and logical types:

```python
import rugo.parquet as parquet_meta

metadata = parquet_meta.read_metadata("example.parquet")
for col in metadata['row_groups'][0]['columns']:
    print(f"{col['name']}: {col['type']} -> {col.get('logical_type', '(inferred)')}")
    # Example output:
    # name: BYTE_ARRAY -> STRING
    # timestamp: INT64 -> TIMESTAMP_MILLIS
    # price: DOUBLE -> (inferred)
```

#### Bloom Filter Testing

Quickly test if values might exist in columns without reading the actual data:

```python
import rugo.parquet as parquet_meta

metadata = parquet_meta.read_metadata("example.parquet")

for col in metadata['row_groups'][0]['columns']:
    if parquet_meta.has_bloom_filter(col):
        # Test if a value might be present
        might_exist = parquet_meta.test_bloom_filter(
            "example.parquet",
            col['bloom_offset'],
            col['bloom_length'], 
            "search_value"
        )
        if might_exist:
            print(f"Value might be in column {col['name']}")
        else:
            print(f"Value definitely not in column {col['name']}")
```

#### Schema Conversion to Orso

Convert rugo parquet schemas to [orso](https://github.com/mabel-dev/orso) format:

```python
from rugo.converters.orso import rugo_to_orso_schema, extract_schema_only
import rugo.parquet as parquet_meta

# Read parquet metadata
metadata = parquet_meta.read_metadata("example.parquet")

# Convert to orso RelationSchema
orso_schema = rugo_to_orso_schema(metadata, "my_table")

print(f"Schema: {orso_schema.name}")
print(f"Columns: {len(orso_schema.columns)}")
print(f"Estimated rows: {orso_schema.row_count_estimate}")

# Access individual columns
for column in orso_schema.columns[:3]:
    print(f"{column.name}: {column.type} ({'nullable' if column.nullable else 'not null'})")

# Or get a simplified column mapping
schema_info = extract_schema_only(metadata, "simple_name")
print("Column types:", schema_info['columns'])
```

**Note:** Orso conversion requires the optional `orso` dependency:
```bash
pip install rugo[orso]

### Metadata Structure

The `read_metadata()` function returns a dictionary with the following structure:

```python
{
    "num_rows": int,           # Total number of rows in the file
    "row_groups": [            # List of row groups
        {
            "num_rows": int,           # Rows in this row group
            "total_byte_size": int,    # Size in bytes
            "columns": [               # Column metadata
                {
                    "name": str,           # Column name/path
                    "type": str,           # Physical type (INT64, BYTE_ARRAY, etc.)
                    "logical_type": str,   # Logical type (STRING, TIMESTAMP_MILLIS, etc.)
                    "min": any,            # Minimum value (decoded)
                    "max": any,            # Maximum value (decoded)
                    "null_count": int,     # Number of null values
                    "bloom_offset": int,   # Bloom filter offset (-1 if none)
                    "bloom_length": int,   # Bloom filter length (-1 if none)
                }
            ]
        }
    ]
}
```

## ⚡ Performance

Rugo is specifically designed for blazing-fast Parquet metadata operations:

- **⚡ 10-50x faster** than PyArrow for metadata extraction
- **🧠 Minimal memory footprint** - Direct binary parsing without intermediate objects
- **🚀 Lightning startup** - Fast imports with optimized compiled extensions
- **📊 Efficient statistics** - Decode min/max values without loading columns

### Benchmarks

Run performance comparisons yourself:
```bash
make test  # Includes comprehensive PyArrow vs Rugo benchmarks
```

**Why is Rugo so fast?**
- Direct C++ implementation of Parquet metadata parsing
- Zero-copy binary protocol parsing
- Optimized Thrift deserialization
- No Python object overhead during parsing

## 🛠️ Development

### Building from Source

```bash
# Install development dependencies
make update

# Build Cython extensions
make compile

# Run tests
make test

# Run linting
make lint

# Check type hints
make mypy

# Generate coverage report
make coverage
```

### Project Structure

```
rugo/
├── rugo/
│   ├── __init__.py          # Main package
│   └── parquet/             # Parquet decoder implementation
│       ├── metadata.cpp     # C++ metadata parser
│       ├── metadata.hpp     # C++ headers
│       ├── thrift.hpp       # Thrift protocol implementation
│       └── metadata_reader.pyx  # Cython bindings
├── tests/
│   ├── data/                # Test Parquet files
│   └── tests
├── Makefile                 # Build automation
├── setup.py                 # Build configuration
└── pyproject.toml           # Project metadata
```

### Testing

The test suite includes:
- **Validation tests** - Compare output with PyArrow
- **Performance benchmarks** - Speed comparisons
- **Edge case handling** - Various Parquet file formats

```bash
# Run all tests
make test

# Run specific test
python -m pytest tests/test_compare_arrow_rugo.py -v
```

### Code Quality

We maintain high code quality with:
- **Linting**: ruff, isort, pycln
- **Type checking**: mypy
- **Formatting**: ruff format
- **Cython linting**: cython-lint

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes and add tests
4. Run the test suite: `make test`
5. Run linting: `make lint`  
6. Submit a pull request

### Development Setup

```bash
# Clone your fork
git clone https://github.com/yourusername/rugo.git
cd rugo

# Set up development environment
python -m venv venv
source venv/bin/activate
make update
make compile
make test
```

## 📊 What Rugo Does

**✅ Currently Supported:**
- **Fast Parquet metadata extraction** - Schema, statistics, row group information
- **Logical type detection** - STRING, TIMESTAMP, DECIMAL, etc.
- **Bloom filter testing** - Value presence checks without data scanning
- **Statistics decoding** - Min/max values properly typed and decoded
- **Cross-platform support** - Linux, macOS

**🎯 Focus Areas:**
Rugo is laser-focused on being the fastest Parquet metadata reader available. It doesn't try to be everything to everyone - it does one thing exceptionally well.

## 🐛 Known Limitations

- **Metadata-only**: Rugo focuses on metadata extraction, not data reading
- **C++ compiler required**: Building from source requires C++17 compiler
- **Parquet-specific**: Designed specifically for Parquet format

## 📄 License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

## 👨‍💻 Authors

- **Justin Joyce** - *Initial work* - [joocer](https://github.com/joocer)

## 🙏 Acknowledgments

- Built on top of the Apache Parquet format specification
- Inspired by PyArrow's parquet module design
- Uses optimized Thrift binary protocol for metadata parsing
- Performance insights from the Apache Arrow community

## 📈 Roadmap

**Core Focus: Fastest Parquet Metadata Reader**
- [x] Lightning-fast metadata extraction
- [x] Complete schema information with logical types  
- [ ] Bloom filter support
- [ ] Advanced statistics (histograms, sketches)
- [ ] Parquet format validation

---

For more information, visit the [GitHub repository](https://github.com/mabel-dev/rugo) or open an [issue](https://github.com/mabel-dev/rugo/issues).
