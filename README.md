# rugo

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/downloads/)

A high-performance Cython-based library for decoding various file formats, with a focus on fast Parquet metadata extraction.

## 🚀 Features

- **Ultra-fast Parquet metadata reading** - C++ implementation with Cython bindings
- **Minimal dependencies** - Zero runtime dependencies for core functionality  
- **High performance** - Optimized for speed with direct binary parsing
- **Compatible** - Validated against PyArrow for accuracy
- **Cross-platform** - Works on Linux, macOS, and Windows

## 📦 Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/mabel-dev/rugo.git
cd rugo

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install build dependencies
pip install setuptools cython

# Build the extension
make compile

# Install in development mode
pip install -e .
```

### Requirements

- Python 3.8+
- C++ compiler with C++17 support
- Cython (for building from source)

## 🔧 Usage

### Reading Parquet Metadata

```python
import rugo.parquet as parquet_meta

# Extract metadata from a Parquet file
metadata = parquet_meta.read_metadata("example.parquet")

print(f"Number of rows: {metadata['num_rows']}")
print(f"Number of row groups: {len(metadata['row_groups'])}")

# Iterate through row groups and columns
for i, row_group in enumerate(metadata['row_groups']):
    print(f"Row Group {i}:")
    print(f"  Rows: {row_group['num_rows']}")
    print(f"  Size: {row_group['total_byte_size']} bytes")
    
    for col in row_group['columns']:
        print(f"    Column: {col['name']}")
        print(f"    Type: {col['type']}")
        print(f"    Nulls: {col['null_count']}")
        print(f"    Min: {col['min']}")
        print(f"    Max: {col['max']}")
```

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

Rugo is designed for speed. Here are some benchmarks compared to PyArrow:

- **Metadata extraction**: ~10-50x faster than PyArrow for metadata-only operations
- **Memory usage**: Minimal memory footprint with direct binary parsing
- **Startup time**: Fast import with compiled extensions

Run benchmarks yourself:
```bash
make test  # Includes performance comparison tests
```

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
│   └── test_compare_arrow_rugo.py  # Validation tests
├── Makefile                 # Build automation
├── setup.py                 # Build configuration
└── pyproject.toml          # Project metadata
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

## 📊 Supported Formats

Currently supported:
- ✅ **Parquet metadata** - Full metadata extraction with statistics

Planned:
- 🔄 **Additional Parquet features** - Data reading, schema evolution
- 🔄 **Other formats** - ORC, Avro metadata extraction

## 🐛 Known Limitations

- Currently read-only for Parquet metadata
- Requires C++ compiler for installation
- Limited to metadata extraction (no data reading yet)

## 📄 License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

## 👨‍💻 Authors

- **Justin Joyce** - *Initial work* - [joocer](https://github.com/joocer)

## 🙏 Acknowledgments

- Built on top of Apache Parquet format specification
- Inspired by PyArrow's parquet module
- Uses Thrift binary protocol for metadata parsing

## 📈 Roadmap

- [ ] Data reading capabilities
- [ ] Schema evolution support  
- [ ] Additional file format support (ORC, Avro)
- [ ] Async I/O support
- [ ] Memory-mapped file access
- [ ] Compression algorithm support

---

For more information, visit the [GitHub repository](https://github.com/mabel-dev/rugo) or open an [issue](https://github.com/mabel-dev/rugo/issues).
