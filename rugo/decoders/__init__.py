"""
File decoders subpackage

Contains various file format decoders implemented in Cython for high performance.
"""

# Import will be available after Cython compilation
try:
    from . import parquet_decoder
    __all__ = ["parquet_decoder"]
except ImportError:
    # During development before compilation
    __all__ = []