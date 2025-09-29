"""
rugo - Fast Parquet File Reader

A lightning-fast Parquet metadata reader built with C++ and Cython.
Optimized for ultra-fast metadata extraction and analysis.
"""

__version__ = "0.1.0"
__author__ = "Mabel Dev"

# Import converters for easy access
try:
    from .converters import rugo_to_orso_schema

    __all__ = ["rugo_to_orso_schema"]
except ImportError:
    # orso may not be available
    __all__ = []
