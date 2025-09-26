# cython: language_level=3
# distutils: language = c++

"""
Parquet stream decoder implemented in Cython for high-performance parsing.

This module provides functions to decode and parse parquet data from streams
using PyArrow as the backend while providing a simplified interface.
"""

import array
from typing import Dict, List, Optional, Any, Union, BinaryIO
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import io

cdef class ParquetDecoder:
    """
    High-performance parquet stream decoder using Cython and PyArrow.
    
    This class provides efficient methods to read and decode parquet data from streams
    with minimal memory overhead and fast execution.
    """
    
    cdef object _stream
    cdef object _parquet_file
    cdef object _metadata
    cdef bint _file_loaded
    
    def __cinit__(self, stream: Union[BinaryIO, bytes, str, Path]):
        """
        Initialize the ParquetDecoder with a stream or file-like object.
        
        Parameters
        ----------
        stream : BinaryIO, bytes, str, or Path
            Stream, bytes data, or path to the parquet data to decode.
            If str or Path, it will be opened as a file.
        """
        if isinstance(stream, (str, Path)):
            # Legacy support for file paths - convert to stream
            with open(stream, 'rb') as f:
                self._stream = io.BytesIO(f.read())
        elif isinstance(stream, bytes):
            # Convert bytes to BytesIO stream
            self._stream = io.BytesIO(stream)
        else:
            # Assume it's already a stream-like object
            self._stream = stream
        
        self._parquet_file = None
        self._metadata = None
        self._file_loaded = False
    
    def __dealloc__(self):
        """Clean up resources when decoder is destroyed."""
        self.close()
    
    cpdef void load_file(self):
        """
        Load the parquet data from stream and cache metadata.
        
        Raises
        ------
        ValueError
            If the stream does not contain valid parquet data
        """
        try:
            # Reset stream position to beginning
            if hasattr(self._stream, 'seek'):
                self._stream.seek(0)
            
            self._parquet_file = pq.ParquetFile(self._stream)
            self._metadata = self._parquet_file.metadata
            self._file_loaded = True
        except Exception as e:
            raise ValueError(f"Invalid parquet data: {e}")
    
    cpdef void close(self):
        """Close the parquet file and free resources."""
        if self._parquet_file is not None:
            self._parquet_file = None
        self._metadata = None
        self._file_loaded = False
    
    cpdef dict get_metadata(self):
        """
        Get metadata information about the parquet file.
        
        Returns
        -------
        dict
            Dictionary containing file metadata including schema, row count, etc.
        """
        if not self._file_loaded:
            self.load_file()
        
        cdef dict metadata = {
            'num_rows': self._metadata.num_rows,
            'num_columns': self._metadata.num_columns,
            'num_row_groups': self._metadata.num_row_groups,
            'serialized_size': self._metadata.serialized_size,
            'schema': self._parquet_file.schema_arrow,
        }
        
        return metadata
    
    cpdef list get_column_names(self):
        """
        Get list of column names in the parquet file.
        
        Returns
        -------
        list
            List of column names
        """
        if not self._file_loaded:
            self.load_file()
        
        return self._parquet_file.schema_arrow.names
    
    cpdef object read_columns(self, columns=None, use_threads=True):
        """
        Read specific columns from the parquet file.
        
        Parameters
        ----------
        columns : list, optional
            List of column names to read. If None, read all columns.
        use_threads : bool, default True
            Whether to use multiple threads for reading
            
        Returns
        -------
        pyarrow.Table
            Arrow table containing the requested columns
        """
        if not self._file_loaded:
            self.load_file()
        
        return self._parquet_file.read(columns=columns, use_threads=use_threads)
    
    cpdef object read_row_groups(self, row_groups, columns=None, use_threads=True):
        """
        Read specific row groups from the parquet file.
        
        Parameters
        ----------
        row_groups : list
            List of row group indices to read
        columns : list, optional
            List of column names to read. If None, read all columns.
        use_threads : bool, default True
            Whether to use multiple threads for reading
            
        Returns
        -------
        pyarrow.Table
            Arrow table containing the requested data
        """
        if not self._file_loaded:
            self.load_file()
        
        return self._parquet_file.read_row_groups(
            row_groups=row_groups, 
            columns=columns, 
            use_threads=use_threads
        )
    
    def read_numeric_column_fast(self, str column_name):
        """
        Fast read of a numeric column into a Python array.array.
        
        Parameters
        ----------
        column_name : str
            Name of the numeric column to read
            
        Returns
        -------
        array.array
            1D array.array containing the column data
        """
        cdef object table = self.read_columns([column_name])
        cdef object column = table.column(column_name)
        
        # Convert to Python list first, then to array.array
        cdef list data_list = column.to_pylist()
        
        # Determine the appropriate array type based on the data
        if len(data_list) == 0:
            return array.array('d')  # Default to double for empty arrays
        
        # Check the first non-None value to determine type
        sample_value = None
        for val in data_list:
            if val is not None:
                sample_value = val
                break
        
        if sample_value is None:
            return array.array('d')  # Default to double for all-None columns
        
        # Choose appropriate array type
        if isinstance(sample_value, int):
            if all(isinstance(x, int) or x is None for x in data_list):
                # Replace None with 0 for integer arrays
                clean_data = [x if x is not None else 0 for x in data_list]
                return array.array('q', clean_data)  # long long
        
        # Default to double for floating point or mixed numeric data
        clean_data = [float(x) if x is not None else 0.0 for x in data_list]
        return array.array('d', clean_data)
    
    cpdef dict get_statistics(self, str column_name):
        """
        Get statistics for a specific column.
        
        Parameters
        ----------
        column_name : str
            Name of the column to get statistics for
            
        Returns
        -------
        dict
            Dictionary containing column statistics
        """
        if not self._file_loaded:
            self.load_file()
        
        cdef dict stats = {}
        cdef int row_group_idx
        
        for row_group_idx in range(self._metadata.num_row_groups):
            rg_metadata = self._metadata.row_group(row_group_idx)
            for col_idx in range(rg_metadata.num_columns):
                col_metadata = rg_metadata.column(col_idx)
                if col_metadata.path_in_schema == column_name:
                    if col_metadata.statistics:
                        stats.update({
                            'min': col_metadata.statistics.min,
                            'max': col_metadata.statistics.max,
                            'null_count': col_metadata.statistics.null_count,
                            'distinct_count': col_metadata.statistics.distinct_count,
                        })
                    break
        
        return stats

    def get_bloom_filters(self, str column_name):
        """
        Get bloom filters for a specific column if available.
        
        Parameters
        ----------
        column_name : str
            Name of the column to get bloom filters for
            
        Returns
        -------
        dict
            Dictionary containing bloom filter information for each row group
        """
        if not self._file_loaded:
            self.load_file()
        
        cdef dict bloom_filters = {}
        cdef int row_group_idx
        
        for row_group_idx in range(self._metadata.num_row_groups):
            rg_metadata = self._metadata.row_group(row_group_idx)
            for col_idx in range(rg_metadata.num_columns):
                col_metadata = rg_metadata.column(col_idx)
                if col_metadata.path_in_schema == column_name:
                    # Check if bloom filter is available
                    try:
                        # PyArrow provides access to bloom filter metadata
                        # This will be None if no bloom filter exists
                        bloom_filter = None
                        if hasattr(col_metadata, 'bloom_filter'):
                            bloom_filter = col_metadata.bloom_filter
                        elif hasattr(self._parquet_file, 'read_row_group'):
                            # Try to access through the parquet file reader
                            try:
                                # Some parquet files store bloom filter info differently
                                rg_reader = self._parquet_file._parquet_reader.row_group(row_group_idx)
                                if hasattr(rg_reader, 'column'):
                                    col_reader = rg_reader.column(col_idx)
                                    if hasattr(col_reader, 'bloom_filter'):
                                        bloom_filter = col_reader.bloom_filter
                            except:
                                pass
                        
                        if bloom_filter is not None:
                            bloom_filters[f'row_group_{row_group_idx}'] = {
                                'available': True,
                                'size_bytes': getattr(bloom_filter, 'size_bytes', None),
                                'num_hash_functions': getattr(bloom_filter, 'num_hash_functions', None),
                                'filter_data': bloom_filter  # Store the actual filter object
                            }
                        else:
                            bloom_filters[f'row_group_{row_group_idx}'] = {
                                'available': False,
                                'size_bytes': None,
                                'num_hash_functions': None,
                                'filter_data': None
                            }
                    except Exception as e:
                        bloom_filters[f'row_group_{row_group_idx}'] = {
                            'available': False,
                            'error': str(e),
                            'size_bytes': None,
                            'num_hash_functions': None,
                            'filter_data': None
                        }
                    break
        
        return bloom_filters

    def get_all_statistics(self) -> Dict[str, Dict[str, Any]]:
        """
        Get statistics for all columns across all row groups.
        
        Returns
        -------
        dict
            Dictionary mapping column names to their aggregated statistics
        """
        if not self._file_loaded:
            self.load_file()
        
        cdef dict all_stats = {}
        cdef list column_names = self.get_column_names()
        
        for column_name in column_names:
            all_stats[column_name] = self.get_statistics(column_name)
        
        return all_stats

    def get_all_bloom_filters(self) -> Dict[str, Dict[str, Any]]:
        """
        Get bloom filters for all columns across all row groups.
        
        Returns
        -------
        dict
            Dictionary mapping column names to their bloom filter information
        """
        if not self._file_loaded:
            self.load_file()
        
        cdef dict all_bloom_filters = {}
        cdef list column_names = self.get_column_names()
        
        for column_name in column_names:
            all_bloom_filters[column_name] = self.get_bloom_filters(column_name)
        
        return all_bloom_filters

    def check_bloom_filter_all_row_groups(self, str column_name, object value) -> List[bool]:
        """
        Check if a value might exist in a column across all row groups using bloom filters.
        
        Parameters
        ----------
        column_name : str
            Name of the column to check
        value : object
            Value to check for existence
            
        Returns
        -------
        list[bool]
            List of boolean values indicating if the value might exist in each row group
        """
        if not self._file_loaded:
            self.load_file()
        
        cdef list results = []
        cdef int row_group_idx
        
        for row_group_idx in range(self._metadata.num_row_groups):
            result = self.check_bloom_filter(column_name, value, row_group_idx)
            results.append(result)
        
        return results

    def get_row_group_statistics(self) -> List[Dict[str, Any]]:
        """
        Get statistics for each row group separately.
        
        Returns
        -------
        list[dict]
            List of dictionaries, one per row group, containing row group metadata
        """
        if not self._file_loaded:
            self.load_file()
        
        cdef list row_group_stats = []
        cdef int row_group_idx
        
        for row_group_idx in range(self._metadata.num_row_groups):
            rg_metadata = self._metadata.row_group(row_group_idx)
            stats = {
                'row_group_idx': row_group_idx,
                'num_rows': rg_metadata.num_rows,
                'num_columns': rg_metadata.num_columns,
                'total_byte_size': rg_metadata.total_byte_size,
                'columns': {}
            }
            
            for col_idx in range(rg_metadata.num_columns):
                col_metadata = rg_metadata.column(col_idx)
                col_stats = {
                    'path_in_schema': col_metadata.path_in_schema,
                    'file_offset': col_metadata.file_offset,
                    'file_path': col_metadata.file_path,
                    'total_compressed_size': col_metadata.total_compressed_size,
                    'total_uncompressed_size': col_metadata.total_uncompressed_size,
                }
                
                if col_metadata.statistics:
                    col_stats.update({
                        'min': col_metadata.statistics.min,
                        'max': col_metadata.statistics.max,
                        'null_count': col_metadata.statistics.null_count,
                        'distinct_count': col_metadata.statistics.distinct_count,
                    })
                
                stats['columns'][col_metadata.path_in_schema] = col_stats
            
            row_group_stats.append(stats)
        
        return row_group_stats

    def check_bloom_filter(self, str column_name, object value, int row_group_idx=0):
        """
        Check if a value might exist in a column using bloom filter.
        
        Parameters
        ----------
        column_name : str
            Name of the column to check
        value : object
            Value to check for existence
        row_group_idx : int, default 0
            Row group index to check
            
        Returns
        -------
        bool
            True if value might exist (bloom filter says "maybe"),
            False if value definitely does not exist
        """
        bloom_filters = self.get_bloom_filters(column_name)
        rg_key = f'row_group_{row_group_idx}'
        
        if rg_key not in bloom_filters:
            return True  # Assume possible if no bloom filter info
        
        bloom_info = bloom_filters[rg_key]
        if not bloom_info['available'] or bloom_info['filter_data'] is None:
            return True  # Assume possible if no bloom filter available
        
        try:
            # Try to use the bloom filter to check the value
            bloom_filter = bloom_info['filter_data']
            if hasattr(bloom_filter, 'check'):
                return bloom_filter.check(value)
            elif hasattr(bloom_filter, 'contains'):
                return bloom_filter.contains(value)
            else:
                return True  # Fallback if we can't check
        except Exception:
            return True  # Assume possible if checking fails


# Module-level convenience functions
def read_parquet(source: Union[BinaryIO, bytes, str, Path], columns=None) -> pa.Table:
    """
    Convenience function to read parquet data from a stream or file.
    
    Parameters
    ----------
    source : BinaryIO, bytes, str, or Path
        Stream, bytes data, or path to the parquet data
    columns : list, optional
        List of columns to read
        
    Returns
    -------
    pyarrow.Table
        Arrow table containing the data
    """
    cdef ParquetDecoder decoder = ParquetDecoder(source)
    return decoder.read_columns(columns=columns)


def get_parquet_info(source: Union[BinaryIO, bytes, str, Path]) -> Dict[str, Any]:
    """
    Get information about parquet data from a stream or file.
    
    Parameters
    ----------
    source : BinaryIO, bytes, str, or Path
        Stream, bytes data, or path to the parquet data
        
    Returns
    -------
    dict
        Dictionary containing file information
    """
    cdef ParquetDecoder decoder = ParquetDecoder(source)
    return decoder.get_metadata()