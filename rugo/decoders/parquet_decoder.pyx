# cython: language_level=3
# distutils: language = c++

"""
Parquet file decoder implemented in Cython for high-performance parsing.

This module provides functions to decode and parse parquet files efficiently
using PyArrow as the backend while providing a simplified interface.
"""

import numpy as np
cimport numpy as cnp
from typing import Dict, List, Optional, Any, Union
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# Initialize numpy C API
cnp.import_array()

cdef class ParquetDecoder:
    """
    High-performance parquet file decoder using Cython and PyArrow.
    
    This class provides efficient methods to read and decode parquet files
    with minimal memory overhead and fast execution.
    """
    
    cdef object _file_path
    cdef object _parquet_file
    cdef object _metadata
    cdef bint _file_loaded
    
    def __cinit__(self, file_path: Union[str, Path]):
        """
        Initialize the ParquetDecoder with a file path.
        
        Parameters
        ----------
        file_path : str or Path
            Path to the parquet file to decode
        """
        self._file_path = Path(file_path)
        self._parquet_file = None
        self._metadata = None
        self._file_loaded = False
    
    def __dealloc__(self):
        """Clean up resources when decoder is destroyed."""
        self.close()
    
    cpdef void load_file(self):
        """
        Load the parquet file and cache metadata.
        
        Raises
        ------
        FileNotFoundError
            If the specified file does not exist
        ValueError
            If the file is not a valid parquet file
        """
        if not self._file_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {self._file_path}")
        
        try:
            self._parquet_file = pq.ParquetFile(str(self._file_path))
            self._metadata = self._parquet_file.metadata
            self._file_loaded = True
        except Exception as e:
            raise ValueError(f"Invalid parquet file: {e}")
    
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
    
    cpdef cnp.ndarray[cnp.float64_t, ndim=1] read_numeric_column_fast(self, str column_name):
        """
        Fast read of a numeric column into a numpy array.
        
        Parameters
        ----------
        column_name : str
            Name of the numeric column to read
            
        Returns
        -------
        numpy.ndarray
            1D numpy array containing the column data
        """
        cdef object table = self.read_columns([column_name])
        cdef object column = table.column(column_name)
        
        # Convert to numpy array - this is optimized for numeric types
        cdef cnp.ndarray[cnp.float64_t, ndim=1] result = column.to_numpy(zero_copy_only=False).astype(np.float64)
        
        return result
    
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


# Module-level convenience functions
def read_parquet(file_path: Union[str, Path], columns=None) -> pa.Table:
    """
    Convenience function to read a parquet file.
    
    Parameters
    ----------
    file_path : str or Path
        Path to the parquet file
    columns : list, optional
        List of columns to read
        
    Returns
    -------
    pyarrow.Table
        Arrow table containing the data
    """
    cdef ParquetDecoder decoder = ParquetDecoder(file_path)
    return decoder.read_columns(columns=columns)


def get_parquet_info(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Get information about a parquet file.
    
    Parameters
    ----------
    file_path : str or Path
        Path to the parquet file
        
    Returns
    -------
    dict
        Dictionary containing file information
    """
    cdef ParquetDecoder decoder = ParquetDecoder(file_path)
    return decoder.get_metadata()