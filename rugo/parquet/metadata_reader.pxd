# parquet_meta.pxd
from libc.stdint cimport uint8_t
from libcpp.string cimport string
from libcpp.vector cimport vector


cdef extern from "metadata.hpp":
    cdef cppclass ColumnStats:
        string name
        string physical_type
        string logical_type
        string min
        string max
        long long null_count
        long long bloom_offset
        long long bloom_length

    cdef cppclass RowGroupStats:
        long long num_rows
        long long total_byte_size
        vector[ColumnStats] columns

    cdef cppclass FileStats:
        long long num_rows
        vector[RowGroupStats] row_groups

    FileStats ReadParquetMetadataC(const char* path)
    FileStats ReadParquetMetadataFromBuffer(const uint8_t* buf, size_t size)
    bint TestBloomFilter(const string& file_path, long long bloom_offset, long long bloom_length, const string& value)
