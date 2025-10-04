# parquet_meta.pxd
from libc.stdint cimport uint8_t, int32_t, int64_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map


cdef extern from "metadata.hpp":
    cdef cppclass ColumnStats:
        string name
        string physical_type
        string logical_type
        
        # Sizes & counts
        int64_t num_values
        int64_t total_uncompressed_size
        int64_t total_compressed_size
        
        # Offsets
        int64_t data_page_offset
        int64_t index_page_offset
        int64_t dictionary_page_offset
        
        # Statistics
        string min
        string max
        int64_t null_count
        int64_t distinct_count
        
        # Bloom filter
        int64_t bloom_offset
        int64_t bloom_length
        
        # Encodings & codec
        vector[int32_t] encodings
        int32_t codec
        
        # Key/value metadata
        unordered_map[string, string] key_value_metadata

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
    
    # Helper functions
    const char* EncodingToString(int32_t enc)
    const char* CompressionCodecToString(int32_t codec)
