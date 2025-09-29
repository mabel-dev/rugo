#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>

struct LogicalTypeInfo {
    std::string type_name;        // e.g. "STRING", "TIMESTAMP_MILLIS", "DECIMAL"
    // Additional logical type parameters could be added here if needed
};

struct ColumnStats {
    std::string name;             // joined path_in_schema: "a.b.c"
    std::string physical_type;    // e.g. "INT64", "BYTE_ARRAY"
    std::string logical_type;     // e.g. "STRING", "TIMESTAMP_MILLIS", "DECIMAL"
    std::string min;              // min_value if present, else min (raw bytes)
    std::string max;              // max_value if present, else max (raw bytes)
    int64_t null_count = -1;
    int64_t distinct_count = -1;
    int64_t bloom_offset = -1;
    int64_t bloom_length = -1;
};

struct RowGroupStats {
    int64_t num_rows = 0;
    int64_t total_byte_size = 0;
    std::vector<ColumnStats> columns;
};

struct FileStats {
    int64_t num_rows = 0;
    std::vector<RowGroupStats> row_groups;
};

FileStats ReadParquetMetadata(const std::string& path);
FileStats ReadParquetMetadataFromBuffer(const uint8_t* buf, size_t size);

inline FileStats ReadParquetMetadataC(const char* path) {
    return ReadParquetMetadata(std::string(path));
}

// New functions for bloom filter testing
bool TestBloomFilter(const std::string& file_path, int64_t bloom_offset, int64_t bloom_length, const std::string& value);