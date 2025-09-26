#pragma once
#include <cstdint>
#include <string>
#include <vector>

struct ColumnStats {
    std::string name;             // joined path_in_schema: "a.b.c"
    std::string physical_type;    // e.g. "INT64", "BYTE_ARRAY"
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