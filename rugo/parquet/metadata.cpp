#include "thrift.hpp"
#include "metadata.hpp"
#include <fstream>
#include <cstring>
#include <stdexcept>

// ------------------- Helpers -------------------

static inline uint32_t ReadLE32(const uint8_t* p) {
    return (uint32_t)p[0]
         | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16)
         | ((uint32_t)p[3] << 24);
}

static inline const char* ParquetTypeToString(int t) {
    switch (t) {
        case 0: return "BOOLEAN";
        case 1: return "INT32";
        case 2: return "INT64";
        case 3: return "INT96";
        case 4: return "FLOAT";
        case 5: return "DOUBLE";
        case 6: return "BYTE_ARRAY";
        case 7: return "FIXED_LEN_BYTE_ARRAY";
        default: return "UNKNOWN";
    }
}

// ------------------- Parsers -------------------

// parquet.thrift Statistics
// 1: optional binary max
// 2: optional binary min
// 3: optional i64 null_count
// 4: optional i64 distinct_count
// 5: optional binary max_value
// 6: optional binary min_value
static void ParseStatistics(TInput& in, ColumnStats& cs) {
    std::string legacy_min, legacy_max, v2_min, v2_max;
    int16_t last_id = 0;
    while (true) {
        auto fh = ReadFieldHeader(in, last_id);
        if (fh.type == 0) break;
        switch (fh.id) {
            case 1: legacy_max = ReadString(in); break;
            case 2: legacy_min = ReadString(in); break;
            case 3: cs.null_count = ReadI64(in); break;
            case 4: cs.distinct_count = ReadI64(in); break;
            case 5: v2_max = ReadString(in); break;
            case 6: v2_min = ReadString(in); break;
            default: SkipField(in, fh.type); break;
        }
    }
    cs.min = !v2_min.empty() ? v2_min : legacy_min;
    cs.max = !v2_max.empty() ? v2_max : legacy_max;
}

// parquet.thrift ColumnMetaData
//  1: required Type type
//  2: required list<Encoding> encodings
//  3: required list<string> path_in_schema
//  4: required CompressionCodec codec
//  5: required i64 num_values
//  6: required i64 total_uncompressed_size
//  7: required i64 total_compressed_size
//  8: optional KeyValueMetaData key_value_metadata
//  9: optional i64 data_page_offset
// 10: optional i64 index_page_offset
// 11: optional i64 dictionary_page_offset
// 12: optional Statistics statistics
// 13: optional list<PageEncodingStats> encoding_stats
// 14+: later additions; Bloom filter fields are commonly (per spec updates):
//      14: optional i64 bloom_filter_offset
//      15: optional i64 bloom_filter_length
static void ParseColumnMeta(TInput& in, ColumnStats& cs) {
    int16_t last_id = 0;
    while (true) {
        auto fh = ReadFieldHeader(in, last_id);
        if (fh.type == 0) break;

        switch (fh.id) {
            case 1: { int32_t t = ReadI32(in); cs.physical_type = ParquetTypeToString(t); break; }
            case 2: { auto lh = ReadListHeader(in);
                      for (uint32_t i = 0; i < lh.size; i++) ReadVarint(in);
                      break; }
            case 3: { auto lh = ReadListHeader(in);
                      std::string name;
                      for (uint32_t i = 0; i < lh.size; i++) {
                          std::string part = ReadString(in);
                          if (!name.empty()) name.push_back('.');
                          name += part;
                      }
                      cs.name = std::move(name);
                      break; }
            case 4: { (void)ReadI32(in); break; }            // codec (unused)
            case 5: { (void)ReadI64(in); break; }            // num_values
            case 6: { (void)ReadI64(in); break; }            // total_uncompressed_size
            case 7: { (void)ReadI64(in); break; }            // total_compressed_size
            case 8: { // key_value_metadata: list<struct>; skip
                      auto lh = ReadListHeader(in);
                      for (uint32_t i = 0; i < lh.size; i++) {
                          int16_t kv_last = 0;
                          while (true) {
                              auto kvfh = ReadFieldHeader(in, kv_last);
                              if (kvfh.type == 0) break;
                              SkipField(in, kvfh.type);
                          }
                      }
                      break; }
            case 9:  { (void)ReadI64(in); break; }           // data_page_offset
            case 10: { (void)ReadI64(in); break; }           // index_page_offset
            case 11: { (void)ReadI64(in); break; }           // dictionary_page_offset
            case 12: { ParseStatistics(in, cs); break; }     // statistics
            case 14: { cs.bloom_offset  = ReadI64(in); break; } // bloom_filter_offset (common)
            case 15: { cs.bloom_length  = ReadI64(in); break; } // bloom_filter_length (common)
            default:
                SkipField(in, fh.type);
                break;
        }
    }
}

// NEW: parse a ColumnChunk, and descend into meta_data when present
static void ParseColumnChunk(TInput& in, ColumnStats &out) {
    int16_t last_id = 0;
    while (true) {
        auto fh = ReadFieldHeader(in, last_id);
        if (fh.type == 0) break;
        switch (fh.id) {
            case 1: { (void)ReadString(in); break; }         // file_path
            case 2: { (void)ReadI64(in); break; }            // file_offset
            case 3: {                                        // meta_data (ColumnMetaData)
                ParseColumnMeta(in, out);
                break;
            }
            // skip everything else
            default: SkipField(in, fh.type); break;
        }
    }
}

// FIX: correct RowGroup field IDs (columns=1, total_byte_size=2, num_rows=3)
static void ParseRowGroup(TInput& in, RowGroupStats& rg) {
    int16_t last_id = 0;
    while (true) {
        auto fh = ReadFieldHeader(in, last_id);
        if (fh.type == 0) break;

        switch (fh.id) {
            case 1: { // columns: list<ColumnChunk>
                auto lh = ReadListHeader(in);
                for (uint32_t i = 0; i < lh.size; i++) {
                    ColumnStats cs;
                    ParseColumnChunk(in, cs);     // <-- go via ColumnChunk
                    rg.columns.push_back(std::move(cs));
                }
                break;
            }
            case 2: rg.total_byte_size = ReadI64(in); break;
            case 3: rg.num_rows = ReadI64(in); break;
            default:
                SkipField(in, fh.type);
                break;
        }
    }
}

static FileStats ParseFileMeta(TInput& in) {
    FileStats fs;
    int16_t last_id = 0;
    while (true) {
        auto fh = ReadFieldHeader(in, last_id);
        if (fh.type == 0) break;

        switch (fh.id) {
            case 1: { // schema (list<SchemaElement>) - skip entirely
                auto lh = ReadListHeader(in);
                for (uint32_t i = 0; i < lh.size; i++) {
                    // skip SchemaElement struct
                    int16_t s_last = 0;
                    while (true) {
                        auto sfh = ReadFieldHeader(in, s_last);
                        if (sfh.type == 0) break;
                        SkipField(in, sfh.type);
                    }
                }
                break;
            }
            case 3: fs.num_rows = ReadI64(in); break;
            case 4: { // row_groups
                auto lh = ReadListHeader(in);
                for (uint32_t i = 0; i < lh.size; i++) {
                    RowGroupStats rg;
                    ParseRowGroup(in, rg);
                    fs.row_groups.push_back(std::move(rg));
                }
                break;
            }
            default:
                SkipField(in, fh.type);
                break;
        }
    }
    return fs;
}

// ------------------- Entry point -------------------

FileStats ReadParquetMetadata(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f.is_open()) throw std::runtime_error("Failed to open file");

    size_t file_size = f.tellg();

    f.seekg(file_size - 8);
    uint8_t trailer[8];
    f.read((char*)trailer, 8);

    if (memcmp(trailer + 4, "PAR1", 4) != 0)
        throw std::runtime_error("Not a parquet file");

    uint32_t footer_len = ReadLE32(trailer);
    std::vector<uint8_t> footer(footer_len);

    f.seekg(file_size - 8 - footer_len);
    f.read((char*)footer.data(), footer_len);

    TInput in{footer.data(), footer.data() + footer.size()};
    return ParseFileMeta(in);
}