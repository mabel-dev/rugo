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

static inline const char* LogicalTypeToString(int t) {
    switch (t) {
        case 1: return "STRING";
        case 2: return "MAP";
        case 3: return "LIST";
        case 4: return "ENUM";
        case 5: return "DECIMAL";
        case 6: return "DATE";
        case 7: return "TIME_MILLIS";
        case 8: return "TIME_MICROS";
        case 9: return "TIMESTAMP_MILLIS";
        case 10: return "TIMESTAMP_MICROS";
        case 11: return "UINT_8";
        case 12: return "UINT_16";
        case 13: return "UINT_32";
        case 14: return "UINT_64";
        case 15: return "INT_8";
        case 16: return "INT_16";
        case 17: return "INT_32";
        case 18: return "INT_64";
        case 19: return "JSON";
        case 20: return "BSON";
        case 21: return "INTERVAL";
        default: return "";
    }
}

// ------------------- Schema parsing -------------------

struct SchemaElement {
    std::string name;
    std::string logical_type;
    int num_children = 0;
};

// Parse a LogicalType structure
static std::string ParseLogicalType(TInput& in) {
    int16_t last_id = 0;
    while (true) {
        auto fh = ReadFieldHeader(in, last_id);
        if (fh.type == 0) break;
        
        switch (fh.id) {
            case 1: return "STRING";          // STRING
            case 2: return "MAP";             // MAP
            case 3: return "LIST";            // LIST  
            case 4: return "ENUM";            // ENUM
            case 5: {                         // DECIMAL
                int16_t decimal_last = 0;
                while (true) {
                    auto dfh = ReadFieldHeader(in, decimal_last);
                    if (dfh.type == 0) break;
                    SkipField(in, dfh.type);
                }
                return "DECIMAL";
            }
            case 6: return "DATE";            // DATE
            case 7: {                         // TIME
                int16_t time_last = 0;
                while (true) {
                    auto tfh = ReadFieldHeader(in, time_last);
                    if (tfh.type == 0) break;
                    if (tfh.id == 1) {
                        bool is_adjusted_utc = (in.readByte() != 0);
                        (void)is_adjusted_utc; // unused for now
                    } else if (tfh.id == 2) {
                        int32_t unit = ReadI32(in);
                        if (unit == 0) return "TIME_MILLIS";
                        else if (unit == 1) return "TIME_MICROS";
                        else return "TIME";
                    } else {
                        SkipField(in, tfh.type);
                    }
                }
                return "TIME";
            }
            case 8: {                         // TIMESTAMP
                int16_t ts_last = 0;
                while (true) {
                    auto tsfh = ReadFieldHeader(in, ts_last);
                    if (tsfh.type == 0) break;
                    if (tsfh.id == 1) {
                        bool is_adjusted_utc = (in.readByte() != 0);
                        (void)is_adjusted_utc; // unused for now
                    } else if (tsfh.id == 2) {
                        int32_t unit = ReadI32(in);
                        if (unit == 0) return "TIMESTAMP_MILLIS";
                        else if (unit == 1) return "TIMESTAMP_MICROS";
                        else return "TIMESTAMP";
                    } else {
                        SkipField(in, tsfh.type);
                    }
                }
                return "TIMESTAMP";
            }
            case 9: {                         // INTEGER  
                int16_t int_last = 0;
                while (true) {
                    auto ifh = ReadFieldHeader(in, int_last);
                    if (ifh.type == 0) break;
                    if (ifh.id == 1) {
                        int8_t bit_width = (int8_t)in.readByte();
                        (void)bit_width; // unused for now
                    } else if (ifh.id == 2) {
                        bool is_signed = (in.readByte() != 0);
                        if (is_signed) return "INT";
                        else return "UINT";
                    } else {
                        SkipField(in, ifh.type);
                    }
                }
                return "INT";
            }
            case 10: return "JSON";           // JSON
            case 11: return "BSON";           // BSON
            case 12: return "UUID";           // UUID
            case 13: return "FLOAT16";        // FLOAT16
            default:
                SkipField(in, fh.type);
                break;
        }
    }
    return "";
}

// Parse a SchemaElement 
static SchemaElement ParseSchemaElement(TInput& in) {
    SchemaElement elem;
    int16_t last_id = 0;
    while (true) {
        auto fh = ReadFieldHeader(in, last_id);
        if (fh.type == 0) break;
        
        switch (fh.id) {
            case 1: { // type (Physical type)
                int32_t t = ReadI32(in);
                (void)t; // We don't need physical type here, it's in column metadata
                break;
            }
            case 2: { // type_length (for FIXED_LEN_BYTE_ARRAY)
                int32_t len = ReadI32(in);
                (void)len;
                break;
            }
            case 3: { // repetition_type
                int32_t rep = ReadI32(in);
                (void)rep;
                break;
            }
            case 4: { // name
                elem.name = ReadString(in);
                break;
            }
            case 5: { // num_children
                elem.num_children = ReadI32(in);
                break;
            }
            case 6: { // converted_type (legacy logical type)
                int32_t ct = ReadI32(in);
                elem.logical_type = LogicalTypeToString(ct);
                break;
            }
            case 7: { // scale (for DECIMAL)
                int32_t scale = ReadI32(in);
                (void)scale;
                break;
            }
            case 8: { // precision (for DECIMAL)
                int32_t precision = ReadI32(in);
                (void)precision;
                break;
            }
            case 9: { // field_id
                int32_t field_id = ReadI32(in);
                (void)field_id;
                break;
            }
            case 10: { // logicalType (newer format)
                std::string logical = ParseLogicalType(in);
                if (!logical.empty()) {
                    elem.logical_type = logical;
                }
                break;
            }
            default:
                SkipField(in, fh.type);
                break;
        }
    }
    return elem;
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
    std::unordered_map<std::string, std::string> logical_type_map; // path -> logical_type
    
    int16_t last_id = 0;
    while (true) {
        auto fh = ReadFieldHeader(in, last_id);
        if (fh.type == 0) break;

        switch (fh.id) {
            case 1: { // schema (list<SchemaElement>) - parse to extract logical types
                auto lh = ReadListHeader(in);
                std::vector<SchemaElement> schema_stack;
                
                for (uint32_t i = 0; i < lh.size; i++) {
                    SchemaElement elem = ParseSchemaElement(in);
                    
                    // Build full path for non-root elements
                    if (i > 0 && !elem.name.empty()) { // Skip root element
                        std::string full_path = elem.name;
                        
                        // If this element has a logical type, map it
                        if (!elem.logical_type.empty()) {
                            logical_type_map[full_path] = elem.logical_type;
                        }
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
                    
                    // Apply logical types to columns
                    for (auto& col : rg.columns) {
                        auto it = logical_type_map.find(col.name);
                        if (it != logical_type_map.end()) {
                            col.logical_type = it->second;
                        } else {
                            // Infer common logical types from physical types when not explicitly defined
                            if (col.physical_type == "BYTE_ARRAY") {
                                col.logical_type = "STRING"; // Most BYTE_ARRAY are strings
                            } else if (col.physical_type == "INT96") {
                                col.logical_type = "TIMESTAMP_NANOS"; // INT96 is usually timestamp
                            } else if (col.physical_type == "INT32") {
                                // Could be DATE, TIME, etc. - for now leave empty unless explicitly defined
                                col.logical_type = "";
                            } else if (col.physical_type == "INT64") {
                                // Could be TIMESTAMP_MILLIS/MICROS, for now leave empty unless explicitly defined
                                col.logical_type = "";
                            }
                            // For BOOLEAN, FLOAT, DOUBLE - physical type is usually the logical type too
                        }
                    }
                    
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

// ------------------- Bloom Filter Implementation -------------------

// Simple hash functions for bloom filter (Parquet uses split block bloom filter)
static inline uint32_t Hash1(const std::string& data) {
    uint32_t h = 0x811c9dc5; // FNV-1a 32-bit offset basis
    for (char c : data) {
        h ^= (uint32_t)(unsigned char)c;
        h *= 0x01000193; // FNV-1a 32-bit prime
    }
    return h;
}

static inline uint32_t Hash2(const std::string& data) {
    // Simple alternative hash
    uint32_t h = 5381; // djb2 hash
    for (char c : data) {
        h = ((h << 5) + h) + (uint32_t)(unsigned char)c;
    }
    return h;
}

bool TestBloomFilter(const std::string& file_path, int64_t bloom_offset, int64_t bloom_length, const std::string& value) {
    if (bloom_offset < 0) {
        return false; // No bloom filter
    }
    
    std::ifstream f(file_path, std::ios::binary | std::ios::ate);
    if (!f.is_open()) {
        return false;
    }
    
    size_t file_size = f.tellg();
    
    // If bloom_length is not provided, we need to calculate it
    int64_t actual_bloom_length = bloom_length;
    if (actual_bloom_length <= 0) {
        // Try to read bloom filter header to determine size
        f.seekg(bloom_offset);
        if (bloom_offset + 12 > (int64_t)file_size) {
            return false; // Not enough space for header
        }
        
        uint8_t header[12];
        f.read((char*)header, 12);
        
        if (!f.good()) {
            return false;
        }
        
        // Parse bloom filter header to determine actual length
        uint32_t num_hash_functions = ReadLE32(header);
        uint32_t num_blocks = ReadLE32(header + 4);
        
        if (num_hash_functions == 0 || num_blocks == 0 || num_hash_functions > 10 || num_blocks > 1024) {
            // Invalid or unreasonable values, try alternative interpretation
            // Some bloom filters might be structured differently
            actual_bloom_length = 1024; // Use a reasonable default
        } else {
            // Calculate length: header + (32 bytes per block)
            actual_bloom_length = 12 + (num_blocks * 32);
        }
    }
    
    // Read the bloom filter data
    f.seekg(bloom_offset);
    std::vector<uint8_t> bloom_data(actual_bloom_length);
    f.read((char*)bloom_data.data(), actual_bloom_length);
    
    if (!f.good()) {
        return false;
    }
    
    // Parse bloom filter header
    if (actual_bloom_length < 12) {
        return false; // Too small to be valid
    }
    
    const uint8_t* data = bloom_data.data();
    uint32_t num_hash_functions = ReadLE32(data);
    uint32_t num_blocks = ReadLE32(data + 4);
    
    if (num_hash_functions == 0 || num_blocks == 0 || num_hash_functions > 10 || num_blocks > 1024) {
        return false; // Invalid bloom filter
    }
    
    // Simple bloom filter test using Parquet's split block bloom filter approach
    uint32_t h1 = Hash1(value);
    uint32_t h2 = Hash2(value);
    
    size_t bits_per_block = 256; // Standard for Parquet bloom filters
    size_t block_size = bits_per_block / 8; // 32 bytes per block
    
    if (actual_bloom_length < (int64_t)(12 + num_blocks * block_size)) {
        return false; // Not enough data
    }
    
    const uint8_t* blocks_data = data + 12; // Skip header
    
    for (uint32_t i = 0; i < num_hash_functions; i++) {
        uint32_t hash = h1 + i * h2;
        uint32_t block_idx = hash % num_blocks;
        uint32_t bit_idx = (hash / num_blocks) % bits_per_block;
        
        const uint8_t* block = blocks_data + block_idx * block_size;
        uint32_t byte_idx = bit_idx / 8;
        uint32_t bit_offset = bit_idx % 8;
        
        if (byte_idx >= block_size) continue; // Safety check
        
        if (!(block[byte_idx] & (1 << bit_offset))) {
            return false; // Definitely not present
        }
    }
    
    return true; // Might be present
}