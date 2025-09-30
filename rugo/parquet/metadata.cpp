#include "thrift.hpp"
#include "metadata.hpp"
#include <fstream>
#include <cstring>
#include <stdexcept>
#include <iostream>

// ------------------- Helpers -------------------

static inline uint32_t ReadLE32(const uint8_t* p) {
    return (uint32_t)p[0]
         | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16)
         | ((uint32_t)p[3] << 24);
}

static inline const char* ParquetTypeToString(int t) {
    switch (t) {
        case 0: return "boolean";
        case 1: return "int32";
        case 2: return "int64";
        case 3: return "int96";
        case 4: return "float32";
        case 5: return "float64";
        case 6: return "byte_array";
        case 7: return "fixed_len_byte_array";
        default: return "unknown";
    }
}

static inline const char* LogicalTypeToString(int t) {
    switch (t) {
        case 0: return "varchar";  // UTF8 -> varchar
        case 1: return "MAP";
        case 2: return "LIST";
        case 3: return "ENUM";
        case 4: return "DECIMAL";
        case 5: return "DATE";
        case 6: return "TIME_MILLIS";
        case 7: return "TIME_MICROS";
        case 8: return "TIMESTAMP_MILLIS";
        case 9: return "TIMESTAMP_MICROS";
        case 10: return "UINT_8";
        case 11: return "UINT_16";
        case 12: return "UINT_32";
        case 13: return "UINT_64";
        case 14: return "INT_8";
        case 15: return "INT_16";
        case 16: return "INT_32";
        case 17: return "INT_64";
        case 18: return "JSON";
        case 19: return "BSON";
        case 20: return "INTERVAL";
        default: return "";
    }
}

static inline std::string CanonicalizeColumnName(std::string name) {
    if (name.rfind("schema.", 0) == 0) {
        name.erase(0, 7); // strip schema.
    }
    if (name.size() >= 13 && name.compare(name.size()-13, 13, ".list.element") == 0) {
        name.erase(name.size()-13);
    } else if (name.size() >= 10 && name.compare(name.size()-10, 10, ".list.item") == 0) {
        name.erase(name.size()-10);
    }
    return name;
}

// ------------------- Schema parsing -------------------

struct SchemaElement {
    std::string name;
    std::string logical_type;
    int num_children = 0;
    int32_t type_length = 0;   // for FIXED_LEN_BYTE_ARRAY (e.g. flba5)
    int32_t scale = 0;         // for DECIMAL
    int32_t precision = 0;     // for DECIMAL
};

// Correct logical type structure parsing
static std::string ParseLogicalType(TInput& in) {
    std::string result;
    int16_t last_id = 0;

    while (true) {
        auto fh = ReadFieldHeader(in, last_id);
        if (fh.type == 0) break;

        switch (fh.id) {
            case 1: { // STRING (StringType - empty struct)
                SkipStruct(in); // Just skip the empty StringType struct
                result = "varchar";  // Use varchar for STRING type
                break;
            }
            case 2: { // MAP (MapType - empty struct)
                SkipStruct(in);
                result = "map";
                break;
            }
            case 3: { // LIST (ListType - empty struct)
                SkipStruct(in);
                result = "array";
                break;
            }
            case 4: { // ENUM (EnumType - empty struct)
                SkipStruct(in);
                result = "enum";
                break;
            }
            case 5: { // DECIMAL (DecimalType)
                int32_t scale = 0, precision = 0;
                int16_t decimal_last = 0;
                while (true) {
                    auto inner = ReadFieldHeader(in, decimal_last);
                    if (inner.type == 0) break;
                    if (inner.id == 1) scale = ReadI32(in);
                    else if (inner.id == 2) precision = ReadI32(in);
                    else SkipField(in, inner.type);
                }
                result = "decimal(" + std::to_string(precision) + "," + std::to_string(scale) + ")";
                break;
            }
            case 6: { // DATE (DateType - empty struct)
                SkipStruct(in);
                result = "date32[day]";
                break;
            }
            case 7: { // TIME (TimeType)
                int16_t time_last = 0;
                bool isAdjustedToUTC = false;
                std::string unit = "ms";
                while (true) {
                    auto inner = ReadFieldHeader(in, time_last);
                    if (inner.type == 0) break;
                    if (inner.id == 1) isAdjustedToUTC = (ReadBool(in) != 0);
                    else if (inner.id == 2) { // unit
                        int16_t unit_last = 0;
                        while (true) {
                            auto unit_fh = ReadFieldHeader(in, unit_last);
                            if (unit_fh.type == 0) break;
                            if (unit_fh.id == 1) { // MILLISECONDS
                                SkipStruct(in);
                                unit = "ms"; 
                            } else if (unit_fh.id == 2) { // MICROSECONDS
                                SkipStruct(in);
                                unit = "us";
                            } else if (unit_fh.id == 3) { // NANOSECONDS
                                SkipStruct(in);
                                unit = "ns";
                            } else {
                                SkipField(in, unit_fh.type);
                            }
                        }
                    } else {
                        SkipField(in, inner.type);
                    }
                }
                result = "time[" + unit + (isAdjustedToUTC ? ",UTC" : "") + "]";
                SkipStruct(in);
                break;
            }
            case 8: { // TIMESTAMP (TimestampType)
                int16_t ts_last = 0;
                bool isAdjustedToUTC = false;
                std::string unit = "ms";
                while (true) {
                    auto inner = ReadFieldHeader(in, ts_last);
                    if (inner.type == 0) break;
                    if (inner.id == 1) isAdjustedToUTC = (ReadBool(in) != 0);
                    else if (inner.id == 2) { // unit
                        int16_t unit_last = 0;
                        while (true) {
                            auto unit_fh = ReadFieldHeader(in, unit_last);
                            if (unit_fh.type == 0) break;
                            if (unit_fh.id == 1) { // MILLISECONDS
                                SkipStruct(in);
                                unit = "ms";
                            } else if (unit_fh.id == 2) { // MICROSECONDS
                                SkipStruct(in);
                                unit = "us";
                            } else if (unit_fh.id == 3) { // NANOSECONDS
                                SkipStruct(in);
                                unit = "ns";
                            } else {
                                SkipField(in, unit_fh.type);
                            }
                        }
                    } else {
                        SkipField(in, inner.type);
                    }
                }
                result = "timestamp[" + unit + (isAdjustedToUTC ? ",UTC" : "") + "]";
                SkipStruct(in);
                break;
            }
            case 10: { // INTEGER (IntType)
                int16_t int_last = 0;
                int8_t bitWidth = 0;
                bool isSigned = true;

                while (true) {
                    auto inner = ReadFieldHeader(in, int_last);
                    if (inner.type == 0) break;  // STOP

                    if (inner.id == 1) {
                        // bitWidth is just a single byte
                        bitWidth = static_cast<int8_t>(in.readByte());
                    } else if (inner.id == 2) {
                        if (inner.type == T_BOOL_TRUE) {
                            isSigned = true;
                        } else if (inner.type == T_BOOL_FALSE) {
                            isSigned = false;
                        } else {
                            isSigned = ReadBool(in);
                        }
                    } else {
                        SkipField(in, inner.type); // future-proof
                    }
                }

                result = (isSigned ? "int" : "uint") + std::to_string((int)bitWidth);
                break;
            }
            case 11: { // UNKNOWN (NullType - empty)
                SkipStruct(in);
                result = "unknown";
                break;
            }
            case 12: { // JSON (JsonType - empty)
                SkipStruct(in);
                result = "json";
                break;
            }
            case 13: { // BSON (BsonType - empty)
                SkipStruct(in);
                result = "bson";
                break;
            }
            case 15: { // FLOAT16 (Float16Type - empty struct)
                SkipStruct(in);  // it’s defined as an empty struct
                result = "float16";
                break;
            }
            default:
                std::cerr << "Skipping unknown logical type id " << fh.id << " type " << (int)fh.type << "\n";   
                SkipField(in, fh.type);
                break;
        }
    }

    return result;
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
                elem.type_length = len;
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
                if (elem.logical_type.empty()) {
                    elem.logical_type = LogicalTypeToString(ct);
                }
                break;
            }
            case 7: { // scale (for DECIMAL)
                int32_t scale = ReadI32(in);
                elem.scale = scale;
                break;
            }
            case 8: { // precision (for DECIMAL)
                int32_t precision = ReadI32(in);
                elem.precision = precision;
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
            case 3: {
                auto lh = ReadListHeader(in);
                std::string name;
                for (uint32_t i = 0; i < lh.size; i++) {
                    std::string part = ReadString(in);
                    if (!name.empty()) name.push_back('.');
                    name += part;
                }
                cs.name = CanonicalizeColumnName(std::move(name));
                break;
            }
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

// ------------------- Schema Walker -------------------

static inline bool EndsWith(const std::string& s, const char* suf) {
    const size_t n = std::strlen(suf);
    return s.size() >= n && std::memcmp(s.data() + s.size() - n, suf, n) == 0;
}

// Walk schema tree recursively
static void WalkSchema(TInput& in, int remaining,
                       int indent,
                       std::string parent_path,
                       std::unordered_map<std::string, std::string>& logical_type_map) {
    for (int i = 0; i < remaining; i++) {
        SchemaElement elem = ParseSchemaElement(in);

        const std::string path = parent_path.empty() ? elem.name : parent_path + "." + elem.name;

        // Collapse Parquet LIST encodings by name (no physical type required).
        // Handle both canonical ".list.element" and legacy ".list.item".
        if (EndsWith(path, ".list.element") || EndsWith(path, ".list.item")) {
            const char* suffix = EndsWith(path, ".list.element") ? ".list.element" : ".list.item";
            const std::size_t base_len = path.size() - std::strlen(suffix);
            const std::string base = path.substr(0, base_len);

            // Prefer the leaf's logical type (e.g., STRING). If absent, we can only mark unknown.
            const std::string child_logical = !elem.logical_type.empty() ? elem.logical_type : "?>";

            // Emit BOTH keys so exact-path lookups succeed.
            logical_type_map[base] = "array<" + child_logical + ">";
            logical_type_map[path] = "array<" + child_logical + ">";

            continue;
        }

        // Special handling for DECIMAL and FIXED_LEN_BYTE_ARRAY to include parameters.
        if (elem.logical_type == "DECIMAL") {
            elem.logical_type = "decimal128(" + std::to_string(elem.precision) + "," +
                                    std::to_string(elem.scale) + ")";
        } else if (elem.logical_type.empty() &&
                elem.type_length > 0) {
            elem.logical_type = "fixed_len_byte_array[" + std::to_string(elem.type_length) + "]";
        } else {
            elem.logical_type = elem.logical_type;
        }

        // Normal emission for non-list nodes (only if a logical type is present).
        if (!elem.logical_type.empty()) {
            logical_type_map[path] = elem.logical_type;
        }

        // Recurse into children.
        if (elem.num_children > 0) {
            WalkSchema(in, elem.num_children, indent + 1, path, logical_type_map);
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
            case 2: { // schema (list<SchemaElement>)
                ReadListHeader(in);
                WalkSchema(in, 1, 0, "", logical_type_map);
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
                        if (it == logical_type_map.end()) {
                            it = logical_type_map.find("schema." + col.name);
                        }
                        // Also try all keys that end with the column name (to handle different root names like "hive_schema")
                        if (it == logical_type_map.end()) {
                            for (const auto& pair : logical_type_map) {
                                if (pair.first.size() > col.name.size() && 
                                    pair.first.compare(pair.first.size() - col.name.size(), col.name.size(), col.name) == 0) {
                                    // Check if it's a proper suffix (preceded by a dot)
                                    if (pair.first[pair.first.size() - col.name.size() - 1] == '.') {
                                        it = logical_type_map.find(pair.first);
                                        break;
                                    }
                                }
                            }
                        }
                        if (it != logical_type_map.end()) {
                            col.logical_type = it->second;
                        } else {
                            // Infer common logical types from physical types when not explicitly defined
                            if (col.physical_type == "int96") {
                                col.logical_type = "timestamp[ns]"; // INT96 is usually timestamp
                            } else if (col.physical_type == "byte_array") {
                                // Default byte_array without logical type to binary
                                col.logical_type = "binary";
                            }
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

FileStats ReadParquetMetadataFromBuffer(const uint8_t* buf, size_t size) {
    if (size < 8) {
        throw std::runtime_error("Buffer too small");
    }

    // trailer is always last 8 bytes
    const uint8_t* trailer = buf + size - 8;

    if (memcmp(trailer + 4, "PAR1", 4) != 0)
        throw std::runtime_error("Not a parquet file");

    uint32_t footer_len = ReadLE32(trailer);
    if (footer_len + 8 > size)
        throw std::runtime_error("Footer length invalid");

    const uint8_t* footer_start = buf + size - 8 - footer_len;
    const uint8_t* footer_end   = buf + size - 8;

    TInput in{footer_start, footer_end};
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