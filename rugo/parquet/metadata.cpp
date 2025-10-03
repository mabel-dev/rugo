#include "metadata.hpp"
#include "thrift.hpp"
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>

// ------------------- Helpers -------------------

static inline uint32_t ReadLE32(const uint8_t *p) {
  return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
         ((uint32_t)p[3] << 24);
}

static inline const char *ParquetTypeToString(int t) {
  switch (t) {
  case 0:
    return "boolean";
  case 1:
    return "int32";
  case 2:
    return "int64";
  case 3:
    return "int96";
  case 4:
    return "float32";
  case 5:
    return "float64";
  case 6:
    return "byte_array";
  case 7:
    return "fixed_len_byte_array";
  default:
    return "unknown";
  }
}

static inline const char *LogicalTypeToString(int t) {
  switch (t) {
  case 0:
    return "varchar"; // UTF8
  case 1:
    return "MAP";
  case 2:
    return "LIST";
  case 3:
    return "ENUM";
  case 4:
    return "DECIMAL";
  case 5:
    return "DATE";
  case 6:
    return "TIME_MILLIS";
  case 7:
    return "TIME_MICROS";
  case 8:
    return "TIMESTAMP_MILLIS";
  case 9:
    return "TIMESTAMP_MICROS";
  case 10:
    return "UINT_8";
  case 11:
    return "UINT_16";
  case 12:
    return "UINT_32";
  case 13:
    return "UINT_64";
  case 14:
    return "INT_8";
  case 15:
    return "INT_16";
  case 16:
    return "INT_32";
  case 17:
    return "INT_64";
  case 18:
    return "JSON";
  case 19:
    return "BSON";
  case 20:
    return "INTERVAL";
  case 21:
    return "struct";
  default:
    return "";
  }
}

static inline std::string CanonicalizeColumnName(std::string name) {
  if (name.rfind("schema.", 0) == 0) {
    name.erase(0, 7); // strip schema.
  }
  if (name.size() >= 13 &&
      name.compare(name.size() - 13, 13, ".list.element") == 0) {
    name.erase(name.size() - 13);
  } else if (name.size() >= 10 &&
             name.compare(name.size() - 10, 10, ".list.item") == 0) {
    name.erase(name.size() - 10);
  }
  return name;
}

// ------------------- Schema parsing -------------------

// Correct logical type structure parsing
static std::string ParseLogicalType(TInput &in) {
  std::string result;
  int16_t last_id = 0;

  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 1: {             // STRING (StringType - empty struct)
      SkipStruct(in);     // Just skip the empty StringType struct
      result = "varchar"; // Use varchar for STRING type
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
        if (inner.type == 0)
          break;
        if (inner.id == 1)
          scale = ReadI32(in);
        else if (inner.id == 2)
          precision = ReadI32(in);
        else
          SkipField(in, inner.type);
      }
      result = "decimal(" + std::to_string(precision) + "," +
               std::to_string(scale) + ")";
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
        if (inner.type == 0)
          break;
        if (inner.id == 1)
          isAdjustedToUTC = (ReadBool(in) != 0);
        else if (inner.id == 2) { // unit
          int16_t unit_last = 0;
          while (true) {
            auto unit_fh = ReadFieldHeader(in, unit_last);
            if (unit_fh.type == 0)
              break;
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
        if (inner.type == 0)
          break;
        if (inner.id == 1)
          isAdjustedToUTC = (ReadBool(in) != 0);
        else if (inner.id == 2) { // unit
          int16_t unit_last = 0;
          while (true) {
            auto unit_fh = ReadFieldHeader(in, unit_last);
            if (unit_fh.type == 0)
              break;
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
        if (inner.type == 0)
          break; // STOP

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
    case 15: {        // FLOAT16 (Float16Type - empty struct)
      SkipStruct(in); // it’s defined as an empty struct
      result = "float16";
      break;
    }
    default:
      std::cerr << "Skipping unknown logical type id " << fh.id << " type "
                << (int)fh.type << "\n";
      SkipField(in, fh.type);
      break;
    }
  }

  return result;
}

// Parse a SchemaElement
static SchemaElement ParseSchemaElement(TInput &in) {
  SchemaElement elem;
  int16_t last_id = 0;
  bool saw_physical_type = false;

  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 1: { // type (Physical type)
      int32_t t = ReadI32(in);
      saw_physical_type = true;
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

  // Detect struct nodes: no physical type, has children, no logical_type
  if (elem.num_children > 0 && !saw_physical_type &&
      elem.logical_type.empty()) {
    elem.logical_type = "struct";
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
static void ParseStatistics(TInput &in, ColumnStats &cs) {
  std::string legacy_min, legacy_max, v2_min, v2_max;
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;
    switch (fh.id) {
    case 1:
      legacy_max = ReadString(in);
      break;
    case 2:
      legacy_min = ReadString(in);
      break;
    case 3:
      cs.null_count = ReadI64(in);
      break;
    case 4:
      cs.distinct_count = ReadI64(in);
      break;
    case 5:
      v2_max = ReadString(in);
      break;
    case 6:
      v2_min = ReadString(in);
      break;
    default:
      SkipField(in, fh.type);
      break;
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
static void ParseColumnMeta(TInput &in, ColumnStats &cs) {
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 1: {
      int32_t t = ReadI32(in);
      cs.physical_type = ParquetTypeToString(t);
      break;
    }
    case 2: { // encodings
      auto lh = ReadListHeader(in);
      for (uint32_t i = 0; i < lh.size; i++) {
        int32_t enc = ReadVarint(in);
        cs.encodings.push_back(enc);
      }
      break;
    }
    case 3: {
      auto lh = ReadListHeader(in);
      std::string name;
      for (uint32_t i = 0; i < lh.size; i++) {
        std::string part = ReadString(in);
        if (!name.empty())
          name.push_back('.');
        name += part;
      }
      cs.name = CanonicalizeColumnName(std::move(name));
      break;
    }
    case 4: {
      cs.codec = ReadI32(in);
      break;
    }
    case 5: {
      cs.num_values = ReadI64(in);
      break;
    }
    case 6: {
      cs.total_uncompressed_size = ReadI64(in);
      break;
    }
    case 7: {
      cs.total_compressed_size = ReadI64(in);
      break;
    }
    case 8: { // key_value_metadata: list<struct>; skip
      auto lh = ReadListHeader(in);
      for (uint32_t i = 0; i < lh.size; i++) {
        int16_t kv_last = 0;
        std::string key, value;
        while (true) {
          auto kvfh = ReadFieldHeader(in, kv_last);
          if (kvfh.type == 0)
            break;
          switch (kvfh.id) {
          case 1:
            key = ReadString(in);
            break;
          case 2:
            value = ReadString(in);
            break;
          default:
            SkipField(in, kvfh.type);
            break;
          }
        }
        if (!key.empty()) {
          cs.key_value_metadata.emplace(std::move(key), std::move(value));
        }
      }
      break;
    }
    case 9: {
      cs.data_page_offset = ReadI64(in);
      break;
    }
    case 10: {
      cs.index_page_offset = ReadI64(in);
      break;
    }
    case 11: {
      cs.dictionary_page_offset = ReadI64(in);
      break;
    }
    case 12: {
      ParseStatistics(in, cs);
      break;
    } // statistics
    case 14: {
      cs.bloom_offset = ReadI64(in);
      break;
    } // bloom_filter_offset (common)
    case 15: {
      cs.bloom_length = ReadI64(in);
      break;
    } // bloom_filter_length (common)
    default:
      SkipField(in, fh.type);
      break;
    }
  }
}

// parse a ColumnChunk, and descend into meta_data when present
static void ParseColumnChunk(TInput &in, ColumnStats &out) {
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;
    switch (fh.id) {
    case 1: {
      (void)ReadString(in);
      break;
    } // file_path
    case 2: {
      (void)ReadI64(in);
      break;
    } // file_offset
    case 3: { // meta_data (ColumnMetaData)
      ParseColumnMeta(in, out);
      break;
    }
    // skip everything else
    default:
      SkipField(in, fh.type);
      break;
    }
  }
}

// FIX: correct RowGroup field IDs (columns=1, total_byte_size=2, num_rows=3)
static void ParseRowGroup(TInput &in, RowGroupStats &rg) {
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 1: { // columns: list<ColumnChunk>
      auto lh = ReadListHeader(in);
      for (uint32_t i = 0; i < lh.size; i++) {
        ColumnStats cs;
        ParseColumnChunk(in, cs); // <-- go via ColumnChunk
        rg.columns.push_back(std::move(cs));
      }
      break;
    }
    case 2:
      rg.total_byte_size = ReadI64(in);
      break;
    case 3:
      rg.num_rows = ReadI64(in);
      break;
    default:
      SkipField(in, fh.type);
      break;
    }
  }
}

// ------------------- Schema Walker -------------------

static std::vector<SchemaElement>
WalkSchema(TInput &in, int remaining, const std::string &parent_path = "") {
  std::vector<SchemaElement> nodes;
  nodes.reserve(remaining);

  for (int i = 0; i < remaining; i++) {
    SchemaElement elem = ParseSchemaElement(in);
    elem.name = parent_path.empty() ? elem.name : parent_path + "." + elem.name;

    if (elem.num_children > 0) {
      elem.children = WalkSchema(in, elem.num_children, elem.name);
    }

    nodes.push_back(std::move(elem));
  }
  return nodes;
}

static void InterpretSchema(const SchemaElement &elem,
                            const std::string &parent_path,
                            std::unordered_map<std::string, std::string> &out) {
  // Build full path
  const std::string path =
      parent_path.empty() ? elem.name : parent_path + "." + elem.name;

  // IMPORTANT: Root struct must NOT collapse; recurse into its children.
  if (elem.logical_type == "struct") {
    if (parent_path.empty()) {
      // Root "schema" node: walk children so we see real columns (identifier,
      // etc.)
      for (const auto &child : elem.children) {
        InterpretSchema(child, path, out);
      }
      return; // handled root; don't also fall through
    } else {
      // Non-root struct columns collapse to json
      out[CanonicalizeColumnName(path)] = "json";
      // return;
    }
  }

  if (elem.logical_type == "array") {
    std::string child_type = "unknown";
    if (!elem.children.empty()) {
      // Walk down until we find a concrete logical type
      const SchemaElement *cur = &elem.children[0];
      while (cur) {
        if (!cur->logical_type.empty() && cur->logical_type != "struct" &&
            cur->logical_type != "array") {
          child_type = cur->logical_type;
          break;
        }
        if (cur->children.empty())
          break;
        cur = &cur->children[0];
      }
    }
    out[CanonicalizeColumnName(path)] = "array<" + child_type + ">";
    return;
  }

  if (elem.logical_type.empty() && elem.type_length > 0) {
    out[path] =
        "fixed_len_byte_array[" + std::to_string(elem.type_length) + "]";
  }

  if (!elem.logical_type.empty()) {
    out[path] = elem.logical_type;
  }

  // Recurse into normal children
  for (const auto &child : elem.children) {
    InterpretSchema(child, path, out);
  }
}

static FileStats ParseFileMeta(TInput &in) {
  FileStats fs;

  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 2: { // schema (list<SchemaElement>)
      ReadListHeader(in);
      fs.schema = WalkSchema(in, 1);
      break;
    }
    case 3:
      fs.num_rows = ReadI64(in);
      break;
    case 4: { // row_groups (list<RowGroup>)
      auto lh = ReadListHeader(in);
      fs.row_groups.reserve(lh.size);
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

static inline bool IsDotPrefixedAncestor(const std::string &ancestor,
                                         const std::string &leaf) {
  // ancestor must be a proper prefix of leaf on a dot boundary: "a.b" is
  // ancestor of "a.b.c"
  return leaf.size() > ancestor.size() &&
         std::memcmp(leaf.data(), ancestor.data(), ancestor.size()) == 0 &&
         leaf[ancestor.size()] == '.';
}

static void ApplyLogicalTypes(
    FileStats &fs,
    const std::unordered_map<std::string, std::string> &logical_type_map) {

  for (auto &rg : fs.row_groups) {
    for (auto &col : rg.columns) {
      // 1) exact
      auto it = logical_type_map.find(col.name);

      // 2) schema.+exact
      if (it == logical_type_map.end()) {
        it = logical_type_map.find("schema." + col.name);
      }

      // 3) suffix match (handles "schema.schema.X" vs "X")
      if (it == logical_type_map.end()) {
        for (const auto &kv : logical_type_map) {
          const std::string &key = kv.first;
          if (key.size() > col.name.size() &&
              key.compare(key.size() - col.name.size(), col.name.size(),
                          col.name) == 0 &&
              key[key.size() - col.name.size() - 1] == '.') {
            it = logical_type_map.find(key);
            break;
          }
        }
      }

      if (it != logical_type_map.end()) {
        col.logical_type = it->second;
        continue;
      }

      // 4) json ancestor propagation (only if no mapping found)
      {
        const std::string &leaf = col.name;
        std::string best_prefix;
        for (const auto &kv : logical_type_map) {
          if (kv.second != "json")
            continue;
          if (IsDotPrefixedAncestor(kv.first, leaf)) {
            if (kv.first.size() > best_prefix.size())
              best_prefix = kv.first; // deepest
          }
        }
        if (!best_prefix.empty()) {
          col.logical_type = "json";
          continue; // don’t override with fallback
        }
      }

      // 5) fallback inference
      if (col.physical_type == "int96") {
        col.logical_type = "timestamp[ns]";
      } else if (col.physical_type == "byte_array") {
        col.logical_type =
            col.logical_type.empty() ? "binary" : col.logical_type;
      } else if (col.physical_type == "fixed_len_byte_array") {
        // Prefer to normalize this in InterpretSchema; otherwise leave as
        // physical fallback.
        col.logical_type = "fixed_len_byte_array";
      } else {
        col.logical_type = col.physical_type;
      }
    }
  }
}

// ------------------- Entry point -------------------

FileStats ReadParquetMetadataFromBuffer(const uint8_t *buf, size_t size) {
  if (size < 8) {
    throw std::runtime_error("Buffer too small");
  }

  // trailer is always last 8 bytes
  const uint8_t *trailer = buf + size - 8;

  if (memcmp(trailer + 4, "PAR1", 4) != 0)
    throw std::runtime_error("Not a parquet file");

  uint32_t footer_len = ReadLE32(trailer);
  if (footer_len + 8 > size)
    throw std::runtime_error("Footer length invalid");

  const uint8_t *footer_start = buf + size - 8 - footer_len;
  const uint8_t *footer_end = buf + size - 8;

  TInput in{footer_start, footer_end};
  FileStats fs = ParseFileMeta(in);

  // Now interpret schema to build logical type map
  std::unordered_map<std::string, std::string> logical_type_map;
  for (auto &elem : fs.schema) {
    InterpretSchema(elem, "", logical_type_map);
  }

  // Apply map to row group columns
  ApplyLogicalTypes(fs, logical_type_map);

  return fs;
}
