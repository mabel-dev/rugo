#pragma once
#include <cstdint>
#include <string>
#include <stdexcept>

struct TInput {
    const uint8_t* p;
    const uint8_t* end;

    uint8_t readByte() {
        if (p >= end) throw std::runtime_error("EOF");
        return *p++;
    }
};

// ------------------- Varint / ZigZag -------------------

inline uint64_t ReadVarint(TInput& in) {
    uint64_t result = 0;
    int shift = 0;
    while (true) {
        uint8_t byte = in.readByte();
        result |= (uint64_t)(byte & 0x7F) << shift;
        if (!(byte & 0x80)) break;
        shift += 7;
    }
    return result;
}

inline int64_t ZigZagDecode(uint64_t n) {
    return (n >> 1) ^ -(int64_t)(n & 1);
}

inline int64_t ReadI64(TInput& in) {
    return ZigZagDecode(ReadVarint(in));
}

inline int32_t ReadI32(TInput& in) {
    return (int32_t)ZigZagDecode(ReadVarint(in));
}

inline std::string ReadString(TInput& in) {
    uint64_t len = ReadVarint(in);
    if (in.p + len > in.end) throw std::runtime_error("Invalid string length");
    std::string s((const char*)in.p, len);
    in.p += len;
    return s;
}

// ------------------- Compact Protocol Structs -------------------

struct FieldHeader {
    int16_t id;
    uint8_t type;
};

// Decode a field header (handles delta encoding)
inline FieldHeader ReadFieldHeader(TInput& in, int16_t& last_id) {
    uint8_t header = in.readByte();
    if (header == 0) {
        return {-1, 0}; // STOP
    }
    uint8_t type = header & 0x0F;
    int16_t field_id;
    uint8_t modifier = header >> 4;
    if (modifier == 0) {
        field_id = (int16_t)ReadVarint(in);
    } else {
        field_id = last_id + modifier;
    }
    last_id = field_id;
    return {field_id, type};
}

// Compact list header
struct ListHeader {
    uint8_t elem_type;
    uint32_t size;
};

inline ListHeader ReadListHeader(TInput& in) {
    uint8_t first = in.readByte();
    uint32_t size = first >> 4;
    uint8_t elem_type = first & 0x0F;
    if (size == 15) {
        size = (uint32_t)ReadVarint(in);
    }
    return {elem_type, size};
}

inline void SkipField(TInput& in, uint8_t type) {
    switch (type) {
    case 0: return;                      // STOP
    case 1: case 2: return;              // BOOL
    case 3: in.readByte(); return;       // BYTE
    case 4: (void)ReadI32(in); return;   // I16 zigzag
    case 5: (void)ReadI32(in); return;   // I32 zigzag
    case 6: (void)ReadI64(in); return;   // I64 zigzag
    case 7: in.p += 8; return;           // DOUBLE
    case 8: (void)ReadString(in); return;// BINARY/STRING
    case 9: { auto lh = ReadListHeader(in);
              for (uint32_t i=0;i<lh.size;i++) SkipField(in, lh.elem_type);
              return; }
    case 10: { auto lh = ReadListHeader(in);                 // SET same as LIST
               for (uint32_t i=0;i<lh.size;i++) SkipField(in, lh.elem_type);
               return; }
    case 11: { // MAP: size + key/val types
               uint8_t first = in.readByte();
               uint32_t size = first >> 4;
               uint8_t key_type = 0, val_type = 0;
               if (size != 0) {
                   if (size == 15) size = (uint32_t)ReadVarint(in);
                   uint8_t types = in.readByte();
                   key_type = types >> 4;
                   val_type = types & 0x0F;
                   for (uint32_t i=0;i<size;i++) {
                       SkipField(in, key_type);
                       SkipField(in, val_type);
                   }
               }
               return; }
    case 12: { // STRUCT
               int16_t last = 0;
               while (true) {
                   auto fh = ReadFieldHeader(in, last);
                   if (fh.type == 0) break;
                   SkipField(in, fh.type);
               }
               return; }
    default:
        // Be forgiving: consume one byte to move on
        in.readByte();
        return;
    }
}