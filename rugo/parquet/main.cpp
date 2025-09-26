#include "metadata.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>

auto dump_bytes = [](const std::string& s) {
    bool printable = true;
    for (unsigned char c : s) { 
        if (c < 32 || c > 126) { 
            printable = false; 
            break; 
        } 
    }
    if (printable) return s;
    std::string hex; 
    hex.reserve(s.size()*2);
    static const char* H = "0123456789abcdef";
    for (unsigned char c : s) { 
        hex.push_back(H[c>>4]); 
        hex.push_back(H[c&15]); 
    }
    return std::string("0x") + hex;
};

template<typename T>
static std::string HexOrValue(const std::string &bytes) {
    if (bytes.size() == sizeof(T)) {
        T v;
        memcpy(&v, bytes.data(), sizeof(T));
        return std::to_string(v);
    }
    // fallback to hex
    std::ostringstream ss;
    ss << "0x";
    for (unsigned char c : bytes) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)c;
    }
    return ss.str();
}

static std::string format_stat(const ColumnStats& col, const std::string& value) {
    if (value.empty()) return "";

    const std::string& t = col.physical_type;

    if (t == "INT32") {
        return HexOrValue<int32_t>(value);
    } else if (t == "INT64") {
        return HexOrValue<int64_t>(value);
    } else if (t == "FLOAT") {
        return HexOrValue<float>(value);
    } else if (t == "DOUBLE") {
        return HexOrValue<double>(value);
    } else {
        // strings and other types
        return dump_bytes(value);
    }
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: ./parquet_meta <file.parquet>\n";
        return 1;
    }

    FileStats stats = ReadParquetMetadata(argv[1]);

    std::cout << "Num rows: " << stats.num_rows << "\n";

    for (size_t i = 0; i < stats.row_groups.size(); i++) {
        auto& rg = stats.row_groups[i];
        std::cout << " RowGroup " << i 
                  << " rows=" << rg.num_rows
                  << " bytes=" << rg.total_byte_size
                  << " cols=" << rg.columns.size() << "\n";

        for (size_t j = 0; j < rg.columns.size(); j++) {
            auto& col = rg.columns[j];
            std::string min_str = format_stat(col, col.min);
            std::string max_str = format_stat(col, col.max);

            std::cout << "  Column " << j
                      << " name=" << col.name
                      << " type=" << col.physical_type
                      << " min=" << min_str
                      << " max=" << max_str
                      << " nulls=" << col.null_count
                      << " bloom_offset=" << col.bloom_offset
                      << " bloom_length=" << col.bloom_length
                      << "\n";
        }
    }

    return 0;
}