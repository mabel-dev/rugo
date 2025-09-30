# distutils: language = c++
# distutils: extra_compile_args = -Wno-unreachable-code-fallthrough

import datetime
import struct

cimport metadata_reader
from libc.stdint cimport uint8_t
from libcpp.string cimport string


# --- value decoder ---
cdef object decode_value(string physical_type, string logical_type, string raw):
    cdef bytes b = raw
    if b is None:
        return None
    if len(b) == 0:
        return b""   # treat empty as empty, not None

    # Decode the C++ string to Python string for comparison
    cdef str type_str = physical_type.decode("utf-8")
    cdef str logical_str = logical_type.decode("utf-8") if logical_type.size() > 0 else ""

    try:
        if type_str == "int32":
            return struct.unpack("<i", b)[0]
        elif type_str == "int64":
            return struct.unpack("<q", b)[0]
        elif type_str == "float32":
            return struct.unpack("<f", b)[0]
        elif type_str == "float64":
            return struct.unpack("<d", b)[0]
        elif type_str in ("byte_array", "fixed_len_byte_array"):
            # If logical type indicates UTF-8 string, decode it
            # Handle "varchar" (new format) and legacy "UTF8" format
            # Also handle array<string> and array<varchar> - the elements are UTF-8 strings
            if logical_str in ("varchar", "UTF8", "JSON", "BSON", "ENUM") or logical_str.startswith("array<string") or logical_str.startswith("array<varchar"):
                try:
                    return b.decode("utf-8")
                except UnicodeDecodeError:
                    # If UTF-8 decoding fails, return as bytes
                    return b
            # Otherwise, return raw bytes (binary data)
            return b
        elif type_str == "int96":
            if len(b) == 12:
                lo, hi = struct.unpack("<qI", b)
                julian_day = hi
                nanos = lo
                # convert Julian day
                days = julian_day - 2440588
                date = datetime.date(1970, 1, 1) + datetime.timedelta(days=days)
                seconds = nanos // 1_000_000_000
                micros = (nanos % 1_000_000_000) // 1000
                return f"{date.isoformat()} {seconds:02d}:{(micros/1e6):.6f}"
            return b.hex()
        elif type_str == "boolean":
            # Parquet encodes boolean as 1 bit, usually in a byte
            return b[0] != 0
        else:
            return b.hex()
    except Exception:
        return b.hex()


def read_metadata(str path):
    """Read parquet metadata from a file path (delegates to buffer version)."""
    with open(path, "rb") as f:
        data = f.read()
    return read_metadata_from_bytes(data)


def read_metadata_from_bytes(bytes data):
    """Read parquet metadata from an in-memory bytes object."""
    cdef const uint8_t* buf = <const uint8_t*> data
    cdef size_t size = len(data)
    return _read_metadata_common(buf, size)


def read_metadata_from_memoryview(memoryview mv):
    """Read parquet metadata from a Python memoryview (zero-copy)."""
    if not mv.contiguous:
        raise ValueError("Memoryview must be contiguous")

    cdef memoryview[uint8_t] mv_bytes = mv.cast('B')  # keep reference alive
    cdef const uint8_t* buf = &mv_bytes[0]
    cdef size_t size = mv_bytes.nbytes

    return _read_metadata_common(buf, size)


cdef object _read_metadata_common(const uint8_t* buf, size_t size):
    cdef metadata_reader.FileStats fs
    fs = metadata_reader.ReadParquetMetadataFromBuffer(buf, size)

    result = {
        "num_rows": fs.num_rows,
        "row_groups": []
    }
    for rg in fs.row_groups:
        rg_dict = {
            "num_rows": rg.num_rows,
            "total_byte_size": rg.total_byte_size,
            "columns": []
        }
        for col in rg.columns:
            if col.logical_type.size() > 0:
                logical_type_str = col.logical_type.decode("utf-8")
            else:
                logical_type_str = ""
            
            # Convert -1 to None for missing stats
            null_count = col.null_count if col.null_count >= 0 else None
            
            # Decode min/max, treating empty strings as None (no stats)
            min_val = decode_value(col.physical_type, col.logical_type, col.min) if col.min.size() > 0 else None
            max_val = decode_value(col.physical_type, col.logical_type, col.max) if col.max.size() > 0 else None
            
            rg_dict["columns"].append({
                "name": col.name.decode("utf-8"),
                "type": col.physical_type.decode("utf-8"),
                "logical_type": logical_type_str,
                "min": min_val,
                "max": max_val,
                "null_count": null_count,
                "bloom_offset": col.bloom_offset,
                "bloom_length": col.bloom_length,
            })
        result["row_groups"].append(rg_dict)
    return result
