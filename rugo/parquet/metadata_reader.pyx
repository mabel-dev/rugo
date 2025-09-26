# distutils: language = c++

from libcpp.string cimport string

import datetime
import struct

cimport metadata_reader


# --- value decoder ---
cdef object decode_value(string physical_type, string raw):
    cdef bytes b = raw
    if b is None:
        return None
    if len(b) == 0:
        return b""   # treat empty as empty, not None

    try:
        if physical_type == "INT32":
            return struct.unpack("<i", b)[0]
        elif physical_type == "INT64":
            return struct.unpack("<q", b)[0]
        elif physical_type == "FLOAT":
            return struct.unpack("<f", b)[0]
        elif physical_type == "DOUBLE":
            return struct.unpack("<d", b)[0]
        elif physical_type in ("BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"):
            return b
        elif physical_type == "INT96":
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
        else:
            return b.hex()
    except Exception:
        return b.hex()


def read_metadata(str path):
    cdef metadata_reader.FileStats fs
    fs = metadata_reader.ReadParquetMetadata(path.encode("utf-8"))
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
            rg_dict["columns"].append({
                "name": col.name.decode("utf-8"),
                "type": col.physical_type.decode("utf-8"),
                "min": decode_value(col.physical_type, col.min),
                "max": decode_value(col.physical_type, col.max),
                "null_count": col.null_count,
                "bloom_offset": col.bloom_offset,
                "bloom_length": col.bloom_length,
            })
        result["row_groups"].append(rg_dict)
    return result
