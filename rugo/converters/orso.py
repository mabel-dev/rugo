"""
Convert rugo parquet metadata schemas to orso RelationSchema format.
"""

from typing import Any
from typing import Dict
from typing import Optional

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes


def _map_parquet_type_to_orso(
    parquet_type: str, logical_type: Optional[str] = None
) -> str:
    """
    Map parquet physical and logical types to orso types.

    Args:
        parquet_type: Physical parquet type (e.g., "int64", "byte_array", "float64")
        logical_type: Logical parquet type if available (e.g., "STRING", "TIMESTAMP_MILLIS")

    Returns:
        Orso type string
    """
    # If we have a logical type, use it for more precise mapping
    if logical_type:
        logical_lower = logical_type.lower()

        # String types
        if logical_lower in ("string", "utf8"):
            return OrsoTypes.VARCHAR

        # Date/time types
        if logical_lower in ("date", "date32[day]"):
            return OrsoTypes.DATE
        if logical_lower.startswith("time") and not logical_lower.startswith(
            "timestamp"
        ):
            return OrsoTypes.TIME
        if logical_lower.startswith("timestamp") or "timestamp" in logical_lower:
            return OrsoTypes.TIMESTAMP

        # Decimal types
        if logical_lower.startswith("decimal"):
            return OrsoTypes.DECIMAL

        # JSON types
        if logical_lower in ("json", "jsonb"):
            return OrsoTypes.JSONB

        # Boolean types
        if logical_lower == "boolean":
            return OrsoTypes.BOOLEAN

    # Fall back to physical type mapping
    physical_lower = parquet_type.lower()

    # Integer types
    if physical_lower in ("int8", "int16", "int32", "int64"):
        return OrsoTypes.INTEGER

    # Floating point types
    if physical_lower in ("float", "float32", "float64", "double"):
        return OrsoTypes.DOUBLE

    # Binary/string types
    if physical_lower in ("byte_array", "fixed_len_byte_array"):
        return OrsoTypes.VARCHAR

    # Boolean type
    if physical_lower == "boolean":
        return OrsoTypes.BOOLEAN

    # Default to VARCHAR for unknown types
    return OrsoTypes.VARCHAR


def rugo_to_orso_schema(
    rugo_metadata: Dict[str, Any], schema_name: str = "parquet_schema"
) -> RelationSchema:
    """
    Convert rugo parquet metadata to an orso RelationSchema.

    Args:
        rugo_metadata: The metadata dictionary returned by rugo.parquet.read_metadata()
        schema_name: Name for the resulting schema (default: "parquet_schema")

    Returns:
        OrsoRelationSchema object

    Raises:
        ValueError: If the metadata format is invalid
    """
    if not isinstance(rugo_metadata, dict):
        raise ValueError("rugo_metadata must be a dictionary")

    if "row_groups" not in rugo_metadata:
        raise ValueError("rugo_metadata must contain 'row_groups' key")

    if not rugo_metadata["row_groups"]:
        raise ValueError("rugo_metadata must contain at least one row group")

    # Get columns from the first row group (schema should be consistent across row groups)
    first_row_group = rugo_metadata["row_groups"][0]

    if "columns" not in first_row_group:
        raise ValueError("Row group must contain 'columns' key")

    columns = []
    for col_metadata in first_row_group["columns"]:
        if "name" not in col_metadata or "type" not in col_metadata:
            raise ValueError("Column metadata must contain 'name' and 'type' keys")

        col_name = col_metadata["name"]
        parquet_type = col_metadata["type"]
        logical_type = col_metadata.get("logical_type")

        # Map to orso type
        orso_type = _map_parquet_type_to_orso(parquet_type, logical_type)

        # Create orso column
        orso_column = FlatColumn(
            name=col_name,
            type=orso_type,
            nullable=col_metadata.get("null_count", 0) > 0,
        )

        columns.append(orso_column)

    # Create and populate the RelationSchema
    schema = RelationSchema(name=schema_name)

    # Add all columns to the schema
    for column in columns:
        schema.columns.append(column)

    # Add row count estimate if available
    if "num_rows" in rugo_metadata:
        schema.row_count_estimate = rugo_metadata["num_rows"]

    return schema


def extract_schema_only(
    rugo_metadata: Dict[str, Any], schema_name: str = "parquet_schema"
) -> Dict[str, str]:
    """
    Extract just the column name to type mapping from rugo metadata.

    Args:
        rugo_metadata: The metadata dictionary returned by rugo.parquet.read_metadata()
        schema_name: Name for the schema (included in result for completeness)

    Returns:
        Dictionary with schema name and column type mappings
    """
    orso_schema = rugo_to_orso_schema(rugo_metadata, schema_name)

    column_types = {}
    for column in orso_schema.columns:
        column_types[column.name] = column.type

    return {
        "schema_name": schema_name,
        "columns": column_types,
        "row_count": orso_schema.row_count_estimate,
    }
