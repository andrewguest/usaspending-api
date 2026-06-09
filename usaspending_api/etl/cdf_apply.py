import csv
import io
from typing import Iterable

import pandas as pd
import pyarrow as pa

# Null marker written for SQL NULL values in the CSV. Any COPY of a buffer produced by
# arrow_to_pg_csv_buffer must specify (FORMAT CSV, NULL '\N') so that empty unquoted
# fields remain empty strings rather than collapsing into NULL. (A text value that is
# literally backslash-N would be misread as NULL; considered acceptably rare.)
PG_COPY_NULL = r"\N"


def _format_pg_array_literal(items) -> str | None:
    if items is None:
        return None
    parts = []
    for element in items:
        if element is None:
            parts.append("NULL")
        else:
            escaped = str(element).replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'"{escaped}"')
    return "{" + ",".join(parts) + "}"


def arrow_to_pg_csv_buffer(table: pa.Table, column_order: list[str]) -> io.StringIO:
    """Convert a PyArrow table to an in-memory CSV buffer compatible with Postgres COPY.

    Columns are reordered to match `column_order`. List columns are rendered as Postgres
    array literals (e.g. `{"a","b"}`). SQL NULLs render as the PG_COPY_NULL marker, so the
    COPY statement must include (FORMAT CSV, NULL '\\N'); empty strings stay empty fields.
    """
    table = table.select(column_order)

    list_columns = [
        field.name
        for field in table.schema
        if pa.types.is_list(field.type) or pa.types.is_large_list(field.type)
    ]

    # types_mapper=pd.ArrowDtype keeps nullable ints as ints (the default conversion
    # coerces them to float64, rendering e.g. 123 as "123.0" which COPY rejects for
    # integer columns) and avoids datetime64[ns] range overflow on very old timestamps
    df = table.to_pandas(types_mapper=pd.ArrowDtype)
    for col in list_columns:
        df[col] = [_format_pg_array_literal(v) for v in table.column(col).to_pylist()]

    buffer = io.StringIO()
    df.to_csv(
        buffer,
        header=False,
        index=False,
        na_rep=PG_COPY_NULL,
        quoting=csv.QUOTE_MINIMAL,
    )
    buffer.seek(0)
    return buffer


def ids_to_csv_buffer(ids: Iterable) -> io.StringIO:
    """Convert an iterable of scalar IDs to a single-column CSV buffer."""
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    for value in ids:
        writer.writerow([value])
    buffer.seek(0)
    return buffer
