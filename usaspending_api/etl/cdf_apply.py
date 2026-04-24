import csv
import io
from typing import Iterable

import pyarrow as pa


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
    array literals (e.g. `{"a","b"}`). Null outer values render as empty CSV fields.
    """
    table = table.select(column_order)

    list_columns = [
        field.name
        for field in table.schema
        if pa.types.is_list(field.type) or pa.types.is_large_list(field.type)
    ]

    df = table.to_pandas()
    for col in list_columns:
        df[col] = df[col].map(lambda v: _format_pg_array_literal(v) if v is not None else None)

    buffer = io.StringIO()
    df.to_csv(
        buffer,
        header=False,
        index=False,
        na_rep="",
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
