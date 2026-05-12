import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable

from usaspending_api.config import CONFIG
from usaspending_api.etl.models import CDFVersionTracking

logger = logging.getLogger(__name__)

CDF_METADATA_COLUMNS = ("_change_type", "_commit_version", "_commit_timestamp")
UPSERT_CHANGE_TYPES = ("insert", "update_postimage")


@dataclass
class CDFChangeSet:
    cdf: pa.Table
    latest_version: int
    latest_commit_timestamp: datetime


def get_last_processed_version(table_name: str) -> Optional[int]:
    """Return the last processed CDF version for the given table, or None if never processed."""
    try:
        return CDFVersionTracking.objects.values_list(
            "last_processed_version", flat=True
        ).get(table_name=table_name)
    except CDFVersionTracking.DoesNotExist:
        return None


def update_last_processed_version(
    table_name: str,
    version: int,
    commit_timestamp: datetime,
) -> None:
    """Upsert the CDF tracking record for the given table."""
    CDFVersionTracking.objects.update_or_create(
        table_name=table_name,
        defaults={
            "last_processed_version": version,
            "last_commit_timestamp": commit_timestamp,
        },
    )


def build_delta_table_s3_uri(destination_database: str, destination_table: str) -> str:
    return (
        f"s3://{CONFIG.SPARK_S3_BUCKET}/{CONFIG.DELTA_LAKE_S3_PATH}/"
        f"{destination_database}/{destination_table}"
    )


def _get_storage_options() -> Optional[dict]:
    if CONFIG.USE_AWS:
        return None
    return {
        "AWS_ACCESS_KEY_ID": CONFIG.AWS_ACCESS_KEY.get_secret_value(),
        "AWS_SECRET_ACCESS_KEY": CONFIG.AWS_SECRET_KEY.get_secret_value(),
        "AWS_REGION": CONFIG.AWS_REGION,
        "AWS_ENDPOINT_URL": f"http://{CONFIG.AWS_S3_ENDPOINT}",
        "AWS_ALLOW_HTTP": "true",
    }


def read_cdf_changes(
    delta_table_uri: str,
    starting_version: int,
) -> Optional[CDFChangeSet]:
    """Read CDF entries from (exclusive) `starting_version` to the latest committed version.

    Returns None if there are no new commits to process or the CDF is empty.
    """
    dt = DeltaTable(delta_table_uri, storage_options=_get_storage_options())
    latest_version = dt.version()

    if starting_version >= latest_version:
        logger.info(
            f"No new CDF changes to process: starting_version={starting_version}, "
            f"latest_version={latest_version}"
        )
        return None

    logger.info(
        f"Loading CDF from {delta_table_uri} for versions "
        f"{starting_version + 1}..{latest_version}"
    )
    cdf = dt.load_cdf(
        starting_version=starting_version + 1,
        ending_version=latest_version,
    ).read_all()

    if cdf.num_rows == 0:
        return None

    max_ts = pc.max(cdf.column("_commit_timestamp")).as_py()
    logger.info(f"Loaded {cdf.num_rows} CDF rows. latest_version={latest_version}")
    return CDFChangeSet(cdf=cdf, latest_version=latest_version, latest_commit_timestamp=max_ts)


def split_cdf_by_change_type(
    cdf: pa.Table,
    pk_column: str,
) -> tuple[list, pa.Table]:
    """Return (deleted_ids, upsert_rows) from the CDF Arrow table.

    `deleted_ids` includes every PK that appears in the CDF (any change_type). We delete
    these PKs before inserting upserts so the delete-then-insert pattern avoids PK
    conflicts, and so retries after a partial failure remain safe.

    `upsert_rows` contains rows whose final (latest _commit_version) non-preimage
    change_type is insert or update_postimage. Rows whose final state is a delete are
    excluded so insert-then-delete within the CDF window does not re-insert a row.
    CDF metadata columns are stripped from the returned Arrow table.
    """
    deleted_ids = pc.unique(cdf.column(pk_column)).to_pylist()

    non_preimage = cdf.filter(pc.not_equal(cdf.column("_change_type"), "update_preimage"))
    if non_preimage.num_rows == 0:
        upsert_rows = non_preimage
    else:
        df = non_preimage.to_pandas()
        df = df.sort_values("_commit_version").drop_duplicates(subset=[pk_column], keep="last")
        df = df[df["_change_type"].isin(UPSERT_CHANGE_TYPES)]
        upsert_rows = pa.Table.from_pandas(df, preserve_index=False)

    cols_to_drop = [c for c in CDF_METADATA_COLUMNS if c in upsert_rows.column_names]
    if cols_to_drop:
        upsert_rows = upsert_rows.drop_columns(cols_to_drop)

    return deleted_ids, upsert_rows
