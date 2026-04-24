import logging

from django import db
from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction

from usaspending_api.etl.cdf_apply import arrow_to_pg_csv_buffer, ids_to_csv_buffer
from usaspending_api.etl.cdf_reader import (
    build_delta_table_s3_uri,
    get_last_processed_version,
    read_cdf_changes,
    split_cdf_by_change_type,
    update_last_processed_version,
)
from usaspending_api.etl.management.commands.create_delta_table import TABLE_SPEC

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    help = (
        "Incrementally apply changes from a Delta table's Change Data Feed (CDF) to the "
        "corresponding Postgres table. Reads the last-processed version from "
        "cdf_version_tracking, loads only the CDF entries after that version, stages them to "
        "tables in the temp schema, and applies the changes together with the tracking-table "
        "update in a single transaction. Does not seed the initial tracking record — a full "
        "reload via load_table_from_delta must run first and the tracking record must be "
        "seeded by the operator."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--delta-table",
            type=str,
            required=True,
            choices=list(TABLE_SPEC),
            help="The source Delta table to read the CDF from",
        )
        parser.add_argument(
            "--cleanup-staging",
            action="store_true",
            help="Drop staging tables after a successful apply. Default keeps them for audit.",
        )

    def handle(self, *args, **options) -> None:
        delta_table = options["delta_table"]
        spec = TABLE_SPEC[delta_table]

        pk_column = spec.get("primary_key_column")
        if not pk_column:
            raise RuntimeError(
                f"TABLE_SPEC[{delta_table!r}] is missing 'primary_key_column'. "
                f"The incremental flow requires this field."
            )

        destination_database = spec.get("destination_database")
        swap_schema = spec.get("swap_schema")
        swap_table = spec.get("swap_table")
        column_names = spec.get("column_names")
        if not (destination_database and swap_schema and swap_table and column_names):
            raise RuntimeError(
                f"TABLE_SPEC[{delta_table!r}] must define destination_database, swap_schema, "
                f"swap_table, and column_names for the incremental flow."
            )

        live_table = f"{swap_schema}.{swap_table}"
        deletes_staging = f"temp.{swap_table}_cdf_deletes"
        upserts_staging = f"temp.{swap_table}_cdf_upserts"

        last_version = get_last_processed_version(delta_table)
        if last_version is None:
            logger.warning(
                f"No CDF tracking record found for {delta_table!r}. The incremental flow "
                f"requires an initial version to be seeded after a full reload. Exiting."
            )
            return

        delta_uri = build_delta_table_s3_uri(destination_database, delta_table)
        change_set = read_cdf_changes(delta_uri, starting_version=last_version)
        if change_set is None:
            logger.info(f"Nothing to apply for {delta_table!r}.")
            return

        deleted_ids, upsert_rows = split_cdf_by_change_type(change_set.cdf, pk_column)
        logger.info(
            f"Planned: {len(deleted_ids)} PK(s) to delete, {upsert_rows.num_rows} row(s) to "
            f"upsert. Target version: {change_set.latest_version} "
            f"({change_set.latest_commit_timestamp})."
        )

        with transaction.atomic():
            with db.connection.cursor() as cursor:
                self._recreate_staging(cursor, deletes_staging, upserts_staging, live_table, pk_column)
                self._populate_staging(
                    cursor,
                    deletes_staging,
                    upserts_staging,
                    pk_column,
                    column_names,
                    deleted_ids,
                    upsert_rows,
                )
                rows_deleted = self._apply_deletes(cursor, live_table, deletes_staging, pk_column)
                rows_inserted = self._apply_upserts(cursor, live_table, upserts_staging, column_names)
            update_last_processed_version(
                delta_table,
                change_set.latest_version,
                change_set.latest_commit_timestamp,
            )

        logger.info(
            f"Applied: {rows_deleted} row(s) deleted, {rows_inserted} row(s) inserted. "
            f"Tracking updated to version {change_set.latest_version}."
        )

        if options["cleanup_staging"]:
            with db.connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {deletes_staging}")
                cursor.execute(f"DROP TABLE IF EXISTS {upserts_staging}")
            logger.info("Staging tables dropped.")

    def _recreate_staging(self, cursor, deletes_staging, upserts_staging, live_table, pk_column):
        logger.info(f"Recreating staging tables {deletes_staging} and {upserts_staging}.")
        cursor.execute(f"DROP TABLE IF EXISTS {deletes_staging}")
        cursor.execute(f"DROP TABLE IF EXISTS {upserts_staging}")
        cursor.execute(
            f"CREATE TABLE {deletes_staging} AS "
            f"SELECT {pk_column} FROM {live_table} WITH NO DATA"
        )
        cursor.execute(
            f"CREATE TABLE {upserts_staging} (LIKE {live_table} INCLUDING DEFAULTS)"
        )

    def _populate_staging(
        self,
        cursor,
        deletes_staging,
        upserts_staging,
        pk_column,
        column_names,
        deleted_ids,
        upsert_rows,
    ):
        if deleted_ids:
            logger.info(f"Staging {len(deleted_ids)} PK(s) to {deletes_staging} via COPY.")
            buffer = ids_to_csv_buffer(deleted_ids)
            cursor.copy_expert(
                sql=f"COPY {deletes_staging} ({pk_column}) FROM STDIN (FORMAT CSV)",
                file=buffer,
            )
        if upsert_rows.num_rows > 0:
            logger.info(f"Staging {upsert_rows.num_rows} row(s) to {upserts_staging} via COPY.")
            buffer = arrow_to_pg_csv_buffer(upsert_rows, column_names)
            cursor.copy_expert(
                sql=f"COPY {upserts_staging} ({','.join(column_names)}) FROM STDIN (FORMAT CSV)",
                file=buffer,
            )

    def _apply_deletes(self, cursor, live_table, deletes_staging, pk_column) -> int:
        cursor.execute(
            f"DELETE FROM {live_table} AS tgt "
            f"USING {deletes_staging} AS stg "
            f"WHERE tgt.{pk_column} = stg.{pk_column}"
        )
        return cursor.rowcount

    def _apply_upserts(self, cursor, live_table, upserts_staging, column_names) -> int:
        cols = ",".join(column_names)
        cursor.execute(
            f"INSERT INTO {live_table} ({cols}) SELECT {cols} FROM {upserts_staging}"
        )
        return cursor.rowcount
