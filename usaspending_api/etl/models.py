from django.db import models


class CDFVersionTracking(models.Model):
    """Tracks the last-processed Delta Lake Change Data Feed (CDF) version for incremental table loads."""

    table_name = models.TextField(primary_key=True)
    last_processed_version = models.BigIntegerField()
    last_commit_timestamp = models.DateTimeField()
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = "cdf_version_tracking"
