from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def upsert_silver_trades(
    spark: SparkSession,
    incoming_df: DataFrame,
    table_path: str,
) -> None:
    """
    Insert new trades into the Silver Delta table.

    Uniqueness key: (product_id, trade_id).
    Existing matching trades remain unchanged.
    """
    deduplicated_df = incoming_df.dropDuplicates(["product_id", "trade_id"])

    if not DeltaTable.isDeltaTable(spark, table_path):
        Path(table_path).parent.mkdir(parents=True, exist_ok=True)
        deduplicated_df.write.format("delta").mode("errorifexists").save(table_path)
        return

    silver_table = DeltaTable.forPath(spark, table_path)
    (
        silver_table.alias("target")
        .merge(
            deduplicated_df.alias("source"),
            "target.product_id = source.product_id "
            "AND target.trade_id = source.trade_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
