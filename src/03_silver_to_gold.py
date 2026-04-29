from pyspark.sql.functions import col
from delta.tables import DeltaTable

from variables import YEAR, MONTH, TABLE_SILVER, TABLE_GOLD

silver_table = TABLE_SILVER
gold_table = TABLE_GOLD

year_id = int(YEAR)
months = [int(m) for m in MONTH]

df_gold = (
    spark.table(silver_table)
    .filter(
        (col("pickup_year") == year_id) &
        (col("pickup_month").isin(months))
    )
    .select(
        col("vendor_id"),
        col("passenger_count"),
        col("total_amount"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("pickup_year"),
        col("pickup_month"),
        col("taxi_type")
    )
    .dropDuplicates([
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "taxi_type"
    ])
)

merge_condition = """
    target.vendor_id = source.vendor_id AND
    target.pickup_datetime = source.pickup_datetime AND
    target.dropoff_datetime = source.dropoff_datetime AND
    target.taxi_type = source.taxi_type
"""

update_condition = """
    NOT (
        target.passenger_count <=> source.passenger_count AND
        target.total_amount <=> source.total_amount AND
        target.pickup_year <=> source.pickup_year AND
        target.pickup_month <=> source.pickup_month
    )
"""

if spark.catalog.tableExists(gold_table):

    delta_gold = DeltaTable.forName(spark, gold_table)

    (
        delta_gold.alias("target")
        .merge(
            df_gold.alias("source"),
            merge_condition
        )
        .whenMatchedUpdate(
            condition=update_condition,
            set={
                "passenger_count": "source.passenger_count",
                "total_amount": "source.total_amount",
                "pickup_year": "source.pickup_year",
                "pickup_month": "source.pickup_month"
            }
        )
        .whenNotMatchedInsert(values={
            "vendor_id": "source.vendor_id",
            "passenger_count": "source.passenger_count",
            "total_amount": "source.total_amount",
            "pickup_datetime": "source.pickup_datetime",
            "dropoff_datetime": "source.dropoff_datetime",
            "pickup_year": "source.pickup_year",
            "pickup_month": "source.pickup_month",
            "taxi_type": "source.taxi_type"
        })
        .execute()
    )

    print(f"MERGE executado com sucesso na tabela {gold_table}.")

else:

    (
        df_gold
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")
        .saveAsTable(gold_table)
    )

    print(f"Tabela {gold_table} criada com sucesso.")