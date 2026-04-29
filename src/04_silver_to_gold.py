# Importação de funções necessárias
from pyspark.sql.functions import col
from delta.tables import DeltaTable

# Caminho da camada Silver no S3
silver_path = "s3://silver-case-ifood-geoleal/taxi_trips"

# Nome da tabela Gold gerenciada
gold_table = "ifood_case.taxi_gold"

print("Lendo dados da camada Silver...")

# Leitura dos dados da camada Silver
df_silver = spark.read.format("delta").load(silver_path)

# Seleção e transformação das colunas para a camada Gold
df_gold = (
    df_silver
    .select(
        col("vendor_id").alias("VendorID"),
        col("passenger_count"),
        col("total_amount"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("pickup_year"),
        col("pickup_month"),
        col("pickup_day"),
        col("taxi_type")
    )

    # Filtra registros válidos
    .filter(col("pickup_datetime").isNotNull())
    .filter(col("dropoff_datetime").isNotNull())
    .filter(col("taxi_type").isNotNull())
    .filter(col("passenger_count").isNotNull())
    .filter(col("total_amount").isNotNull())

    # Remove duplicidades dentro do próprio DataFrame antes do merge
    .dropDuplicates([
        "VendorID",
        "pickup_datetime",
        "dropoff_datetime",
        "taxi_type"
    ])
)

print("Aplicando merge na tabela Gold...")

delta_gold = DeltaTable.forName(spark, gold_table)

(
    delta_gold.alias("target")
    .merge(
        df_gold.alias("source"),
        """
        target.VendorID = source.VendorID AND
        target.pickup_datetime = source.pickup_datetime AND
        target.dropoff_datetime = source.dropoff_datetime AND
        target.taxi_type = source.taxi_type
        """
    )
    .whenMatchedUpdate(set={
        "passenger_count": "source.passenger_count",
        "total_amount": "source.total_amount"
    })
    .whenNotMatchedInsert(values={
        "VendorID": "source.VendorID",
        "passenger_count": "source.passenger_count",
        "total_amount": "source.total_amount",
        "pickup_datetime": "source.pickup_datetime",
        "dropoff_datetime": "source.dropoff_datetime",
        "pickup_year": "source.pickup_year",
        "pickup_month": "source.pickup_month",
        "pickup_day": "source.pickup_day",
        "taxi_type": "source.taxi_type"
    })
    .execute()
)

print(f"Merge concluído com sucesso na tabela: {gold_table}")