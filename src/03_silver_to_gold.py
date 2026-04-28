# Importação de funções necessárias
from pyspark.sql.functions import col

# Caminho da camada Silver
# Contém os dados padronizados e unificados (multi-taxi)
silver_path = "s3://silver-case-ifood-geoleal/taxi_trips"

# Caminho da camada Gold
# Camada de consumo final para usuários analíticos
gold_path = "s3://gold-case-ifood-geoleal/taxi_trips"

print("Lendo dados da camada Silver...")

# Leitura dos dados da camada Silver
df_silver = spark.read.format("delta").load(silver_path)

# Seleção e transformação das colunas para a camada Gold
# Mantém apenas os campos essenciais para análise
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

    # Filtra os registros com dados válidos
    .filter(col("pickup_datetime").isNotNull())
    .filter(col("passenger_count").isNotNull())
    .filter(col("total_amount").isNotNull())
)

print("Gravando dados na camada Gold...")

# Escrita da camada Gold em formato Delta
# Particionamento por ano e mês para otimizar leitura
(
    df_gold
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("pickup_year", "pickup_month")
    .save(gold_path)
)

print(f"Camada Gold gravada com sucesso em: {gold_path}")