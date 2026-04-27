from pyspark.sql.functions import col

# Caminho da camada Silver.
# Contém os dados padronizados, tipados e limpos em formato Delta.
silver_path = "s3://silver-case-ifood-geoleal/yellow_taxi"

# Caminho da camada Gold.
# Contém a visão final de consumo, com apenas as colunas necessárias
# para as análises solicitadas no case.
gold_path = "s3://gold-case-ifood-geoleal/yellow_taxi"

print("Lendo dados da camada Silver...")

df_silver = spark.read.format("delta").load(silver_path)

# Seleciona e renomeia as colunas finais de consumo.
# A Gold mantém somente os campos necessários para atender os requisitos de negocio,
# reduzindo complexidade para usuários analíticos.
df_gold = df_silver.select(    
    col("vendor_id").alias("VendorID"),
    col("passenger_count"),
    col("total_amount"),
    col("pickup_datetime").alias("tpep_pickup_datetime"),
    col("dropoff_datetime").alias("tpep_dropoff_datetime"),
    col("pickup_year"),
    col("pickup_month")
)

print("Gravando dados na camada Gold...")

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
