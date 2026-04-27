from pyspark.sql.functions import col, year, month
from pyspark.sql.types import LongType, DoubleType, StringType, TimestampType
from functools import reduce

# Caminho da camada Bronze.
# Nesta camada estão os arquivos Parquet originais ingeridos da NYC TLC.
bronze_path = "s3://bronze-case-ifood-geoleal/yellow_taxi"

# Caminho da camada Silver.
# Nesta camada os dados são padronizados, tipados, limpos e gravados em Delta Lake.
silver_path = "s3://silver-case-ifood-geoleal/yellow_taxi"

# Parâmetros de período para leitura dos dados na camada Bronze
year_id = "2023"
months = [f"{m:02d}" for m in range(1, 6)]

# Filtro baseado na data real (pickup)
year_start = 2023
year_end = 2023
month_start = 1
month_end = 5

dfs = []

for month_id in months:
    path = f"{bronze_path}/year={year_id}/month={month_id}"

    print(f"Lendo dados da Bronze: {path}")

    # A leitura é feita mês a mês porque os arquivos Parquet originais possuem
    # diferenças de schema físico entre os meses.
    df_raw = spark.read.parquet(path)

    # Alguns arquivos possuem variação no nome da coluna airport_fee.
    airport_col = "airport_fee" if "airport_fee" in df_raw.columns else "Airport_fee"

    df = (
        df_raw
        .select(
            col("VendorID").cast(LongType()).alias("vendor_id"),
            col("tpep_pickup_datetime").cast(TimestampType()).alias("pickup_datetime"),
            col("tpep_dropoff_datetime").cast(TimestampType()).alias("dropoff_datetime"),
            col("passenger_count").cast(DoubleType()).alias("passenger_count"),
            col("trip_distance").cast(DoubleType()).alias("trip_distance"),
            col("RatecodeID").cast(DoubleType()).alias("rate_code_id"),
            col("store_and_fwd_flag").cast(StringType()).alias("store_and_fwd_flag"),
            col("PULocationID").cast(LongType()).alias("pu_location_id"),
            col("DOLocationID").cast(LongType()).alias("do_location_id"),
            col("payment_type").cast(LongType()).alias("payment_type"),
            col("fare_amount").cast(DoubleType()).alias("fare_amount"),
            col("extra").cast(DoubleType()).alias("extra"),
            col("mta_tax").cast(DoubleType()).alias("mta_tax"),
            col("tip_amount").cast(DoubleType()).alias("tip_amount"),
            col("tolls_amount").cast(DoubleType()).alias("tolls_amount"),
            col("improvement_surcharge").cast(DoubleType()).alias("improvement_surcharge"),
            col("total_amount").cast(DoubleType()).alias("total_amount"),
            col("congestion_surcharge").cast(DoubleType()).alias("congestion_surcharge"),
            col(airport_col).cast(DoubleType()).alias("airport_fee"),
            col("_metadata.file_path").alias("_source_file")
        )
        # Colunas auxiliares para particionamento e análises temporais.
        .withColumn("pickup_year", year(col("pickup_datetime")))
        .withColumn("pickup_month", month(col("pickup_datetime")))
    )

    dfs.append(df)

# Une os DataFrames mensais em um único DataFrame com schema padronizado.
df_silver = reduce(lambda df1, df2: df1.unionByName(df2), dfs)

# Limpeza mínima para garantir qualidade na camada de consumo.
df_silver_clean = (
    df_silver
    .filter(col("pickup_datetime").isNotNull())
    .filter(col("dropoff_datetime").isNotNull())
    .filter(col("total_amount").isNotNull())
    .filter(col("dropoff_datetime") >= col("pickup_datetime"))
    .filter(col("pickup_year").between(year_start, year_end))
    .filter(col("pickup_month").between(month_start, month_end))
)

# Grava a camada Silver em Delta Lake, particionada por ano e mês.
(
    df_silver_clean
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("pickup_year", "pickup_month")
    .save(silver_path)
)

print(f"Camada Silver gravada com sucesso em: {silver_path}")






