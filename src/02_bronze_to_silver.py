# Importações de funções e tipos do PySpark
from pyspark.sql.functions import col, lit, year, month, dayofmonth
from pyspark.sql.types import LongType, DoubleType, StringType, TimestampType
from functools import reduce

# Caminho base da camada Bronze (dados brutos ingeridos do TLC)
bronze_base_path = "s3://bronze-case-ifood-geoleal"

# Caminho da camada Silver (dados padronizados e prontos para consumo intermediário)
silver_path = "s3://silver-case-ifood-geoleal/taxi_trips"

# Parâmetros de período
year_id = "2023"
months = [f"{m:02d}" for m in range(1, 6)]

# Tipos de datasets disponíveis na NYC TLC
taxi_types = ["yellow", "green"]

# Lista que armazenará os DataFrames de cada leitura mensal
dfs = []

# Função auxiliar para padronizar colunas entre datasets diferentes
# Caso a coluna não exista no dataset, retorna NULL com o tipo esperado
def get_col(df, col_name, data_type, alias_name):
    if col_name in df.columns:
        return col(col_name).cast(data_type).alias(alias_name)
    return lit(None).cast(data_type).alias(alias_name)

# Loop principal para leitura dos dados por tipo de taxi e mês
for taxi_type in taxi_types:
    for month_id in months:

        # Caminho da Bronze organizado por tipo, ano e mês
        path = f"{bronze_base_path}/{taxi_type}/year={year_id}/month={month_id}"

        print(f"Lendo dados da Bronze: {path}")

        # Leitura do arquivo Parquet bruto
        df_raw = spark.read.parquet(path)

        # Definição das colunas de data/hora dependendo do tipo de dataset
        # (cada dataset possui naming diferente)
        if taxi_type == "yellow":
            pickup_col = "tpep_pickup_datetime"
            dropoff_col = "tpep_dropoff_datetime"

        elif taxi_type == "green":
            pickup_col = "lpep_pickup_datetime"
            dropoff_col = "lpep_dropoff_datetime"    

        # Algumas variações de schema possuem nomes diferentes para airport_fee
        airport_col = "airport_fee" if "airport_fee" in df_raw.columns else "Airport_fee"

        # Seleção e padronização das colunas
        df = (
            df_raw
            .select(
                # Identificação do tipo de serviço (importante para análises futuras)
                lit(taxi_type).alias("taxi_type"),

                # Campos principais
                get_col(df_raw, "VendorID", LongType(), "vendor_id"),

                # Padronização das colunas de data
                col(pickup_col).cast(TimestampType()).alias("pickup_datetime"),
                col(dropoff_col).cast(TimestampType()).alias("dropoff_datetime"),

                # Campos de negócio (quando disponíveis)
                get_col(df_raw, "passenger_count", DoubleType(), "passenger_count"),
                get_col(df_raw, "trip_distance", DoubleType(), "trip_distance"),
                get_col(df_raw, "RatecodeID", DoubleType(), "rate_code_id"),
                get_col(df_raw, "store_and_fwd_flag", StringType(), "store_and_fwd_flag"),
                get_col(df_raw, "PULocationID", LongType(), "pu_location_id"),
                get_col(df_raw, "DOLocationID", LongType(), "do_location_id"),
                get_col(df_raw, "payment_type", LongType(), "payment_type"),

                # Valores financeiros
                get_col(df_raw, "fare_amount", DoubleType(), "fare_amount"),
                get_col(df_raw, "extra", DoubleType(), "extra"),
                get_col(df_raw, "mta_tax", DoubleType(), "mta_tax"),
                get_col(df_raw, "tip_amount", DoubleType(), "tip_amount"),
                get_col(df_raw, "tolls_amount", DoubleType(), "tolls_amount"),
                get_col(df_raw, "improvement_surcharge", DoubleType(), "improvement_surcharge"),
                get_col(df_raw, "total_amount", DoubleType(), "total_amount"),
                get_col(df_raw, "congestion_surcharge", DoubleType(), "congestion_surcharge"),

                # Campo com variação de schema
                get_col(df_raw, airport_col, DoubleType(), "airport_fee"),

                # Coluna de rastreabilidade (data lineage)
                col("_metadata.file_path").alias("_source_file")
            )

            # Criação de colunas auxiliares para particionamento e análises temporais
            .withColumn("pickup_year", year(col("pickup_datetime")))
            .withColumn("pickup_month", month(col("pickup_datetime")))
            .withColumn("pickup_day", dayofmonth(col("pickup_datetime")))
        )

        # Adiciona o DataFrame padronizado à lista
        dfs.append(df)

# União de todos os DataFrames em um único DataFrame Silver
# unionByName garante alinhamento correto das colunas
df_silver = reduce(lambda df1, df2: df1.unionByName(df2), dfs)

# Filtro para garantir qualidade e escopo do case (Jan–May 2023)
df_silver_clean = (
    df_silver
    .filter(col("pickup_year") == 2023)
    .filter(col("pickup_month").between(1, 5))
)

# Escrita da camada Silver em formato Delta
# Particionamento por tipo de taxi, ano e mês para otimizar leitura
(
    df_silver_clean
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("taxi_type", "pickup_year", "pickup_month")
    .save(silver_path)
)

print(f"Camada Silver gravada com sucesso em: {silver_path}")






