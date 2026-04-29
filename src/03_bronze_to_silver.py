# Silver processing

# Objetivo:
# Ler os dados brutos da camada Bronze, padronizar schemas entre datasets
# aplicar tipagem, adicionar metadados de linhagem
# e gravar os dados tratados na camada Silver em Delta Lake.

# Importações Spark e Delta
from pyspark.sql.functions import col, lit, year, month
from pyspark.sql.types import LongType, DoubleType, StringType, TimestampType
from functools import reduce
from delta.tables import DeltaTable

# Variáveis de configuração do projeto
from variables import BRONZE, MONTH, YEAR, TAXI_TYPE, TABLE_SILVER

# Caminho base da camada Bronze
bronze_base_path = BRONZE

# Parâmetros de processamento do período
year_id = YEAR
months = MONTH

# Tabela destino da camada Silver
silver_table = TABLE_SILVER

# Datasets contemplados no processamento
taxi_types = TAXI_TYPE

# Lista auxiliar para consolidar os DataFrames processados
dfs = []


def get_col(df, col_name, data_type, alias_name):
    """
    Padroniza a leitura de colunas opcionais entre arquivos.
    Quando a coluna não existe na origem, retorna NULL tipado,
    mantendo compatibilidade de schema na camada Silver.
    """
    if col_name in df.columns:
        return col(col_name).cast(data_type).alias(alias_name)

    return lit(None).cast(data_type).alias(alias_name)


# ============================================================
# Leitura da Bronze e padronização do layout Silver
# ============================================================

for taxi_type in taxi_types:
    for month_id in months:

        # Caminho particionado da Bronze por tipo de taxi, ano e mês
        bronze_path = f"{bronze_base_path}/{taxi_type}/year={year_id}/month={month_id}"

        print(f"Lendo dados da Bronze: {bronze_path}")

        # Leitura dos arquivos Parquet preservados na camada Bronze
        df_raw = spark.read.parquet(bronze_path)

        # Mapeia as colunas de data conforme o padrão de cada dataset
        if taxi_type == "yellow":
            pickup_col = "tpep_pickup_datetime"
            dropoff_col = "tpep_dropoff_datetime"

        elif taxi_type == "green":
            pickup_col = "lpep_pickup_datetime"
            dropoff_col = "lpep_dropoff_datetime"

        # Trata variação de nomenclatura observada entre arquivos mensais
        airport_col = "airport_fee" if "airport_fee" in df_raw.columns else "Airport_fee"

        # Seleção, renomeação e tipagem das colunas para o modelo Silver
        df = (
            df_raw
            .select(
                lit(taxi_type).alias("taxi_type"),

                get_col(df_raw, "VendorID", LongType(), "vendor_id"),

                col(pickup_col).cast(TimestampType()).alias("pickup_datetime"),
                col(dropoff_col).cast(TimestampType()).alias("dropoff_datetime"),

                get_col(df_raw, "passenger_count", DoubleType(), "passenger_count"),
                get_col(df_raw, "trip_distance", DoubleType(), "trip_distance"),
                get_col(df_raw, "RatecodeID", DoubleType(), "rate_code_id"),
                get_col(df_raw, "store_and_fwd_flag", StringType(), "store_and_fwd_flag"),
                get_col(df_raw, "PULocationID", LongType(), "pu_location_id"),
                get_col(df_raw, "DOLocationID", LongType(), "do_location_id"),
                get_col(df_raw, "payment_type", LongType(), "payment_type"),

                get_col(df_raw, "fare_amount", DoubleType(), "fare_amount"),
                get_col(df_raw, "extra", DoubleType(), "extra"),
                get_col(df_raw, "mta_tax", DoubleType(), "mta_tax"),
                get_col(df_raw, "tip_amount", DoubleType(), "tip_amount"),
                get_col(df_raw, "tolls_amount", DoubleType(), "tolls_amount"),
                get_col(df_raw, "improvement_surcharge", DoubleType(), "improvement_surcharge"),
                get_col(df_raw, "total_amount", DoubleType(), "total_amount"),
                get_col(df_raw, "congestion_surcharge", DoubleType(), "congestion_surcharge"),

                get_col(df_raw, airport_col, DoubleType(), "airport_fee"),

                # Linhagem técnica do dado para auditoria e rastreabilidade
                col("_metadata.file_path").alias("source_file")
            )
            # Colunas derivadas para particionamento e consumo analítico
            .withColumn("pickup_year", year(col("pickup_datetime")))
            .withColumn("pickup_month", month(col("pickup_datetime")))
        )

        dfs.append(df)


# ============================================================
# Consolidação dos lotes mensais
# ============================================================

# Une todos os DataFrames mantendo alinhamento por nome de coluna
df_silver_clean = reduce(lambda df1, df2: df1.unionByName(df2), dfs)


# ============================================================
# Deduplicação do lote
# ============================================================

# Remove duplicidades antes do MERGE usando chave natural da corrida
df_silver_clean = df_silver_clean.dropDuplicates([
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "taxi_type",
    "pu_location_id",
    "do_location_id"
])


# ============================================================
# Regras do MERGE
# ============================================================

# Condição de chave para identificar registros já existentes na Silver
merge_condition = """
    target.vendor_id = source.vendor_id AND
    target.pickup_datetime = source.pickup_datetime AND
    target.dropoff_datetime = source.dropoff_datetime AND
    target.taxi_type = source.taxi_type AND
    target.pu_location_id = source.pu_location_id AND
    target.do_location_id = source.do_location_id
"""

# Atualiza apenas registros com alteração real de conteúdo
update_condition = """
    NOT (
        target.passenger_count <=> source.passenger_count AND
        target.trip_distance <=> source.trip_distance AND
        target.rate_code_id <=> source.rate_code_id AND
        target.store_and_fwd_flag <=> source.store_and_fwd_flag AND
        target.payment_type <=> source.payment_type AND
        target.fare_amount <=> source.fare_amount AND
        target.extra <=> source.extra AND
        target.mta_tax <=> source.mta_tax AND
        target.tip_amount <=> source.tip_amount AND
        target.tolls_amount <=> source.tolls_amount AND
        target.improvement_surcharge <=> source.improvement_surcharge AND
        target.total_amount <=> source.total_amount AND
        target.congestion_surcharge <=> source.congestion_surcharge AND
        target.airport_fee <=> source.airport_fee AND
        target.source_file <=> source.source_file AND
        target.pickup_year <=> source.pickup_year AND
        target.pickup_month <=> source.pickup_month
    )
"""


# ============================================================
# Escrita incremental na camada Silver
# ============================================================

if spark.catalog.tableExists(silver_table):

    # Carrega a tabela Delta existente para operação incremental
    delta_silver = DeltaTable.forName(spark, silver_table)

    (
        delta_silver.alias("target")
        .merge(
            df_silver_clean.alias("source"),
            merge_condition
        )
        # Atualiza registros existentes somente quando houver mudança
        .whenMatchedUpdate(
            condition=update_condition,
            set={
                "passenger_count": "source.passenger_count",
                "trip_distance": "source.trip_distance",
                "rate_code_id": "source.rate_code_id",
                "store_and_fwd_flag": "source.store_and_fwd_flag",
                "payment_type": "source.payment_type",
                "fare_amount": "source.fare_amount",
                "extra": "source.extra",
                "mta_tax": "source.mta_tax",
                "tip_amount": "source.tip_amount",
                "tolls_amount": "source.tolls_amount",
                "improvement_surcharge": "source.improvement_surcharge",
                "total_amount": "source.total_amount",
                "congestion_surcharge": "source.congestion_surcharge",
                "airport_fee": "source.airport_fee",
                "source_file": "source.source_file",
                "pickup_year": "source.pickup_year",
                "pickup_month": "source.pickup_month",
                "last_updated": "current_timestamp()"
            }
        )
        # Insere novos registros ainda não existentes na Silver
        .whenNotMatchedInsert(values={
            "taxi_type": "source.taxi_type",
            "vendor_id": "source.vendor_id",
            "pickup_datetime": "source.pickup_datetime",
            "dropoff_datetime": "source.dropoff_datetime",
            "passenger_count": "source.passenger_count",
            "trip_distance": "source.trip_distance",
            "rate_code_id": "source.rate_code_id",
            "store_and_fwd_flag": "source.store_and_fwd_flag",
            "pu_location_id": "source.pu_location_id",
            "do_location_id": "source.do_location_id",
            "payment_type": "source.payment_type",
            "fare_amount": "source.fare_amount",
            "extra": "source.extra",
            "mta_tax": "source.mta_tax",
            "tip_amount": "source.tip_amount",
            "tolls_amount": "source.tolls_amount",
            "improvement_surcharge": "source.improvement_surcharge",
            "total_amount": "source.total_amount",
            "congestion_surcharge": "source.congestion_surcharge",
            "airport_fee": "source.airport_fee",
            "source_file": "source.source_file",
            "pickup_year": "source.pickup_year",
            "pickup_month": "source.pickup_month",
            "last_ingestion": "current_timestamp()"
        })
        .execute()
    )

    print(f"MERGE executado com sucesso na tabela {silver_table}.")

else:
    print(f"Tabela {silver_table} não criada.")