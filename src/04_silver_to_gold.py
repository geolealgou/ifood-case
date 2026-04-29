# Gold processing

# Objetivo:
# Construir a camada Gold a partir da Silver, expondo apenas os dados
# necessários para consumo analítico, com modelo simplificado,
# consistente e otimizado para queries de negócio.

# Importações necessárias
from pyspark.sql.functions import col 
from delta.tables import DeltaTable

# Variáveis de configuração (tabelas e período)
from variables import YEAR, MONTH, TABLE_SILVER, TABLE_GOLD

# Tabelas origem (Silver) e destino (Gold)
silver_table = TABLE_SILVER
gold_table = TABLE_GOLD

# Parâmetros de filtro do período (escopo do case)
year_id = int(YEAR)
months = [int(m) for m in MONTH]


# ============================================================
# Leitura da Silver e construção do dataset Gold
# ============================================================

df_gold = (
    spark.table(silver_table)
    
    # Filtra apenas o período relevante para o processamento
    .filter(
        (col("pickup_year") == year_id) &
        (col("pickup_month").isin(months))
    )
    
    # Seleciona somente colunas necessárias para consumo analítico
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

    # Regras de qualidade aplicadas antes da carga na Gold:
    # remove registros sem datas, sem valor total ou com inconsistência temporal
    .filter(
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull() &
        col("total_amount").isNotNull() &
        (col("dropoff_datetime") >= col("pickup_datetime"))
    )
    
    # Remove duplicidades com base na granularidade de corrida
    .dropDuplicates([
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "taxi_type"
    ])
)

# ============================================================
# Regras de MERGE (carga incremental)
# ============================================================

# Chave de negócio para identificação de registros únicos
merge_condition = """
    target.vendor_id = source.vendor_id AND
    target.pickup_datetime = source.pickup_datetime AND
    target.dropoff_datetime = source.dropoff_datetime AND
    target.taxi_type = source.taxi_type
"""

# Atualiza apenas registros com alteração efetiva
# (evita reescrita desnecessária no Delta Lake)
update_condition = """
    NOT (
        target.passenger_count <=> source.passenger_count AND
        target.total_amount <=> source.total_amount 
    )
"""


# ============================================================
# Escrita na camada Gold
# ============================================================

if spark.catalog.tableExists(gold_table):

    # Carrega a tabela Delta existente para operação incremental
    delta_gold = DeltaTable.forName(spark, gold_table)

    (
        delta_gold.alias("target")
        .merge(
            df_gold.alias("source"),
            merge_condition
        )
        
        # Atualiza registros existentes apenas quando necessário
        .whenMatchedUpdate(
            condition=update_condition,
            set={
                "passenger_count": "source.passenger_count",
                "total_amount": "source.total_amount",
                "pickup_year": "source.pickup_year",
                "pickup_month": "source.pickup_month"
            }
        )
        
        # Insere novos registros não existentes na Gold
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
    print(f"Tabela {gold_table} não criada.")