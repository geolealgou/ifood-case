# Databricks notebook - Bronze ingestion
# Objetivo:
# Ingerir os dados originais de corridas de taxi NYC TLC (yellow, green)
# para a camada Bronze no Data Lake.
# Parâmetros de período fixo para o case, no ambiente produtivo o ideal é parametrizar , uma tabela ou algo assim

# Importações de variaveis
from variables import BRONZE, MONTH, YEAR, TAXI_TYPE

# Datas de entrada
year = YEAR
months = MONTH

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Tipos de datasets disponíveis na NYC TLC
taxi_types = TAXI_TYPE

# Caminho base da camada Bronze no Data Lake (S3)
bronze_base_path = BRONZE

for taxi_type in taxi_types:
    for month in months:
        file_name = f"{taxi_type}_tripdata_{year}-{month}.parquet"

        source_url = f"{base_url}/{file_name}"
        target_dir = f"{bronze_base_path}/{taxi_type}/year={year}/month={month}"
        target_path = f"{target_dir}/{file_name}"

        try:
            # Cria o diretório de destino (caso ainda não exista)
            dbutils.fs.mkdirs(target_dir)

            # Copia o arquivo original para a camada Bronze
            # overwrite=True garante idempotência em reprocessamentos
            dbutils.fs.cp(source_url, target_path, True)

            # Validação simples para garantir que o arquivo foi ingerido corretamente
            files = dbutils.fs.ls(target_dir)
            ingested_files = [file.name for file in files]

            if file_name not in ingested_files:
                raise Exception(f"File {file_name} not found after ingestion.")

            print(f"Successfully ingested: {target_path}")

        except Exception as error:
            print(f"Error ingesting {file_name}: {str(error)}")