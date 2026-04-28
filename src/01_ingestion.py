# Databricks notebook - Bronze ingestion
# Objetivo:
# Ingerir os arquivos originais de corridas da NYC TLC (yellow, green, fhv, fhvhv)
# para a camada Bronze no Data Lake.
#
# A camada Bronze mantém os dados em seu formato original, sem transformações,
# garantindo rastreabilidade, reprocessamento e auditoria dos dados ingeridos.

year = "2023"
months = ["01", "02", "03", "04", "05"]

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Tipos de datasets disponíveis na NYC TLC
taxi_types = ["yellow", "green", "fhv", "fhvhv"]

# Caminho base da camada Bronze no Data Lake (S3)
bronze_base_path = "s3://bronze-case-ifood-geoleal"

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