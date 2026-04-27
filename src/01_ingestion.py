# Databricks notebook - Bronze ingestion
# Objetivo:
# Ingerir os arquivos originais de corridas Yellow Taxi NYC de janeiro a maio de 2023
# diretamente da fonte pública da TLC para a camada Bronze no Data Lake.
#
# A camada Bronze mantém os dados em seu formato original, sem transformação,
# preservando rastreabilidade e permitindo reprocessamentos futuros.

year = "2023"
months = ["01", "02", "03", "04", "05"]

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# External location configurada no Databricks apontando para bucket S3
bronze_path = "s3://bronze-case-ifood-geoleal/yellow_taxi"

for month in months:
    file_name = f"yellow_tripdata_{year}-{month}.parquet"

    source_url = f"{base_url}/{file_name}"
    target_dir = f"{bronze_path}/year={year}/month={month}"
    target_path = f"{target_dir}/{file_name}"

    try:
        # Cria o diretório de destino caso ainda não exista
        dbutils.fs.mkdirs(target_dir)

        # Copia o arquivo original para a camada Bronze
        # overwrite=True garante idempotência em caso de reprocessamento
        dbutils.fs.cp(source_url, target_path, True)

        # Validação simples para garantir que o arquivo foi gravado
        files = dbutils.fs.ls(target_dir)
        ingested_files = [file.name for file in files]

        if file_name not in ingested_files:
            raise Exception(f"File {file_name} was not found after ingestion.")

        print(f"Successfully ingested: {target_path}")

    except Exception as error:
        print(f"Error ingesting file {file_name}: {str(error)}")
        raise