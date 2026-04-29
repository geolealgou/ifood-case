# Bronze ingestion

# Objetivo:
# Realizar a ingestão dos dados brutos de corridas de taxi (NYC TLC)
# diretamente da fonte pública para a camada Bronze no Data Lake,
# preservando o formato original (raw) e garantindo rastreabilidade.

# Observação:
# Os parâmetros de período estão fixos nas variaveis para o contexto do case.
# Em ambiente produtivo, recomenda-se externalizar via tabela de controle, config service

# Importação de variáveis de configuração (ambiente/data lake)
from variables import BRONZE, MONTH, YEAR, TAXI_TYPE

# Parâmetros de ingestão
year = YEAR
months = MONTH

# Endpoint público oficial da NYC TLC
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Tipos de datasets a serem ingeridos (ex: yellow, green)
taxi_types = TAXI_TYPE

# Caminho da camada Bronze (S3 / Data Lake)
bronze_base_path = BRONZE

# Loop de ingestão por tipo de taxi e período
for taxi_type in taxi_types:
    for month in months:
        
        # Nome padrão do arquivo conforme convenção da NYC TLC
        file_name = f"{taxi_type}_tripdata_{year}-{month}.parquet"

        # Origem (HTTP) e destino (Data Lake)
        source_url = f"{base_url}/{file_name}"
        target_dir = f"{bronze_base_path}/{taxi_type}/year={year}/month={month}"
        target_path = f"{target_dir}/{file_name}"

        try:
            # Garante a existência do diretório de destino (particionamento lógico)
            dbutils.fs.mkdirs(target_dir)

            # Ingestão do arquivo bruto mantendo formato original
            # overwrite=True assegura idempotência em reprocessamentos
            dbutils.fs.cp(source_url, target_path, True)

            # Validação pós-ingestão:
            # Confirma se o arquivo está disponível no destino
            files = dbutils.fs.ls(target_dir)
            ingested_files = [file.name for file in files]

            if file_name not in ingested_files:
                raise Exception(f"File {file_name} not found after ingestion.")

            print(f"Successfully ingested: {target_path}")

        except Exception as error:
            # Tratamento de erro por arquivo (resiliência do pipeline)
            # Permite continuidade do processamento para demais partições
            print(f"Error ingesting {file_name}: {str(error)}")