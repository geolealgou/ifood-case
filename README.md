# README — ifood-case


## Visão Geral

Este projeto apresenta uma solução de ingestão, transformação e disponibilização de dados de corridas de táxi de Nova York (NYC TLC), seguindo o modelo de arquitetura **Data Lake em camadas (Bronze, Silver e Gold)**.

## Estrutura do Repositório

ifood-case/

├─ src/

│ ├─ 01_ingestion.py # Ingestão camada Bronze

│ ├─ 02_bronze_to_silver.py # Transformação Bronze → Silver

│ └─ 03_silver_to_gold.py # Transformação Silver → Gold

├─ analysis/ # Queries e análises

├─ README.md

### Organização

- **src/**: contém toda a pipeline de dados  
- **analysis/**: contém as análises solicitadas pelo time de negocio  
- **README.md**: documentação da solução  

## Arquitetura da Solução

A solução foi estruturada seguindo o padrão de camadas:

- **Bronze** → dados bruto  
- **Silver** → dados tratados e padronizados  
- **Gold** → dados prontos para consumo analítico  

## 🟤 Camada Bronze — Ingestão

### Objetivo

Ingerir os arquivos originais de corridas de táxi (Yellow Taxi) disponibilizados pela NYC TLC do ano de 2023 e meses de jan a maio, mantendo os dados em seu formato original e garantindo rastreabilidade.

### Implementação

A ingestão foi realizada diretamente a partir da fonte pública:

https://d37ci6vzurychx.cloudfront.net/trip-data

dicionario de dados - https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf
https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf


no site temos os temos os dados yello , green como no case precisavamos dos dados do taxi,
 foi baixado apenas os dados de taxi.

 explicar o porque do particionamento ser por ano e mes, o ideal de um particionamente é não ser muito pequeno, mas o suficente para que na consulta leia uma pequena parte .


Os dados foram armazenados no S3 onde foi o Data Lake com a seguinte estrutura:

s3://bronze-case-ifood-geoleal/yellow_taxi/

└── year=2023/

└── month=01/

└── month=02/

└── month=03/

└── month=04/

└── month=05/

### Decisões Técnicas

#### Uso de External Location (Databricks)

Foi utilizada uma **External Location no Databricks** apontando para um bucket S3 para armazenar os dados da camada Bronze.

Essa abordagem foi escolhida porque, o S3 é um serviço que o databricks consegue gerencia, tem uma integração bem consolidada e é uma pratica de mercado, bem utilizada em ambientes produtivos.

### Armazenamento dados brutos (sem transformação)

Os dados são armazenados **exatamente como foram recebidos**, sem qualquer modificação.
É recomendado sempre manter uma camada Bronze com dados os dados brutos, pois isso permite reconstruir pipelines, auditar dados e lidar com mudanças futuras de schema

Isso garante:

- rastreabilidade completa;  
- possibilidade de reprocessamento;  
- preservação da fonte original.  


## ⚪ Camada Silver — Padronização e Qualidade de Dados

### Objetivo

A camada Silver tem como objetivo transformar os dados brutos da Bronze em um formato estruturado, consistente e pronto para consumo analítico.

Nesta etapa são aplicados:

- padronização de schema;
- tipagem explícita das colunas;
- limpeza dos dados;
- organização otimizada para consultas.

### Implementação

Os dados são lidos a partir da camada Bronze e transformados utilizando PySpark, sendo posteriormente armazenados em formato **Delta Lake**, com particionamento por ano e mês.

###  Decisões Técnicas

#### Leitura arquivo a arquivo (por mês)

Essa abordagem foi adotada devido a um comportamento observado nos dados de origem, os arquivos Parquet apresentam variações de schema físico entre os meses, especialmente em colunas numéricas (ex: INT64 vs DOUBLE).
E quando o spark vai ler os arquivos com schemas diferentes ele da erro, por isso a necessidade da leitura aqrquivo por arquivo.

#### Consideração para ambiente produtivo

Em um ambiente produtivo, com milhares de arquivos isso não é viavel,  essa limitação seria tratada com soluções mais robustas, como:
- Databricks Auto Loader (cloudFiles)
- controle de schema evolution
- uso de schema registry ou versionamento de schema
- ingestão incremental com checkpoints

Devido às limitações do ambiente utilizado no case (Databricks Community Edition), essas abordagens não foram aplicadas.

### Padronização de Schema

Todas as colunas são explicitamente convertidas para tipos consistentes:

LongType, DoubleType, TimestampType, etc.

Além disso, os nomes das colunas são padronizados para snake_case, garantindo maior legibilidade.

Exemplo:
VendorID → vendor_id
tpep_pickup_datetime → pickup_datetime

### Tratamento de inconsistências de schema

Foi identificado que a coluna airport_fee possui variação de nome entre arquivos:

airport_col = "airport_fee" if "airport_fee" in df_raw.columns else "Airport_fee"

Essa lógica evita falhas na leitura e garante consistência na estrutura final.

## Qualidade de Dados

Foi aplicada uma limpeza mínima para garantir consistência:

remoção de registros com datas nulas;
remoção de registros com total_amount nulo;
validação de integridade temporal (dropoff >= pickup);
filtro por período baseado na data real (pickup_datetime).

## 💡 Importante

Mesmo com partições físicas por ano/mês na Bronze, foi observado que o campo pickup_datetime pode conter valores fora do período esperado. Por isso, o filtro na Silver considera a data real do evento como fonte de verdade.

## Colunas auxiliares para análise

Foram adicionadas colunas derivadas:

pickup_year
pickup_month

Essas colunas são utilizadas para:

particionamento físico;
otimização de consultas analíticas.
## Uso de Delta Lake

Os dados são armazenados em formato Delta, permitindo:

melhor performance em leitura;
suporte a ACID;
evolução de schema;
integração com SQL no Databricks.

## Particionamento

A tabela é particionada por:

pickup_year, pickup_month

Essa decisão otimiza consultas temporais, que são o principal padrão de acesso esperado.

## Conclusão da Silver

A camada Silver foi projetada para garantir:

consistência de schema entre arquivos;
qualidade mínima dos dados;
padronização para consumo analítico;
otimização de leitura via Delta e particionamento.
