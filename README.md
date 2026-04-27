# README — ifood-case

## Visão Geral

Este projeto apresenta uma solução de ingestão, transformação e disponibilização de dados de corridas de táxi de Nova York (NYC TLC), seguindo o modelo de arquitetura **Data Lake em camadas (Bronze, Silver e Gold)**.

O objetivo é demonstrar boas práticas de engenharia de dados, incluindo:

- ingestão de dados externos;
- organização em camadas;
- padronização e qualidade de dados;
- disponibilização para consumo analítico via SQL.

---

## 🗂️ Estrutura do Repositório

ifood-case/

├─ src/

│ ├─ 01_ingestion.py # Ingestão camada Bronze

│ ├─ 02_bronze_to_silver.py # Transformação Bronze → Silver

│ └─ 03_silver_to_gold.py # Transformação Silver → Gold

├─ analysis/ # Queries e análises

├─ README.md

### Organização

- **src/**: contém toda a pipeline de dados  
- **analysis/**: contém as análises solicitadas no case  
- **README.md**: documentação da solução  

---

## Arquitetura da Solução

A solução foi estruturada seguindo o padrão de camadas:

- **Bronze** → dados crus (raw)  
- **Silver** → dados tratados e padronizados  
- **Gold** → dados prontos para consumo analítico  

---

# 🟤 Camada Bronze — Ingestão

## Objetivo

Ingerir os arquivos originais de corridas de táxi (Yellow Taxi) disponibilizados pela NYC TLC, mantendo os dados em seu formato original e garantindo rastreabilidade.

---

## Implementação

A ingestão foi realizada diretamente a partir da fonte pública:

https://d37ci6vzurychx.cloudfront.net/trip-data

Os dados foram armazenados no Data Lake com a seguinte estrutura:

s3://bronze-case-ifood-geoleal/yellow_taxi/

└── year=2023/

└── month=01/

└── month=02/

└── month=03/

└── month=04/

└── month=05/


---

## 🧠 Decisões Técnicas

### 🔹 Uso de External Location (Databricks)

Foi utilizada uma **External Location no Databricks** apontando para um bucket S3 para armazenar os dados da camada Bronze.

Essa abordagem foi escolhida porque:

- permite **separação entre storage e compute**;  
- segue boas práticas de **governança de dados com Unity Catalog**;  
- facilita controle de acesso e organização do Data Lake;  
- é o padrão recomendado em arquiteturas modernas de Lakehouse.  

**Observação importante:**

Devido às limitações do ambiente de desenvolvimento (Databricks Community Edition), a utilização de External Location foi a melhor alternativa disponível para simular um ambiente produtivo.

---

### 🔹 Armazenamento Raw (sem transformação)

Os dados são armazenados **exatamente como foram recebidos**, sem qualquer modificação.

Isso garante:

- rastreabilidade completa;  
- possibilidade de reprocessamento;  
- preservação da fonte original.  

Em um ambiente produtivo:

É recomendado sempre manter uma camada Bronze com dados crus (raw), pois isso permite reconstruir pipelines, auditar dados e lidar com mudanças futuras de schema.

---

### 🔹 Idempotência

A ingestão utiliza:

```python
dbutils.fs.cp(..., overwrite=True)

Isso garante que o processo pode ser reexecutado sem gerar duplicidade.

🏁 Conclusão camada Bronze

A camada Bronze foi implementada seguindo boas práticas de Data Lake, garantindo:

armazenamento confiável dos dados originais;
organização por partições (ano/mês);
rastreabilidade e reprocessamento;
preparação adequada para as próximas camadas.

# ⚪ Camada Silver — Padronização e Qualidade de Dados

## Objetivo

A camada Silver tem como objetivo transformar os dados brutos da Bronze em um formato estruturado, consistente e pronto para consumo analítico.

Nesta etapa são aplicados:

- padronização de schema;
- tipagem explícita das colunas;
- limpeza mínima de dados;
- organização otimizada para consultas.

---

## Implementação

Os dados são lidos a partir da camada Bronze e transformados utilizando PySpark, sendo posteriormente armazenados em formato **Delta Lake**, com particionamento por ano e mês.

---

##  Decisões Técnicas

### Leitura arquivo a arquivo (por mês)

A leitura dos dados é feita iterando sobre os diretórios mensais:

```python
for month_id in months:
    df_raw = spark.read.parquet(path)
    
Essa abordagem foi adotada devido a um comportamento observado nos dados de origem:

Os arquivos Parquet apresentam variações de schema físico entre os meses, especialmente em colunas numéricas (ex: INT64 vs DOUBLE).

Essas diferenças causam erros como:

FAILED_READ_FILE.PARQUET_COLUMN_DATA_TYPE_MISMATCH

## Solução adotada
leitura individual por mês;
aplicação de cast explícito para um schema padronizado;
união dos dados com unionByName.

## Consideração

Embora a leitura arquivo a arquivo não seja a abordagem mais performática para grandes volumes, ela garante consistência e previsibilidade no contexto deste case.

## Consideração para ambiente produtivo

Em um ambiente produtivo, essa limitação seria tratada com soluções mais robustas, como:

Databricks Auto Loader (cloudFiles)
controle de schema evolution
uso de schema registry ou versionamento de schema
ingestão incremental com checkpoints

Devido às limitações do ambiente utilizado no case (Databricks Community Edition), essas abordagens não foram aplicadas.

## Padronização de Schema

Todas as colunas são explicitamente convertidas para tipos consistentes:

LongType, DoubleType, TimestampType, etc.

Além disso, os nomes das colunas são padronizados para snake_case, garantindo maior consistência e legibilidade.

Exemplo:

VendorID → vendor_id
tpep_pickup_datetime → pickup_datetime

## Tratamento de inconsistências de schema

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