# README — ifood-case

## Visão Geral

Este projeto apresenta uma solução de ingestão, transformação e disponibilização de dados de corridas de táxi de Nova York (NYC TLC), seguindo o modelo de arquitetura **Data Lake em camadas (Bronze, Silver e Gold)**.

### Estrutura do Repositório

ifood-case/

├─ src/ #contém toda a pipeline de dados

│ ├─ 00_create_table.py # Criacao da tabela

│ ├─ 01_ingestion.py # Ingestão camada Bronze

│ ├─ 02_bronze_to_silver.py # Transformação Bronze → Silver

│ ├─ 03_silver_to_gold.py # Transformação Silver → Gold

│ └─ variables.py 

├─ analysis/ # Queries e análises

│ ├─ analisys.ipynb

├─ README.md #documentação da solução

### Arquitetura da Solução

A solução foi estruturada seguindo o padrão de camadas:

- **Bronze** → dados bruto  
- **Silver** → dados tratados e padronizados  
- **Gold** → dados prontos para consumo analítico  

## 🟤 Camada Bronze — Ingestão

#### Objetivo

Ingerir os arquivos do ano de 2023 e meses de janeiro a maio de corridas de táxi, disponibilizados pela NYC TLC, mantendo os dados em seu formato original e garantindo rastreabilidade.

#### Implementação

A ingestão foi realizada diretamente a partir da fonte pública:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Dicionario de dados utilizado para entendimento dos dados da frota de taxi:
Green: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf
Yellow: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

Na camada bronze os dados estao na estrutura de particionamento hive por ano e mes, baseado na data de referencia da base de dados disponibilizadas no site, cuja  atualizacoa incremental é mensal.
 A regra do particionamento segue a forma em que os dados serão consultados nas proximas camadas, como a etapa de processamento será realizada mensalmente, foi definido por ano e mes, o que evita que seja rastreados todos os dados para que seja escaneado os dados mensal.

Os dados da camada bronze foram armazenados no S3 com a seguinte estrutura:

s3://bronze-case-ifood-geoleal/nome_da_frota/

└── year=YYYY/

└── month=MM/


#### Decisões Técnicas

##### Uso de External Location (Databricks)

Foi utilizada uma **External Location no Databricks** apontando para um bucket S3 para armazenar os dados da camada Bronze,
 que uma funcionalidade disponibilizada pelo databricks e o S3.

#### Armazenamento dados brutos (sem transformação)

Os dados são armazenados exatamente como foram recebidos, sem qualquer modificação.
É recomendado sempre manter uma camada Bronze com  os dados brutos, pois isso permite reconstruir pipelines, auditar dados, reprocessamento e lidar com mudanças futuras de schema


## ⚪ Camada Silver — Padronização e Qualidade de Dados

### Objetivo

A camada Silver tem como objetivo transformar os dados brutos da Bronze em um formato estruturado, consistente e pronto para consumo analítico.

Nesta etapa são aplicados:

- padronização de schema;
- tipagem explícita das colunas;
- tratamento de inconsistências entre arquivos;
- adição de metadados de linhagem;
- organização otimizada para leitura.

###  Implementação

Os dados são lidos a partir da camada Bronze, transformados utilizando PySpark e armazenados em formato **Delta Lake** em uma tabela no database `silver`.

A carga é realizada de forma **incremental**, utilizando `MERGE`, garantindo:

- atualização apenas quando necessário;
- inserção de novos registros;
- consistência transacional (ACID).

---

### Decisões Técnicas

#### Leitura arquivo a arquivo (por mês)

Essa abordagem foi adotada devido a um comportamento observado nos dados de origem:

Os arquivos Parquet apresentam variações de schema físico entre os meses, especialmente em colunas numéricas (ex: `INT64` vs `DOUBLE`).

Quando o Spark tenta ler múltiplos arquivos com schemas diferentes simultaneamente, ocorre erro de inferência.

Por isso, foi adotada a leitura arquivo a arquivo, garantindo controle total sobre a padronização.

##### Consideração para ambiente produtivo

Em um ambiente produtivo com grande volume de dados, essa abordagem não escala.

Nesse cenário, seriam adotadas soluções mais robustas, como:

- Databricks Auto Loader (`cloudFiles`)
- controle de schema evolution
- versionamento de schema
- ingestão incremental com checkpoint

#### Padronização de Schema

Todas as colunas são explicitamente convertidas para tipos consistentes:

- `LongType`
- `DoubleType`
- `TimestampType`
- `StringType`

Além disso, os nomes das colunas são padronizados para **snake_case**, garantindo maior legibilidade e padronização.

**Exemplo:**

| Origem                  | Silver            |
|------------------------|------------------|
| VendorID               | vendor_id        |
| tpep_pickup_datetime   | pickup_datetime  |

---

#### Tratamento de inconsistências de schema

Foi identificado que alguns arquivos apresentam variação de nomenclatura:

Exemplo:

```python
airport_col = "airport_fee" if "airport_fee" in df_raw.columns else "Airport_fee"
```

Essa abordagem garante resiliência na leitura e consistência no schema final.

Também foi realizada a padronização das colunas de data:

tpep_* (yellow taxi)
lpep_* (green taxi)

#### Análise dos dados

Os dados foram ingeridos conforme o escopo do case:

Ano: 2023
Período: Janeiro a Maio

Durante a análise na camada Silver, foi observado que:

existem registros com datas no período de (ex: 2021 até novembro de 2023)

Esses dados foram mantidos na Silver, pois essa camada preserva maior fidelidade à origem.

#### Colunas auxiliares

Foram adicionadas colunas derivadas para otimização e governança:

pickup_year → utilizada no particionamento
pickup_month → utilizada no particionamento
source_file → rastreabilidade (linhagem do dado)
last_ingestion → auditoria de carga
last_updated → auditoria de atualização

#### Uso de Delta Lake

Os dados são armazenados em formato Delta, permitindo:
- melhor performance em leitura;
- suporte a transações ACID;
- controle de atualizações via MERGE;
- evolução de schema;
- integração com SQL no Databricks.

#### Particionamento

A tabela é particionada por:
```text
taxi_type, pickup_year, pickup_month
```
Essa estratégia:
- evita alta cardinalidade;
- melhora a performance de consultas filtradas por período e tipo;
- está alinhada com o padrão de acesso esperado.

#### Estratégia de carga

A carga é incremental via MERGE, com:

chave negócio definida:
```text
"vendor_id","pickup_datetime", "dropoff_datetime", "taxi_type", "pu_location_id", "do_location_id"
```
atualização apenas quando há mudança real nos dados;
inserção de novos registros.

Isso reduz custo de processamento e evita reescritas desnecessárias.

##### Conclusão da Silver

A camada Silver foi projetada para garantir:

consistência de schema entre arquivos;
padronização dos dados;
rastreabilidade (data lineage);
base confiável para consumo analítico;
otimização de leitura via Delta e particionamento.

## 🟡 Gold Layer – Data Processing

#### Objetivo

A camada **Gold** tem como objetivo disponibilizar dados prontos para consumo analítico, com um modelo simplificado, consistente e otimizado para consultas de negócio.

Nesta etapa, os dados da camada Silver são filtrados, refinados e enriquecidos com regras de qualidade, garantindo confiabilidade para dashboards, análises e exploração de dados.


#### Fonte de Dados

- Origem: Tabela Silver (`TABLE_SILVER`)
- Formato: Delta Lake
- Período: definido via variáveis `YEAR` e `MONTH`

#### Transformações Aplicadas

##### 1. Filtro de Período
Seleciona apenas os dados do ano e meses definidos, conforme solicitação:

- `pickup_year = YEAR`
- `pickup_month IN (MONTH)`

##### 2. Seleção de Colunas

A camada Gold mantém apenas os campos necessários para consumo analítico:

| Coluna            | Descrição |
|------------------|----------|
| vendor_id        | Identificador do fornecedor |
| passenger_count  | Quantidade de passageiros |
| total_amount     | Valor total da corrida |
| pickup_datetime  | Data/hora de início |
| dropoff_datetime | Data/hora de término |
| pickup_year      | Ano da corrida |
| pickup_month     | Mês da corrida |
| taxi_type        | Tipo de taxi |


##### 3. Regras de Qualidade de Dados

Antes da carga na Gold, são aplicadas validações para garantir consistência, essas regras são definidas com a area de negócio, fazendo sentido para a analise:

- Remoção de registros com `pickup_datetime` nulo  
- Remoção de registros com `dropoff_datetime` nulo  
- Remoção de registros com `total_amount` nulo  
- Validação temporal:
  - `dropoff_datetime >= pickup_datetime`

Existe registros com a coluna passenger_count nula, esse tratamento não foi feito, poderia ter criado uma regra que se existe corrida com data e hora e valor pago considerasse no minimo uma pessoa.

#### 4. Deduplicação

Remoção de registros duplicados com base na granularidade da corrida, usada a chave de negocio:

```text
vendor_id + pickup_datetime + dropoff_datetime + taxi_type
```

#### Estratégia de Carga

A carga na Gold é incremental, utilizando MERGE no Delta Lake.

##### Chave de negócio

- vendor_id
- pickup_datetime
- dropoff_datetime
- taxi_type

#### Atualização de dados

Registros existentes são atualizados somente quando há alteração real nos dados, evitando reprocessamento desnecessário.

Campos monitorados:
passenger_count
total_amount

Inserção de novos registros: 
Registros inexistentes na Gold são inseridos normalmente.

##### Estrutura da Tabela
  Formato: Delta Lake
  Particionamento:
  pickup_year, pickup_month

Esse particionamento melhora a performance de consultas filtradas por período.


#### Boas Práticas Aplicadas

- Separação por camadas, ideal para governança, regras de quem pode acessar cada camada
- Redução de colunas, apenas colunas necessarias para o negocio, otimizando a leitura
- Deduplicação antes da persistência
- Carga incremental com MERGE
- Evita updates desnecessários (performance + custo)
- Aplicação de regras de qualidade na camada de consumo
- Particionamento orientado a uso analítico

#### Consumo

A camada Gold está pronta para:
- Dashboards 
- Queries analíticas (Spark SQL / Databricks SQL)
- Exploração de dados por analistas


## Passo a passo para executar o codigo

1 - Criar uma extarnal location apontando para o armazento que tiver disponivel na location databricks e tiver acesso, no caso do case foi utilizado o AWS S3, que tenho disponivel

2 - Rodar o script src/00_create_table, que cria as tabelas necessarias para o pipeline

3 - preencher as variaveis no arquivo 01_variables.py, como o nome do external location, os demais já estão preenchidos conforme a regra definida.

4-  executar o codigo src/02_ingestion.py, script extrai os dados do site e faz a ingestão na camada bronze no S3

5 - executar o codigo src/03_bronze_to_silver.py,  faz a ingestão dos dados a tabela taxi_silver

6- executar o codigo src/04_silver_to_gold.py,  faz  a ingestão dos dados na tabela taxi_gold

7- Notebook de analises dos dados, executar as consultas para analises dos dados, adicionado os comentarios em cada analise. 






