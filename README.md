# README — ifood-case

## Visão Geral

Este projeto apresenta uma solução de ingestão, transformação e disponibilização de dados de corridas de táxi de Nova York (NYC TLC), seguindo o modelo de arquitetura **Data Lake em camadas (Bronze, Silver e Gold)**.

### Estrutura do Repositório

ifood-case/

├─ src/ # Contém toda a pipeline de dados  
│ ├─ 00_create_table.py # Criação da tabela  
│ ├─ 01_variables.py # Variáveis globais  
│ ├─ 02_ingestion.py # Ingestão camada Bronze  
│ ├─ 03_bronze_to_silver.py # Transformação Bronze → Silver  
│ ├─ 04_silver_to_gold.py # Transformação Silver → Gold  
│ └─ 05_create_job.py # Criação da pipeline  

├─ analysis/ # Queries e análises  
│ ├─ analisys.ipynb  

├─ README.md # Documentação da solução  

### Arquitetura da Solução

A solução foi estruturada seguindo o padrão de camadas:

- **Bronze** → dados brutos  
- **Silver** → dados tratados e padronizados  
- **Gold** → dados prontos para consumo analítico  

Desenvolvida no Databricks Community Edition, utilizando Spark e S3.

<p align="center">
  <img src="../ifood-case/image/arquitetura_solucao.png" width="800">
</p>

## 🟤 Camada Bronze — Ingestão

#### Objetivo

Ingerir os arquivos do ano de 2023, dos meses de janeiro a maio, referentes às corridas de táxi disponibilizadas pela NYC TLC, mantendo os dados em seu formato original e garantindo rastreabilidade. 

#### Implementação

A ingestão da camada Bronze é realizada pelo script:

`src/02_ingestion.py`

Os dados são extraídos a partir da fonte pública:  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

O dicionário de dados utilizado para entendimento do dadaset das frotas de táxi:

Green: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf  
Yellow: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf  

Na camada Bronze, os dados estão organizados com particionamento Hive por ano e mês, baseado na data de referência dos dados disponibilizados no site, cuja atualização incremental é mensal.

A estratégia de particionamento segue o padrão de consumo das próximas camadas. Como o processamento é mensal, foi definido particionamento por ano e mês, evitando a leitura desnecessária de grandes volumes de dados.

Os dados da camada Bronze foram armazenados no S3 com a seguinte estrutura:

s3://bronze-case-ifood-geoleal/nome_da_frota/

└── year=YYYY/  
  └── month=MM/

#### Decisões Técnicas

##### Uso de External Location (Databricks)

Foi utilizada uma **External Location no Databricks** apontando para um bucket S3 para armazenar os dados da camada Bronze, utilizando integração nativa entre Databricks e S3.

#### Armazenamento de dados brutos (sem transformação)

Os dados são armazenados exatamente como foram recebidos, sem qualquer modificação.

Manter a camada Bronze com dados brutos é uma boa prática, pois permite:

- reconstrução de pipelines;
- auditoria de dados;
- reprocessamento;
- adaptação a mudanças futuras de schema.

---

## ⚪ Camada Silver — Padronização e Qualidade de Dados

### Objetivo

A camada Silver tem como objetivo transformar os dados brutos da Bronze em um formato estruturado, consistente e pronto para consumo analítico.

Nesta etapa são aplicados:

- padronização de schema;
- tipagem explícita das colunas;
- tratamento de inconsistências entre arquivos;
- adição de metadados de linhagem;
- organização otimizada para leitura.

### Implementação

Os dados são lidos a partir da camada Bronze, transformados utilizando PySpark e armazenados em formato **Delta Lake** em uma tabela no database `silver`.

A carga é realizada de forma **incremental**, utilizando `MERGE`, garantindo:

- atualização apenas quando necessário;
- inserção de novos registros;
- consistência transacional (ACID).

### Decisões Técnicas

#### Leitura arquivo a arquivo (por mês)

Essa abordagem foi adotada devido a um comportamento observado nos dados de origem:

Os arquivos Parquet apresentam variações de schema físico entre os meses, especialmente em colunas numéricas (ex: `INT64` vs `DOUBLE`).

Quando o Spark tenta ler múltiplos arquivos com schemas diferentes simultaneamente, ocorre erro de inferência.

Por isso, foi adotada a leitura arquivo a arquivo, garantindo controle total sobre a padronização.

##### Consideração para ambiente produtivo

Em um ambiente produtivo com grande volume de dados, essa abordagem não escala.

Nesse cenário, seriam adotadas soluções mais robustas, como:

- Databricks Auto Loader (`cloudFiles`);
- controle de schema evolution;
- versionamento de schema;
- ingestão incremental com checkpoint.

#### Padronização de Schema

Todas as colunas são explicitamente convertidas para tipos consistentes:

- `LongType`
- `DoubleType`
- `TimestampType`
- `StringType`

Além disso, os nomes das colunas são padronizados para **snake_case**, garantindo maior legibilidade e padronização.

**Exemplo:**

| Origem                | Silver           |
|----------------------|------------------|
| VendorID             | vendor_id        |
| tpep_pickup_datetime | pickup_datetime  |

---

#### Tratamento de inconsistências de schema

Foi identificado que alguns arquivos apresentam variação de nomenclatura:

Exemplo:

airport_col = "airport_fee" if "airport_fee" in df_raw.columns else "Airport_fee"

Essa abordagem garante resiliência na leitura e consistência no schema final.

Também foi realizada a padronização das colunas de data:

- `tpep_*` (yellow taxi)  
- `lpep_*` (green taxi)

#### Análise dos dados

Os dados foram ingeridos conforme o escopo do case:

- Ano: 2023  
- Período: Janeiro a Maio  

Durante a análise na camada Silver, foi observado que existem registros fora desse período (ex: de 2021 até novembro de 2023).

Esses dados foram mantidos na Silver, pois essa camada preserva maior fidelidade à origem.

#### Colunas auxiliares

Foram adicionadas colunas derivadas para otimização e governança:

- pickup_year → particionamento  
- pickup_month → particionamento  
- source_file → rastreabilidade  
- last_ingestion → auditoria de carga  
- last_updated → auditoria de atualização  

#### Uso de Delta Lake

Os dados são armazenados em formato Delta, permitindo:

- melhor performance em leitura;
- suporte a transações ACID;
- controle de atualizações via MERGE;
- evolução de schema;
- integração com SQL no Databricks.

#### Particionamento

A tabela é particionada por:

taxi_type, pickup_year, pickup_month

Essa estratégia:

- evita alta cardinalidade;
- melhora a performance de consultas filtradas por período e tipo;
- está alinhada com o padrão de acesso esperado.

#### Estratégia de carga

A carga é incremental via MERGE, com chave de negócio definida:

"vendor_id","pickup_datetime","dropoff_datetime","taxi_type","pu_location_id","do_location_id"

- atualização apenas quando há mudança real nos dados;
- inserção de novos registros.

Isso reduz custo de processamento e evita reescritas desnecessárias.

##### Conclusão da Silver

A camada Silver foi projetada para garantir:

- consistência de schema entre arquivos;
- padronização dos dados;
- rastreabilidade (data lineage);
- base confiável para consumo analítico;
- otimização de leitura via Delta e particionamento.

---

## 🟡 Gold Layer – Data Processing

#### Objetivo

A camada Gold tem como objetivo disponibilizar dados prontos para consumo analítico, com um modelo simplificado, consistente e otimizado para consultas de negócio.

#### Fonte de Dados

- Origem: Tabela Silver (`TABLE_SILVER`)
- Formato: Delta Lake
- Período: definido via variáveis `YEAR` e `MONTH`

#### Transformações Aplicadas

##### 1. Filtro de Período

Seleciona apenas os dados do ano e meses definidos:

- `pickup_year = YEAR`
- `pickup_month IN (MONTH)`

##### 2. Seleção de Colunas

| Coluna            | Descrição |
|------------------|----------|
| vendor_id        | Identificador do fornecedor |
| passenger_count  | Quantidade de passageiros |
| total_amount     | Valor total da corrida |
| pickup_datetime  | Data/hora de início |
| dropoff_datetime | Data/hora de término |
| pickup_year      | Ano da corrida |
| pickup_month     | Mês da corrida |
| taxi_type        | Tipo de táxi |

##### 3. Regras de Qualidade de Dados

Antes da carga na Gold, são aplicadas validações para garantir consistência:

- Remoção de registros com `pickup_datetime` nulo  
- Remoção de registros com `dropoff_datetime` nulo  
- Remoção de registros com `total_amount` nulo  
- Validação temporal:
  - `dropoff_datetime >= pickup_datetime`

Observação: Existem registros com `passenger_count` nulo. Não foi aplicado tratamento, mas poderia ser considerada uma regra assumindo no mínimo 1 passageiro.

#### 4. Deduplicação

Remoção de registros duplicados com base na chave de negócio:

vendor_id + pickup_datetime + dropoff_datetime + taxi_type

#### Estratégia de Carga

A carga na Gold é incremental, utilizando MERGE no Delta Lake.

##### Chave de negócio

- vendor_id  
- pickup_datetime  
- dropoff_datetime  
- taxi_type  

#### Atualização de dados

Registros existentes são atualizados apenas quando há alteração nos campos:

- passenger_count  
- total_amount  

Inserção de novos registros ocorre normalmente.

##### Estrutura da Tabela

- Formato: Delta Lake  
- Particionamento:
  - pickup_year
  - pickup_month  

Esse particionamento melhora a performance de consultas.

#### Boas Práticas Aplicadas

- Separação por camadas  
- Redução de colunas  
- Deduplicação  
- Carga incremental com MERGE  
- Otimização de performance e custo  
- Aplicação de regras de qualidade  
- Particionamento orientado ao consumo  

#### Consumo

A camada Gold está pronta para:

- Dashboards  
- Queries analíticas (Spark SQL / Databricks SQL)  
- Exploração de dados  

---

### Analises e resultados

Qual a média de valor total (total_amount) recebido em um mês considerando todos os yellow táxis da frota? 

A análise indica que o mês de maio apresentou a maior média de valor recebido quando comparado aos demais meses.

<p align="center">
  <img src="../ifood-case/image/valor_medio_pago_por_mes.png" width="450">
</p>

Qual a média de passageiros (passenger_count) por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?

Com base no resultado, o horário das duas da manhã apresenta a maior média de passageiros por corrida.

<p align="center">
  <img src="../ifood-case/image/media_passageiro_por_corrida.png" width="400">
</p>


Qual o dia da semana em que a duração média das corridas foi maior no mês de maio, considerando todos os táxis da frota?

A análise indica que, às quintas-feiras, ocorre a maior duração média das corridas.

<p align="center">
  <img src="../ifood-case/image/media_duracao_corrida_dia_semana.png" width="450">
</p>

### Passo a passo para execução

#### Pré-requisitos

1 - Criar uma External Location no Databricks apontando para um armazenamento ao qual você tenha acesso.  
Neste case, foi utilizado o AWS S3.

2 - Executar o script `src/00_create_table.py`, responsável pela criação das tabelas do pipeline.

---

#### Opção 1 — Execução via Job (pipeline)

3 - Preencher as variáveis no arquivo `01_variables.py`, incluindo o nome da External Location, WORKSPACE e WAREHOUSE_NAME
Os demais parâmetros já estão definidos conforme as regras do projeto, caso necessario, apenas alterar para os valores correspondete.

4 - Executar o script `src/05_create_job.py` para criação da pipeline no Databricks.

5 - Executar a pipeline através da console **Jobs & Pipelines do Databricks**.  
Obs: não foi configurado agendamento para este case.

6 - Executar o notebook de análises, que contém as queries e comentários explicativos.

---

#### Opção 2 — Execução manual (step-by-step)

3 - Preencher as variáveis no arquivo `01_variables.py`, incluindo o nome da External Location.  
Os demais parâmetros já estão definidos conforme as regras do projeto,caso necessario, apenas alterar para os valores correspondete.

4 - Executar o script `src/02_ingestion.py`, responsável pela ingestão dos dados na camada Bronze no S3.

5 - Executar o script `src/03_bronze_to_silver.py`, responsável pela transformação e carga na tabela `taxi_silver`.

6 - Executar o script `src/04_silver_to_gold.py`, responsável pela carga dos dados na tabela `taxi_gold`.

7 - Executar o notebook de análises para validação e exploração dos dados.