# ifood-case
Case técnico engenharia de dados
## Ingestão de Dados (Camada Bronze)

A ingestão dos dados foi realizada diretamente a partir da fonte pública da NYC TLC, utilizando arquivos no formato Parquet.

Deixando a ingestão mais automatica os dados são copiados diretamente da URL de origem para a camada Bronze no Data Lake (S3), utilizando o Databricks.

A camada Bronze armazena os arquivos Parquet originais disponibilizados pela NYC TLC, sem alteração de schema ou transformação. Os dados são particionados fisicamente por ano e mês no S3 para facilitar rastreabilidade, organização e reprocessamento.

### Estratégia adotada

- Leitura dos dados diretamente da fonte pública (CloudFront)
- Cópia automatizada para o bucket S3
- Organização dos dados em partições por ano e mês
- Armazenamento no formato original (Parquet), sem transformações


## Transformação (Camada silver)
A camada Silver foi criada em formato Delta Lake para disponibilizar dados estruturados e padronizados para consumo analítico. Nessa etapa, os arquivos originais da Bronze são lidos mês a mês, têm seus tipos normalizados, suas colunas renomeadas para um padrão mais consistente e passam por validações mínimas de qualidade. A tabela é particionada por ano e mês de pickup para otimizar consultas temporais, que são o principal padrão de acesso esperado para as análises solicitadas no case.