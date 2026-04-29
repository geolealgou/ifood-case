-- Criar database ifood_case
CREATE DATABASE IF NOT EXISTS ifood_case;

-- Criação da tabela Silver no database ifood_case
-- É utilizada para validação dos dados ingeridos e como base para análises exploratórias,
-- Inclui coluna _source_file para rastreabilidade (data lineage) dos dados ingeridos.
CREATE TABLE IF NOT EXISTS ifood_case.taxi_silver
USING DELTA
LOCATION 's3://silver-case-ifood-geoleal/taxi_trips/';

COMMENT ON TABLE ifood_case.taxi_silver 
IS 'Tabela armazena os dados padronizados e tipados das corridas de táxi (NYC TLC)';


-- Tabela Gold (camada de consumo)
-- Dados simplificados para uso analítico
-- Particionamento por ano e mês para otimização de leitura
CREATE TABLE IF NOT EXISTS ifood_case.taxi_gold (
    VendorID BIGINT COMMENT 'Identificador do fornecedor do serviço de taxi',
    passenger_count DOUBLE COMMENT 'Quantidade de passageiros na corrida',
    total_amount DOUBLE COMMENT 'Valor total pago na corrida',
    pickup_datetime TIMESTAMP COMMENT 'Data e hora de início da corrida',
    dropoff_datetime TIMESTAMP COMMENT 'Data e hora de término da corrida',
    pickup_year INT COMMENT 'Ano da corrida',
    pickup_month INT COMMENT 'Mês da corrida',
    pickup_day INT COMMENT 'Dia da corrida derivado do pickup_datetime',
    taxi_type STRING COMMENT 'Tipo de taxi: yellow, green, fhv, fhvhv'
)
USING DELTA
PARTITIONED BY (pickup_year, pickup_month);

COMMENT ON TABLE ifood_case.taxi_gold 
IS 'Tabela Gold com dados de corridas do yellow táxi NYC (Jan–May 2023)';


