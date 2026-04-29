-- Criar database ifood_case
CREATE DATABASE IF NOT EXISTS ifood_case;

-- Criação da tabela Silver no database ifood_case
-- É utilizada para validação dos dados ingeridos e como base para análises exploratórias,
-- Inclui coluna _source_file para rastreabilidade (data lineage) dos dados ingeridos.
-- Tabela Silver (camada de dados padronizados e integrados)
-- Contém dados unificados de diferentes tipos de táxi da NYC TLC
CREATE TABLE IF NOT EXISTS ifood_case.taxi_silver (
    taxi_type STRING COMMENT 'Tipo de taxi: yellow, green',
    vendor_id BIGINT COMMENT 'Identificador do fornecedor do serviço de taxi',
    pickup_datetime TIMESTAMP COMMENT 'Data e hora de início da corrida',
    dropoff_datetime TIMESTAMP COMMENT 'Data e hora de término da corrida',
    passenger_count DOUBLE COMMENT 'Quantidade de passageiros dentro do veiculo na corrida.',
    trip_distance DOUBLE COMMENT 'Distância percorrida na corrida (milhas)',
    rate_code_id DOUBLE COMMENT 'Código da tarifa aplicada',
    store_and_fwd_flag STRING COMMENT 'Indica se a corrida foi armazenada antes do envio',
    pu_location_id BIGINT COMMENT 'ID da zona de embarque (pickup)',
    do_location_id BIGINT COMMENT 'ID da zona de desembarque (dropoff)',
    payment_type BIGINT COMMENT 'Forma de pagamento da corrida',
    fare_amount DOUBLE COMMENT 'Valor base da corrida',
    extra DOUBLE COMMENT 'Taxas adicionais aplicadas',
    mta_tax DOUBLE COMMENT 'Taxa MTA (Metropolitan Transportation Authority)',
    tip_amount DOUBLE COMMENT 'Valor da gorjeta',
    tolls_amount DOUBLE COMMENT 'Valor de pedágios',
    improvement_surcharge DOUBLE COMMENT 'Taxa adicional de melhoria',
    total_amount DOUBLE COMMENT 'Valor total pago na corrida',
    congestion_surcharge DOUBLE COMMENT 'Taxa de congestionamento urbano',
    airport_fee DOUBLE COMMENT 'Taxa adicional de aeroporto (quando aplicável)',
    source_file STRING COMMENT 'Caminho do arquivo de origem (data lineage)',
    pickup_year INT COMMENT 'Ano da corrida (derivado do pickup_datetime)',
    pickup_month INT COMMENT 'Mês da corrida (derivado do pickup_datetime)',
    last_ingestion TIMESTAMP COMMENT 'Data e hora da última ingestão do arquivo',
    last_updated TIMESTAMP COMMENT 'Data e hora da última atualização'
)
USING DELTA
PARTITIONED BY (pickup_year, pickup_month, taxi_type);

COMMENT ON TABLE ifood_case.taxi_silver 
IS 'Tabela armazena os dados padronizados e tipados das corridas de táxi (NYC TLC)';


-- Tabela Gold (camada de consumo)
-- Dados simplificados para uso analítico
-- Particionamento por ano e mês para otimização de leitura
CREATE TABLE IF NOT EXISTS ifood_case.taxi_gold (
    vendor_id BIGINT COMMENT 'Identificador do fornecedor do serviço de taxi',
    passenger_count DOUBLE COMMENT 'Quantidade de passageiros na corrida',
    total_amount DOUBLE COMMENT 'Valor total pago na corrida',
    pickup_datetime TIMESTAMP COMMENT 'Data e hora de início da corrida',
    dropoff_datetime TIMESTAMP COMMENT 'Data e hora de término da corrida',
    pickup_year INT COMMENT 'Ano da corrida',
    pickup_month INT COMMENT 'Mês da corrida',
    taxi_type STRING COMMENT 'Tipo de taxi: yellow, green'
)
USING DELTA
PARTITIONED BY (pickup_year, pickup_month, taxi_type);

COMMENT ON TABLE ifood_case.taxi_gold 
IS 'Tabela Gold com dados de corridas do yellow táxi NYC (Jan–May 2023)';


