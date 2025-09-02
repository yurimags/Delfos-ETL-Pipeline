-- Script de inicialização do banco de dados Fonte
-- Cria a tabela data com as colunas especificadas

CREATE TABLE IF NOT EXISTS data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    wind_speed FLOAT,
    power FLOAT,
    ambient_temprature FLOAT
);

-- Criar índice para melhorar performance das consultas por timestamp
CREATE INDEX IF NOT EXISTS idx_data_timestamp ON data(timestamp);

-- Comentários sobre a tabela
COMMENT ON TABLE data IS 'Tabela contendo dados de sensores com timestamp, velocidade do vento, potência e temperatura ambiente';
COMMENT ON COLUMN data.timestamp IS 'Timestamp da medição';
COMMENT ON COLUMN data.wind_speed IS 'Velocidade do vento em m/s';
COMMENT ON COLUMN data.power IS 'Potência em kW';
COMMENT ON COLUMN data.ambient_temprature IS 'Temperatura ambiente em graus Celsius';
