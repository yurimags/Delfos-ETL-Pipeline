import os
import sys
import httpx
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import Dict, List, Any, Optional
import logging

load_dotenv()

required_env_vars = ['API_URL', 'ALVO_DATABASE_URL']
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Variáveis de ambiente obrigatórias não definidas: {', '.join(missing_vars)}")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLProcessor:
    
    def __init__(self):
        self.api_url = os.getenv('API_URL')
        self.alvo_db_url = os.getenv('ALVO_DATABASE_URL')
        
        self.alvo_engine = create_engine(self.alvo_db_url)
        self.alvo_session = sessionmaker(bind=self.alvo_engine)
        
        self.signal_mapping = {}
    
    def extract_data(self, target_date: datetime) -> pd.DataFrame:
        try:
            start_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
            
            start_iso = start_date.isoformat()
            end_iso = end_date.isoformat()
            
            params = {
                'start_date': start_iso,
                'end_date': end_iso,
                'variables': 'timestamp,wind_speed,power'
            }
            
            logger.info(f"Extraindo dados para {target_date.date()}...")
            logger.info(f"URL: {self.api_url}/data/")
            logger.info(f"Parâmetros: {params}")
            
            with httpx.Client(timeout=30.0) as client:
                response = client.get(f"{self.api_url}/data/", params=params)
                response.raise_for_status()
                
                data = response.json()
                
                if not data.get('data'):
                    logger.warning(f"Nenhum dado encontrado para {target_date.date()}")
                    return pd.DataFrame()
                
                df = pd.DataFrame(data['data'])
                
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                logger.info(f"Extraídos {len(df)} registros")
                return df
                
        except httpx.HTTPStatusError as e:
            logger.error(f"Erro HTTP na API: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Erro na extração: {e}")
            raise
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            logger.warning("DataFrame vazio, pulando transformação")
            return df
        
        try:
            logger.info("Iniciando transformação dos dados...")
            
            df = df.set_index('timestamp')
            
            logger.info("Agregando dados em intervalos de 10 minutos...")
            
            wind_speed_agg = df['wind_speed'].resample('10T').agg(['mean', 'min', 'max', 'std'])
            wind_speed_agg.columns = ['wind_speed_mean', 'wind_speed_min', 'wind_speed_max', 'wind_speed_std']
            
            power_agg = df['power'].resample('10T').agg(['mean', 'min', 'max', 'std'])
            power_agg.columns = ['power_mean', 'power_min', 'power_max', 'power_std']
            
            aggregated_df = pd.concat([wind_speed_agg, power_agg], axis=1)
            
            aggregated_df = aggregated_df.dropna(how='all')
            
            logger.info(f"Dados agregados: {len(aggregated_df)} intervalos de 10 minutos")
            
            logger.info("Transformando para formato longo...")
            
            aggregated_df = aggregated_df.reset_index()
            
            long_df = aggregated_df.melt(
                id_vars=['timestamp'],
                var_name='signal_name',
                value_name='value'
            )
            
            long_df = long_df.dropna()
            
            logger.info(f"Transformação concluída: {len(long_df)} registros")
            return long_df
            
        except Exception as e:
            logger.error(f"Erro na transformação: {e}")
            raise
    
    def load_signal_mapping(self) -> Dict[str, int]:
        try:
            with self.alvo_engine.connect() as conn:
                result = conn.execute(text("SELECT id, name FROM signal"))
                mapping = {row.name: row.id for row in result}
                logger.info(f"Carregados {len(mapping)} sinais do banco alvo")
                return mapping
        except Exception as e:
            logger.error(f"Erro ao carregar mapeamento de sinais: {e}")
            raise
    
    def load_data(self, df: pd.DataFrame) -> int:
        if df.empty:
            logger.warning("DataFrame vazio, pulando carga")
            return 0
        
        try:
            logger.info("Iniciando carga dos dados...")
            
            if not self.signal_mapping:
                self.signal_mapping = self.load_signal_mapping()
            
            df['signal_id'] = df['signal_name'].map(self.signal_mapping)
            
            unmapped_signals = df[df['signal_id'].isna()]['signal_name'].unique()
            if len(unmapped_signals) > 0:
                logger.warning(f"Sinais não mapeados: {unmapped_signals}")
                df = df.dropna(subset=['signal_id'])
            
            if df.empty:
                logger.warning("Nenhum dado válido para inserir após mapeamento")
                return 0
            
            df_to_insert = df[['timestamp', 'signal_id', 'value']].copy()
            
            rows_inserted = df_to_insert.to_sql(
                'data',
                self.alvo_engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"Carga concluída: {rows_inserted} registros inseridos")
            return rows_inserted
            
        except Exception as e:
            logger.error(f"Erro na carga: {e}")
            raise
    
    def process_date(self, target_date: datetime) -> Dict[str, Any]:
        logger.info(f"Iniciando ETL para {target_date.date()}")
        
        try:
            raw_data = self.extract_data(target_date)
            
            if raw_data.empty:
                return {
                    'date': target_date.date(),
                    'status': 'no_data',
                    'records_processed': 0,
                    'records_inserted': 0
                }
            
            transformed_data = self.transform_data(raw_data)
            
            records_inserted = self.load_data(transformed_data)
            
            return {
                'date': target_date.date(),
                'status': 'success',
                'records_processed': len(raw_data),
                'records_inserted': records_inserted,
                'intervals_processed': len(transformed_data) // 8
            }
            
        except Exception as e:
            logger.error(f"Erro no processamento para {target_date.date()}: {e}")
            return {
                'date': target_date.date(),
                'status': 'error',
                'error': str(e),
                'records_processed': 0,
                'records_inserted': 0
            }

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Processo de ETL para dados de sensores')
    parser.add_argument('--date', type=str, help='Data para processar (YYYY-MM-DD)', required=True)
    parser.add_argument('--api-url', type=str, help='URL da API')
    parser.add_argument('--db-url', type=str, help='URL do banco de dados alvo')
    
    args = parser.parse_args()
    
    if args.api_url:
        os.environ['API_URL'] = args.api_url
    if args.db_url:
        os.environ['ALVO_DATABASE_URL'] = args.db_url
    
    try:
        target_date = datetime.strptime(args.date, '%Y-%m-%d')
        
        processor = ETLProcessor()
        
        result = processor.process_date(target_date)
        
        print(f"\nResultado do ETL para {result['date']}:")
        print(f"Status: {result['status']}")
        if result['status'] == 'success':
            print(f"Registros processados: {result['records_processed']}")
            print(f"Registros inseridos: {result['records_inserted']}")
            print(f"Intervalos processados: {result['intervals_processed']}")
        elif result['status'] == 'error':
            print(f"Erro: {result['error']}")
        
        sys.exit(0 if result['status'] in ['success', 'no_data'] else 1)
        
    except ValueError:
        print("Erro: Formato de data inválido. Use YYYY-MM-DD")
        sys.exit(1)
    except Exception as e:
        print(f"Erro inesperado: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
