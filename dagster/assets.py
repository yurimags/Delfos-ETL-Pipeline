import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dagster import (
    asset, 
    DailyPartitionsDefinition,
    AssetExecutionContext,
    Output,
    MetadataValue
)
from sqlalchemy import text
import httpx
from typing import Dict, Any

daily_partitions = DailyPartitionsDefinition(
    start_date="2025-08-10",
    end_date="2025-08-20"
)

@asset(
    partitions_def=daily_partitions,
    description="Dados agregados de sensores processados diariamente",
    required_resource_keys={"fonte_db", "alvo_db", "api"}
)
def processed_sensor_data(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    fonte_db = context.resources.fonte_db
    alvo_db = context.resources.alvo_db
    api_client = context.resources.api
    
    partition_date = context.partition_key
    target_date = datetime.strptime(partition_date, "%Y-%m-%d")
    
    context.log.info(f"Processando dados para {partition_date}")
    
    try:
        context.log.info("Iniciando extração de dados...")
        
        start_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)
        
        params = {
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'variables': 'timestamp,wind_speed,power'
        }
        
        with api_client.get_client() as client:
            response = client.get("/data/", params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get('data'):
                context.log.warning(f"Nenhum dado encontrado para {partition_date}")
                return Output(
                    value={
                        'date': partition_date,
                        'status': 'no_data',
                        'records_processed': 0,
                        'records_inserted': 0,
                        'intervals_processed': 0
                    },
                    metadata={
                        "date": MetadataValue.text(partition_date),
                        "status": MetadataValue.text("no_data"),
                        "records_processed": MetadataValue.int(0)
                    }
                )
            
            df = pd.DataFrame(data['data'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            context.log.info(f"Extraídos {len(df)} registros")
        
        context.log.info("Iniciando transformação dos dados...")
        
        df = df.set_index('timestamp')
        
        wind_speed_agg = df['wind_speed'].resample('10T').agg(['mean', 'min', 'max', 'std'])
        wind_speed_agg.columns = ['wind_speed_mean', 'wind_speed_min', 'wind_speed_max', 'wind_speed_std']
        
        power_agg = df['power'].resample('10T').agg(['mean', 'min', 'max', 'std'])
        power_agg.columns = ['power_mean', 'power_min', 'power_max', 'power_std']
        
        aggregated_df = pd.concat([wind_speed_agg, power_agg], axis=1)
        aggregated_df = aggregated_df.dropna(how='all')
        
        aggregated_df = aggregated_df.reset_index()
        long_df = aggregated_df.melt(
            id_vars=['timestamp'],
            var_name='signal_name',
            value_name='value'
        )
        long_df = long_df.dropna()
        
        context.log.info(f"Transformação concluída: {len(long_df)} registros")
        
        context.log.info("Iniciando carga dos dados...")
        
        with alvo_db.get_engine().connect() as conn:
            result = conn.execute(text("SELECT id, name FROM signal"))
            signal_mapping = {row.name: row.id for row in result}
        
        long_df['signal_id'] = long_df['signal_name'].map(signal_mapping)
        
        long_df = long_df.dropna(subset=['signal_id'])
        
        if long_df.empty:
            context.log.warning("Nenhum dado válido para inserir após mapeamento")
            return Output(
                value={
                    'date': partition_date,
                    'status': 'no_valid_data',
                    'records_processed': len(df),
                    'records_inserted': 0,
                    'intervals_processed': 0
                },
                metadata={
                    "date": MetadataValue.text(partition_date),
                    "status": MetadataValue.text("no_valid_data"),
                    "records_processed": MetadataValue.int(len(df)),
                    "records_inserted": MetadataValue.int(0)
                }
            )
        
        df_to_insert = long_df[['timestamp', 'signal_id', 'value']].copy()
        
        rows_inserted = df_to_insert.to_sql(
            'data',
            alvo_db.get_engine(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        context.log.info(f"Carga concluída: {rows_inserted} registros inseridos")
        
        result = {
            'date': partition_date,
            'status': 'success',
            'records_processed': len(df),
            'records_inserted': rows_inserted,
            'intervals_processed': len(aggregated_df)
        }
        
        return Output(
            value=result,
            metadata={
                "date": MetadataValue.text(partition_date),
                "status": MetadataValue.text("success"),
                "records_processed": MetadataValue.int(len(df)),
                "records_inserted": MetadataValue.int(rows_inserted),
                "intervals_processed": MetadataValue.int(len(aggregated_df)),
                "wind_speed_stats": MetadataValue.json({
                    "mean": float(df['wind_speed'].mean()),
                    "min": float(df['wind_speed'].min()),
                    "max": float(df['wind_speed'].max()),
                    "std": float(df['wind_speed'].std())
                }),
                "power_stats": MetadataValue.json({
                    "mean": float(df['power'].mean()),
                    "min": float(df['power'].min()),
                    "max": float(df['power'].max()),
                    "std": float(df['power'].std())
                })
            }
        )
        
    except Exception as e:
        context.log.error(f"Erro no processamento para {partition_date}: {e}")
        
        return Output(
            value={
                'date': partition_date,
                'status': 'error',
                'error': str(e),
                'records_processed': 0,
                'records_inserted': 0,
                'intervals_processed': 0
            },
            metadata={
                "date": MetadataValue.text(partition_date),
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(str(e))
            }
        )

@asset(
    description="Visualização dos dados processados no banco alvo",
    required_resource_keys={"alvo_db"}
)
def alvo_database_summary(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    alvo_db = context.resources.alvo_db
    
    try:
        with alvo_db.get_engine().connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) as total FROM data"))
            total_records = result.fetchone().total
            
            result = conn.execute(text("""
                SELECT s.name, COUNT(d.id) as count, 
                       AVG(d.value) as avg_value,
                       MIN(d.value) as min_value,
                       MAX(d.value) as max_value
                FROM data d
                JOIN signal s ON d.signal_id = s.id
                GROUP BY s.id, s.name
                ORDER BY s.id
            """))
            signal_stats = [dict(row._mapping) for row in result]
            
            result = conn.execute(text("SELECT MIN(timestamp), MAX(timestamp) FROM data"))
            min_date, max_date = result.fetchone()
            
            result = conn.execute(text("""
                SELECT DATE(timestamp) as date, COUNT(*) as count
                FROM data
                GROUP BY DATE(timestamp)
                ORDER BY date
            """))
            daily_distribution = [dict(row._mapping) for row in result]
            
            summary = {
                "total_records": total_records,
                "date_range": {
                    "min": min_date.isoformat() if min_date else None,
                    "max": max_date.isoformat() if max_date else None
                },
                "signal_statistics": signal_stats,
                "daily_distribution": daily_distribution
            }
            
            context.log.info(f"Resumo do banco alvo: {total_records} registros")
            
            return Output(
                value=summary,
                metadata={
                    "total_records": MetadataValue.int(total_records),
                    "date_range": MetadataValue.text(f"{min_date} a {max_date}" if min_date and max_date else "N/A"),
                    "signals_count": MetadataValue.int(len(signal_stats)),
                    "days_with_data": MetadataValue.int(len(daily_distribution))
                }
            )
            
    except Exception as e:
        context.log.error(f"Erro ao consultar banco alvo: {e}")
        raise

@asset(
    description="Estrutura completa das tabelas e dados do banco alvo",
    required_resource_keys={"alvo_db"}
)
def alvo_database_structure(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    alvo_db = context.resources.alvo_db
    
    try:
        with alvo_db.get_engine().connect() as conn:
            context.log.info("Consultando estrutura da tabela signal...")
            
            result = conn.execute(text("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns 
                WHERE table_name = 'signal' 
                ORDER BY ordinal_position
            """))
            signal_structure = [dict(row._mapping) for row in result]
            
            result = conn.execute(text("SELECT * FROM signal ORDER BY id"))
            signal_data = [dict(row._mapping) for row in result]
            
            context.log.info("Consultando estrutura da tabela data...")
            
            result = conn.execute(text("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns 
                WHERE table_name = 'data' 
                ORDER BY ordinal_position
            """))
            data_structure = [dict(row._mapping) for row in result]
            
            context.log.info("Consultando relacionamentos...")
            
            result = conn.execute(text("""
                SELECT 
                    tc.constraint_name,
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_name = 'data'
            """))
            foreign_keys = [dict(row._mapping) for row in result]
            
            context.log.info("Consultando amostra de dados...")
            
            result = conn.execute(text("""
                SELECT 
                    d.id,
                    d.timestamp,
                    d.signal_id,
                    d.value,
                    s.name as signal_name
                FROM data d
                JOIN signal s ON d.signal_id = s.id
                ORDER BY d.timestamp DESC
                LIMIT 10
            """))
            data_sample = [dict(row._mapping) for row in result]
            
            context.log.info("Calculando estatísticas...")
            
            result = conn.execute(text("""
                SELECT 
                    s.id,
                    s.name,
                    COUNT(d.id) as total_records,
                    AVG(d.value) as avg_value,
                    MIN(d.value) as min_value,
                    MAX(d.value) as max_value,
                    STDDEV(d.value) as std_value
                FROM signal s
                LEFT JOIN data d ON s.id = d.signal_id
                GROUP BY s.id, s.name
                ORDER BY s.id
            """))
            signal_statistics = [dict(row._mapping) for row in result]
            
            structure_summary = {
                "database_info": {
                    "name": "alvo_db",
                    "description": "Banco de dados alvo com dados processados de sensores"
                },
                "tables": {
                    "signal": {
                        "description": "Tabela de definição dos sinais/indicadores",
                        "structure": signal_structure,
                        "data": signal_data,
                        "total_records": len(signal_data)
                    },
                    "data": {
                        "description": "Tabela de dados processados dos sensores",
                        "structure": data_structure,
                        "sample_data": data_sample,
                        "total_records": sum(stat['total_records'] for stat in signal_statistics)
                    }
                },
                "relationships": {
                    "foreign_keys": foreign_keys,
                    "description": "data.signal_id -> signal.id"
                },
                "statistics": {
                    "signals": signal_statistics,
                    "data_distribution": {
                        "total_records": sum(stat['total_records'] for stat in signal_statistics),
                        "signals_count": len(signal_statistics),
                        "data_range": {
                            "min_timestamp": min((row['timestamp'] for row in data_sample), default=None),
                            "max_timestamp": max((row['timestamp'] for row in data_sample), default=None)
                        }
                    }
                }
            }
            
            context.log.info(f"Estrutura do banco alvo consultada com sucesso")
            
            return Output(
                value=structure_summary,
                metadata={
                    "tables_count": MetadataValue.int(2),
                    "signals_count": MetadataValue.int(len(signal_data)),
                    "total_data_records": MetadataValue.int(sum(stat['total_records'] for stat in signal_statistics)),
                    "foreign_keys_count": MetadataValue.int(len(foreign_keys)),
                    "sample_records": MetadataValue.int(len(data_sample))
                }
            )
            
    except Exception as e:
        context.log.error(f"Erro ao consultar estrutura do banco alvo: {e}")
        raise
