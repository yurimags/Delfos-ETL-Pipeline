import os
from dotenv import load_dotenv

load_dotenv()

from dagster import (
    define_asset_job,
    schedule,
    ScheduleDefinition,
    Definitions,
    load_assets_from_modules
)
from assets import processed_sensor_data, alvo_database_summary, alvo_database_structure
from resources import (
    FonteDatabaseResource,
    AlvoDatabaseResource,
    APIResource,
    DatabaseConfig,
    APIConfig
)

sensor_data_job = define_asset_job(
    name="sensor_data_etl_job",
    selection=[processed_sensor_data],
    description="Job para processar dados de sensores via ETL"
)

alvo_summary_job = define_asset_job(
    name="alvo_database_summary_job",
    selection=[alvo_database_summary],
    description="Job para visualizar resumo dos dados no banco alvo"
)

alvo_structure_job = define_asset_job(
    name="alvo_database_structure_job",
    selection=[alvo_database_structure],
    description="Job para visualizar estrutura completa das tabelas e dados"
)

daily_sensor_etl_schedule = ScheduleDefinition(
    job=sensor_data_job,
    cron_schedule="0 1 * * *",
    name="daily_sensor_etl_schedule",
    description="Executa o ETL de dados de sensores diariamente às 1h da manhã"
)

historical_sensor_etl_schedule = ScheduleDefinition(
    job=sensor_data_job,
    cron_schedule="0 * * * *",
    name="historical_sensor_etl_schedule",
    description="Executa o ETL de dados históricos a cada hora"
)

defs = Definitions(
    assets=[processed_sensor_data, alvo_database_summary, alvo_database_structure],
    jobs=[sensor_data_job, alvo_summary_job, alvo_structure_job],
    schedules=[daily_sensor_etl_schedule, historical_sensor_etl_schedule],
    resources={
        "fonte_db": FonteDatabaseResource(
            config=DatabaseConfig(
                host=os.getenv('DB_HOST'),
                port=int(os.getenv('DB_PORT')),
                database=os.getenv('DB_NAME'),
                username=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
        ),
        "alvo_db": AlvoDatabaseResource(
            config=DatabaseConfig(
                host=os.getenv('DB_ALVO_HOST'),
                port=int(os.getenv('DB_ALVO_PORT')),
                database=os.getenv('DB_ALVO_NAME'),
                username=os.getenv('DB_ALVO_USER'),
                password=os.getenv('DB_ALVO_PASSWORD')
            )
        ),
        "api": APIResource(
            config=APIConfig(
                base_url=os.getenv('API_URL'),
                timeout=30
            )
        )
    }
)
