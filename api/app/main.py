from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from datetime import datetime
import logging

from .database import db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Delfos ETL API",
    description="API para expor dados do banco de dados Fonte",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "message": "Delfos ETL API - Pipeline de Dados de Sensores",
        "version": "1.0.0",
        "description": "API para expor dados do banco de dados Fonte com sensores de energia eólica",
        "data_period": "10/08/2025 a 20/08/2025 (11 dias)",
        "endpoints": {
            "data": "/data/ - Consulta dados com filtros",
            "health": "/health - Status da API e banco",
            "info": "/info - Informações sobre dados disponíveis",
            "docs": "/docs - Documentação interativa"
        },
        "example_usage": {
            "query_data": "/data/?start_date=2025-08-10T00:00:00&end_date=2025-08-11T00:00:00&variables=wind_speed,power",
            "check_health": "/health",
            "get_info": "/info"
        }
    }

@app.get("/health")
async def health_check():
    try:
        count = db.get_data_count()
        return {
            "status": "healthy",
            "database_connection": "ok",
            "total_records": count,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")

@app.get("/info")
async def get_info():
    try:
        count = db.get_data_count()
        date_range = db.get_data_range()
        
        return {
            "total_records": count,
            "date_range": date_range,
            "available_columns": ["timestamp", "wind_speed", "power", "ambient_temprature"],
            "description": "Dados de sensores com timestamp, velocidade do vento, potência e temperatura ambiente",
            "data_period": "10/08/2025 a 20/08/2025 (11 dias)",
            "frequency": "1 minuto",
            "estimated_records": "~15.840 registros",
            "example_query": "/data/?start_date=2025-08-10T00:00:00&end_date=2025-08-11T00:00:00&variables=wind_speed,power"
        }
    except Exception as e:
        logger.error(f"Error getting info: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving information: {str(e)}")

@app.get("/data/")
async def get_data(
    start_date: Optional[str] = Query(
        None, 
        description="Data de início no formato ISO (ex: 2025-08-10T00:00:00)",
        example="2025-08-10T00:00:00"
    ),
    end_date: Optional[str] = Query(
        None, 
        description="Data de fim no formato ISO (ex: 2025-08-11T00:00:00)",
        example="2025-08-11T00:00:00"
    ),
    variables: Optional[str] = Query(
        None,
        description="Lista de variáveis separadas por vírgula (ex: wind_speed,power)",
        example="wind_speed,power"
    )
):
    try:
        start_dt = None
        end_dt = None
        
        if start_date:
            try:
                start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(
                    status_code=400, 
                    detail="Formato de data inválido para start_date. Use formato ISO (ex: 2025-08-10T00:00:00)"
                )
        
        if end_date:
            try:
                end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(
                    status_code=400, 
                    detail="Formato de data inválido para end_date. Use formato ISO (ex: 2025-08-11T00:00:00)"
                )
        
        variable_list = None
        if variables:
            variable_list = [v.strip() for v in variables.split(',')]
            
            valid_variables = ['timestamp', 'wind_speed', 'power', 'ambient_temprature']
            invalid_vars = [v for v in variable_list if v not in valid_variables]
            
            if invalid_vars:
                raise HTTPException(
                    status_code=400,
                    detail=f"Variáveis inválidas: {invalid_vars}. Variáveis válidas: {valid_variables}"
                )
        
        data = db.get_data_with_filters(
            start_date=start_dt,
            end_date=end_dt,
            variables=variable_list
        )
        
        for record in data:
            if 'timestamp' in record and record['timestamp']:
                record['timestamp'] = record['timestamp'].isoformat()
        
        return {
            "data": data,
            "count": len(data),
            "filters": {
                "start_date": start_date,
                "end_date": end_date,
                "variables": variables
            },
            "metadata": {
                "query_executed_at": datetime.now().isoformat(),
                "data_period_available": "10/08/2025 a 20/08/2025",
                "total_records_available": db.get_data_count(),
                "frequency": "1 minuto",
                "columns_returned": variable_list if variable_list else ["timestamp", "wind_speed", "power", "ambient_temprature"]
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error querying data: {e}")
        raise HTTPException(status_code=500, detail=f"Error querying data: {str(e)}")

@app.get("/data/count")
async def get_data_count():
    try:
        count = db.get_data_count()
        return {"total_records": count}
    except Exception as e:
        logger.error(f"Error getting count: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting count: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
