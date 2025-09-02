import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
from datetime import datetime

class DatabaseConnection:
    
    def __init__(self):
        self.connection_string = os.getenv('DATABASE_URL')
        if not self.connection_string:
            db_user = os.getenv('DB_USER')
            db_password = os.getenv('DB_PASSWORD')
            db_host = os.getenv('DB_HOST')
            db_port = os.getenv('DB_PORT')
            db_name = os.getenv('DB_NAME')
            
            if not all([db_user, db_password, db_host, db_port, db_name]):
                raise ValueError("Configurações de banco de dados incompletas")
            
            self.connection_string = (
                f"postgresql://{db_user}:"
                f"{db_password}@{db_host}:"
                f"{db_port}/{db_name}"
            )
    
    def get_connection(self):
        return psycopg2.connect(self.connection_string)
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query, params)
                    results = cursor.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            print(f"Erro ao executar query: {e}")
            raise
    
    def get_data_with_filters(
        self, 
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        variables: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        if not variables:
            variables = ['timestamp', 'wind_speed', 'power', 'ambient_temprature']
        
        columns = ', '.join(variables)
        query = f"SELECT {columns} FROM data WHERE 1=1"
        params = []
        
        if start_date:
            query += " AND timestamp >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND timestamp <= %s"
            params.append(end_date)
        
        query += " ORDER BY timestamp"
        
        return self.execute_query(query, tuple(params) if params else None)
    
    def get_data_count(self) -> int:
        query = "SELECT COUNT(*) as count FROM data"
        result = self.execute_query(query)
        return result[0]['count'] if result else 0
    
    def get_data_range(self) -> Dict[str, datetime]:
        query = "SELECT MIN(timestamp) as min_date, MAX(timestamp) as max_date FROM data"
        result = self.execute_query(query)
        return result[0] if result else {}

db = DatabaseConnection()
