#!/usr/bin/env python3

import os
import sys
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

def wait_for_database():
    max_attempts = 30
    attempt = 0
    
    while attempt < max_attempts:
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'db_fonte'),
                port=os.getenv('DB_PORT', '5432'),
                database=os.getenv('DB_NAME', 'fonte_db'),
                user=os.getenv('DB_USER', 'fonte_user'),
                password=os.getenv('DB_PASSWORD', 'fonte_password')
            )
            conn.close()
            print("Banco de dados disponível!")
            return True
        except psycopg2.OperationalError:
            attempt += 1
            print(f"Aguardando banco de dados... tentativa {attempt}/{max_attempts}")
            time.sleep(2)
    
    print("Erro: Não foi possível conectar ao banco de dados")
    return False

def generate_sample_data():
    start_date = datetime(2025, 8, 10, 0, 0, 0)
    end_date = datetime(2025, 8, 20, 23, 59, 59)
    
    timestamps = pd.date_range(start=start_date, end=end_date, freq='1min')
    
    np.random.seed(42)
    
    wind_speed = np.random.normal(12, 5, len(timestamps))
    wind_speed = np.clip(wind_speed, 0, 25)
    
    power = np.where(wind_speed < 3, 0, 
                    np.where(wind_speed > 20, 2000,
                            wind_speed ** 2 * 8 + np.random.normal(0, 100, len(timestamps))))
    power = np.clip(power, 0, 2000)
    
    base_temp = 20 + 10 * np.sin(2 * np.pi * np.arange(len(timestamps)) / (24 * 60))
    ambient_temperature = base_temp + np.random.normal(0, 3, len(timestamps))
    
    df = pd.DataFrame({
        'timestamp': timestamps,
        'wind_speed': wind_speed,
        'power': power,
        'ambient_temprature': ambient_temperature
    })
    
    return df

def insert_data_to_database(df):
    db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print(f"Conectado ao banco de dados: {db_config['database']}")
        
        cursor.execute("SELECT COUNT(*) FROM data")
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0:
            print(f"Banco já possui {existing_count} registros. Pulando inserção.")
            return
        
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                row['timestamp'],
                row['wind_speed'],
                row['power'],
                row['ambient_temprature']
            ))
        
        batch_size = 1000
        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i + batch_size]
            
            cursor.executemany(
                """
                INSERT INTO data (timestamp, wind_speed, power, ambient_temprature)
                VALUES (%s, %s, %s, %s)
                """,
                batch
            )
            
            print(f"Inseridos {len(batch)} registros...")
        
        conn.commit()
        print(f"Total de {len(data_to_insert)} registros inseridos com sucesso!")
        
        cursor.execute("SELECT COUNT(*) FROM data")
        count = cursor.fetchone()[0]
        print(f"Total de registros na tabela: {count}")
        
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM data")
        min_date, max_date = cursor.fetchone()
        print(f"Período dos dados: {min_date} a {max_date}")
        
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    print("Iniciando população do banco de dados Fonte...")
    
    if not wait_for_database():
        sys.exit(1)
    
    df = generate_sample_data()
    print(f"Dados gerados: {len(df)} registros")
    print(f"Período: {df['timestamp'].min()} a {df['timestamp'].max()}")
    
    insert_data_to_database(df)
    
    print("Processo concluído com sucesso!")

if __name__ == "__main__":
    main()
