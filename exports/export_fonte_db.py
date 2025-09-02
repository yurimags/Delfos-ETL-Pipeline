#!/usr/bin/env python3

import psycopg2
import pandas as pd
from datetime import datetime
import os

def connect_to_fonte_db():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco fonte: {e}")
        return None

def export_fonte_data():
    print("CONECTANDO AO BANCO FONTE...")
    
    conn = connect_to_fonte_db()
    if not conn:
        return
    
    try:
        export_dir = "exports"
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
        
        print("EXPORTANDO DADOS DA TABELA 'data'...")
        
        query = """
            SELECT 
                timestamp,
                wind_speed,
                power,
                ambient_temprature
            FROM data 
            ORDER BY timestamp
        """
        
        df = pd.read_sql_query(query, conn)
        
        print(f"Total de registros encontrados: {len(df):,}")
        print(f"Período: {df['timestamp'].min()} a {df['timestamp'].max()}")
        
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{export_dir}/fonte_db_data_{timestamp_str}.xlsx"
        
        df.to_excel(filename, sheet_name='Dados_Completos', index=False)
        
        print(f"Dados exportados com sucesso para: {filename}")
        print(f"Arquivo salvo em: {os.path.abspath(filename)}")
        
        return filename
        
    except Exception as e:
        print(f"Erro durante exportação: {e}")
        return None
    
    finally:
        conn.close()

def main():
    print("EXPORTAÇÃO DO BANCO DE DADOS FONTE")
    print("=" * 50)
    
    filename = export_fonte_data()
    
    if filename:
        print("\n" + "=" * 50)
        print("EXPORTAÇÃO CONCLUÍDA COM SUCESSO!")
        print("=" * 50)
        print(f"Arquivo: {filename}")
        print(f"Abra o arquivo Excel para visualizar os dados")
    else:
        print("\n" + "=" * 50)
        print("EXPORTAÇÃO FALHOU!")
        print("=" * 50)

if __name__ == "__main__":
    main()
