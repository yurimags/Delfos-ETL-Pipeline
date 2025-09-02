#!/usr/bin/env python3

import psycopg2
import pandas as pd
from datetime import datetime
import os

def connect_to_alvo_db():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_ALVO_HOST'),
            port=os.getenv('DB_ALVO_PORT'),
            database=os.getenv('DB_ALVO_NAME'),
            user=os.getenv('DB_ALVO_USER'),
            password=os.getenv('DB_ALVO_PASSWORD')
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco alvo: {e}")
        return None

def export_alvo_data():
    print("CONECTANDO AO BANCO ALVO...")
    
    conn = connect_to_alvo_db()
    if not conn:
        return
    
    try:
        export_dir = "exports"
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
        
        print("EXPORTANDO DADOS DAS TABELAS...")
        
        print("  Exportando tabela 'signal'...")
        signal_query = "SELECT * FROM signal ORDER BY id"
        signal_df = pd.read_sql_query(signal_query, conn)
        
        print("  Exportando tabela 'data'...")
        data_query = """
            SELECT 
                d.id,
                d.timestamp,
                d.signal_id,
                d.value,
                s.name as signal_name,
                s.description as signal_description
            FROM data d
            JOIN signal s ON d.signal_id = s.id
            ORDER BY d.timestamp DESC, d.signal_id
        """
        data_df = pd.read_sql_query(data_query, conn)
        
        print(f"Total de sinais: {len(signal_df)}")
        print(f"Total de registros de dados: {len(data_df):,}")
        print(f"Período dos dados: {data_df['timestamp'].min()} a {data_df['timestamp'].max()}")
        
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{export_dir}/alvo_db_data_{timestamp_str}.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            signal_df.to_excel(writer, sheet_name='Tabela_Signal', index=False)
            
            data_df.to_excel(writer, sheet_name='Tabela_Data', index=False)
        
        print(f"Dados exportados com sucesso para: {filename}")
        print(f"Arquivo salvo em: {os.path.abspath(filename)}")
        
        print("\nABAS CRIADAS NO EXCEL:")
        print("  Tabela_Signal: Definição dos sinais")
        print("  Tabela_Data: Todos os dados processados")
        
        return filename
        
    except Exception as e:
        print(f"Erro durante exportação: {e}")
        return None
    
    finally:
        conn.close()

def main():
    print("EXPORTAÇÃO DO BANCO DE DADOS ALVO")
    print("=" * 50)
    
    filename = export_alvo_data()
    
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
