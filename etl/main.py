#!/usr/bin/env python3

import os
import sys
from datetime import datetime
from etl_process import ETLProcessor
from prepare_alvo_db import main as prepare_db

def main():
    print("=== Pipeline de ETL Delfos ===")
    
    if len(sys.argv) < 2:
        print("Uso: python main.py YYYY-MM-DD")
        print("Exemplo: python main.py 2024-01-01")
        sys.exit(1)
    
    try:
        target_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')
        print(f"Processando dados para: {target_date.date()}")
        
        print("\n1. Preparando banco de dados alvo...")
        prepare_db()
        
        print("\n2. Iniciando processamento ETL...")
        processor = ETLProcessor()
        
        result = processor.process_date(target_date)
        
        print(f"\n=== Resultado do ETL ===")
        print(f"Data: {result['date']}")
        print(f"Status: {result['status']}")
        
        if result['status'] == 'success':
            print(f"Registros processados: {result['records_processed']}")
            print(f"Registros inseridos: {result['records_inserted']}")
            print(f"Intervalos processados: {result['intervals_processed']}")
            print("\nETL executado com sucesso!")
        elif result['status'] == 'no_data':
            print("Nenhum dado encontrado para a data especificada")
        elif result['status'] == 'error':
            print(f"Erro: {result['error']}")
            sys.exit(1)
        
    except ValueError:
        print("Erro: Formato de data inválido. Use YYYY-MM-DD")
        sys.exit(1)
    except Exception as e:
        print(f"Erro inesperado: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
