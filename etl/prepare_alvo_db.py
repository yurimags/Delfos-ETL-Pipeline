#!/usr/bin/env python3

import os
import sys
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

DATABASE_URL = os.getenv('ALVO_DATABASE_URL')

if not DATABASE_URL:
    raise ValueError("ALVO_DATABASE_URL não está definida")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class Signal(Base):
    __tablename__ = "signal"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    
    data_records = relationship("Data", back_populates="signal")

class Data(Base):
    __tablename__ = "data"
    
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    signal_id = Column(Integer, ForeignKey("signal.id"), nullable=False, index=True)
    value = Column(Float, nullable=False)
    
    signal = relationship("Signal", back_populates="data_records")

def create_tables():
    try:
        Base.metadata.create_all(bind=engine)
        print("Tabelas criadas com sucesso!")
    except Exception as e:
        print(f"Erro ao criar tabelas: {e}")
        sys.exit(1)

def insert_initial_signals():
    db = SessionLocal()
    
    try:
        existing_signals = db.query(Signal).count()
        if existing_signals > 0:
            print(f"Já existem {existing_signals} sinais na tabela. Pulando inserção inicial.")
            return
        
        signals_data = [
            {"name": "wind_speed_mean", "description": "Média da velocidade do vento em intervalos de 10 minutos"},
            {"name": "wind_speed_min", "description": "Mínimo da velocidade do vento em intervalos de 10 minutos"},
            {"name": "wind_speed_max", "description": "Máximo da velocidade do vento em intervalos de 10 minutos"},
            {"name": "wind_speed_std", "description": "Desvio padrão da velocidade do vento em intervalos de 10 minutos"},
            
            {"name": "power_mean", "description": "Média da potência em intervalos de 10 minutos"},
            {"name": "power_min", "description": "Mínimo da potência em intervalos de 10 minutos"},
            {"name": "power_max", "description": "Máximo da potência em intervalos de 10 minutos"},
            {"name": "power_std", "description": "Desvio padrão da potência em intervalos de 10 minutos"},
        ]
        
        for signal_data in signals_data:
            signal = Signal(**signal_data)
            db.add(signal)
        
        db.commit()
        print(f"Inseridos {len(signals_data)} sinais na tabela signal")
        
        signals = db.query(Signal).all()
        print("\nSinais disponíveis:")
        for signal in signals:
            print(f"  ID: {signal.id}, Nome: {signal.name}")
            
    except Exception as e:
        print(f"Erro ao inserir sinais: {e}")
        db.rollback()
        sys.exit(1)
    finally:
        db.close()

def verify_database():
    db = SessionLocal()
    
    try:
        signals_count = db.query(Signal).count()
        data_count = db.query(Data).count()
        
        print(f"\nVerificação do banco de dados:")
        print(f"  Sinais na tabela signal: {signals_count}")
        print(f"  Registros na tabela data: {data_count}")
        
        if signals_count > 0:
            print("\nSinais configurados:")
            signals = db.query(Signal).all()
            for signal in signals:
                print(f"  - {signal.name}: {signal.description}")
        
    except Exception as e:
        print(f"Erro ao verificar banco: {e}")
    finally:
        db.close()

def main():
    print("Preparando banco de dados Alvo...")
    
    create_tables()
    
    insert_initial_signals()
    
    verify_database()
    
    print("\nBanco de dados Alvo preparado com sucesso!")

if __name__ == "__main__":
    main()
