FROM python:3.11-slim

WORKDIR /app

# Copiar requirements e instalar dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código do ETL
COPY etl/ ./

# Tornar scripts executáveis
RUN chmod +x main.py

# Comando padrão (pode ser sobrescrito)
CMD ["python", "main.py", "2025-08-10"]
