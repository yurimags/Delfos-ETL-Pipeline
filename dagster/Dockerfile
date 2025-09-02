FROM python:3.11-slim

WORKDIR /app

# Copiar requirements e instalar dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Criar diretório DAGSTER_HOME
RUN mkdir -p /app/dagster_home

# Copiar arquivo de ambiente
COPY .env .env

# Copiar código do Dagster (copiar o diretório dagster para dentro de /app)
COPY dagster/ ./

# Expor porta
EXPOSE 3000

# Comando para executar o Dagster apontando para o arquivo jobs.py
CMD ["dagster", "dev", "--host", "0.0.0.0", "--port", "3000", "--python-file", "jobs.py"]
