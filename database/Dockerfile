FROM python:3.11-slim

WORKDIR /app

# Copiar requirements e instalar dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar script de população
COPY database/seed_fonte_docker.py ./

# Comando para executar a população de dados
CMD ["python", "seed_fonte_docker.py"]
