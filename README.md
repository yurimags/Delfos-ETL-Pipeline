# Delfos ETL Pipeline

Pipeline de ETL para processamento de dados de sensores.

## Execução

### Pré-requisitos
- Docker
- Docker Compose

### Comandos

```bash
# Iniciar todos os serviços
./start.sh          # Linux/Mac
start.bat           # Windows

# Ou manualmente
docker-compose up --build

# Parar serviços
docker-compose down

# Ver logs
docker-compose logs [serviço]

# Status dos containers
docker-compose ps
```

## Acesso

- **API:** http://localhost:8000
- **Dagster:** http://localhost:3000
- **Docs API:** http://localhost:8000/docs
