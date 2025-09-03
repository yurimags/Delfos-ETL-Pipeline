# Delfos ETL Pipeline

Pipeline de ETL para processamento de dados de sensores.

## Execução

### Pré-requisitos
- Docker
- Docker Compose
- Criar arquivo `.env` copiando os valores de `env.example`:
  ```bash
  cp env.example .env
  ```

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

## Como usar o Dagster

### Acessando o Dashboard
1. Acesse http://localhost:3000
2. O Dagster carregará automaticamente os assets e jobs do projeto

### Executando o Pipeline ETL
1. **Assets:** Visualize todos os assets disponíveis na aba "Assets"
2. **Jobs:** Execute o pipeline completo na aba "Jobs"
3. **Materialização:** Clique em "Materialize" para executar o ETL

### Monitoramento
- **Logs:** Acompanhe a execução em tempo real
- **Status:** Verifique o histórico de execuções
- **Métricas:** Monitore o desempenho dos assets

## Exportação de Dados (Funcionalidade Adicional)

### Scripts de Exportação
O projeto inclui scripts adicionais para exportar dados dos bancos:

```bash
# Exportar dados do banco fonte
python export_fonte_db.py

# Exportar dados do banco alvo
python export_alvo_db.py
```

### Formato de Saída
- Os dados são exportados em formato **Excel (.xlsx)**
- Arquivos são salvos na pasta `exports/`
- Nomenclatura: `[banco]_data_[timestamp].xlsx`

### Uso dos Scripts
1. **Certifique-se que os serviços estão rodando**
2. **Execute o script desejado** dentro do container ou localmente
3. **Verifique a pasta exports** para os arquivos gerados

> **Nota:** Esta é uma funcionalidade adicional ao pipeline ETL principal, útil para análise e backup dos dados.