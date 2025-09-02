#!/bin/bash

echo "=== Pipeline de ETL Delfos ==="
echo "Iniciando todos os serviços..."

# Verificar se Docker está instalado
if ! command -v docker &> /dev/null; then
    echo "Erro: Docker não está instalado"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "Erro: Docker Compose não está instalado"
    exit 1
fi

# Parar containers existentes
echo "Parando containers existentes..."
docker-compose down

# Construir e iniciar todos os serviços
echo "Construindo e iniciando serviços..."
docker-compose up --build -d

# Aguardar serviços estarem prontos
echo "Aguardando serviços estarem prontos..."
sleep 30

# Verificar status dos serviços
echo "Verificando status dos serviços..."
docker-compose ps

echo ""
echo "=== Serviços Disponíveis ==="
echo "API: http://localhost:8000"
echo "API Docs: http://localhost:8000/docs"
echo "Dagster UI: http://localhost:3000"
echo ""
echo "=== Comandos Úteis ==="
echo "Ver logs: docker-compose logs -f [servico]"
echo "Parar: docker-compose down"
echo "Reiniciar: docker-compose restart"
echo ""
echo "=== Teste Rápido ==="
echo "Testando API..."
curl -s http://localhost:8000/health | jq . 2>/dev/null || curl -s http://localhost:8000/health

echo ""
echo "Setup concluído! Acesse os serviços nos links acima."
