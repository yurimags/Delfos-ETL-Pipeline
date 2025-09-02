@echo off
echo === Pipeline de ETL Delfos ===
echo Iniciando todos os servicos...

REM Verificar se Docker está instalado
docker --version >nul 2>&1
if errorlevel 1 (
    echo Erro: Docker nao esta instalado
    pause
    exit /b 1
)

docker-compose --version >nul 2>&1
if errorlevel 1 (
    echo Erro: Docker Compose nao esta instalado
    pause
    exit /b 1
)

REM Parar containers existentes
echo Parando containers existentes...
docker-compose down

REM Construir e iniciar todos os serviços
echo Construindo e iniciando servicos...
docker-compose up --build -d

REM Aguardar serviços estarem prontos
echo Aguardando servicos estarem prontos...
timeout /t 30 /nobreak >nul

REM Verificar status dos serviços
echo Verificando status dos servicos...
docker-compose ps

echo.
echo === Servicos Disponiveis ===
echo API: http://localhost:8000
echo API Docs: http://localhost:8000/docs
echo Dagster UI: http://localhost:3000
echo.
echo === Comandos Uteis ===
echo Ver logs: docker-compose logs -f [servico]
echo Parar: docker-compose down
echo Reiniciar: docker-compose restart
echo.
echo === Teste Rapido ===
echo Testando API...
curl -s http://localhost:8000/health

echo.
echo Setup concluido! Acesse os servicos nos links acima.
pause
