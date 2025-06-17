#!/bin/bash
# ==============================================================================
# Script de Execução de ETLs - Performance Indicators & Assignments
# ==============================================================================
# Autor: bruno.chiaramonti@multisete.com
# Data: 2025-01-17
# ==============================================================================

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Diretório base
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Função para exibir o menu
show_menu() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}ETL Performance - Menu de Execução${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo -e "${GREEN}Execução Individual:${NC}"
    echo "1) Executar ETL-001 (Indicators)"
    echo "2) Executar ETL-002 (Assignments)"
    echo "3) Executar ETL-003 (Targets)"
    echo -e "${GREEN}Pipelines Completos (ETL + Procedure):${NC}"
    echo "4) Pipeline 001 (Indicators: ETL + Procedure)"
    echo "5) Pipeline 002 (Assignments: ETL + Procedure)"
    echo "6) Pipeline 003 (Targets: ETL + Procedure)"
    echo -e "${GREEN}Execução em Lote:${NC}"
    echo "7) Executar todos os ETLs"
    echo "8) Executar todos os pipelines (ETLs + Procedures)"
    echo -e "${GREEN}Utilitários:${NC}"
    echo "9) Verificar conexão com banco"
    echo "10) Verificar dados no Bronze"
    echo "11) Instalar dependências"
    echo "0) Sair"
    echo -e "${BLUE}========================================${NC}"
}

# Definir Python 3.11
PYTHON="/opt/homebrew/bin/python3.11"

# Função para verificar Python
check_python() {
    if [ -x "$PYTHON" ]; then
        echo -e "${GREEN}✓ Python 3.11 encontrado${NC}"
        $PYTHON --version
        return 0
    elif command -v python3.11 &> /dev/null; then
        PYTHON="python3.11"
        echo -e "${GREEN}✓ Python 3.11 encontrado${NC}"
        $PYTHON --version
        return 0
    else
        echo -e "${RED}✗ Python 3.11 não encontrado${NC}"
        echo "Por favor, instale o Python 3.11"
        return 1
    fi
}

# Função para verificar arquivo .env
check_env() {
    if [ -f "credentials/.env" ]; then
        echo -e "${GREEN}✓ Arquivo .env encontrado${NC}"
        return 0
    else
        echo -e "${RED}✗ Arquivo .env não encontrado${NC}"
        echo "Por favor, crie um arquivo credentials/.env com as configurações do banco"
        return 1
    fi
}

# Função para executar procedure Bronze to Silver
execute_procedure() {
    local proc_name=$1
    local proc_desc=$2
    
    echo -e "${YELLOW}Executando procedure: ${proc_desc}...${NC}"
    
    # Ler configurações do .env
    if [ -f "credentials/.env" ]; then
        export $(cat credentials/.env | grep -v '^#' | xargs)
    fi
    
    # Criar script SQL temporário
    cat > /tmp/exec_proc.sql << EOF
USE M7Medallion;
GO
EXEC ${proc_name};
GO
EOF
    
    # Executar usando sqlcmd
    if command -v sqlcmd &> /dev/null; then
        sqlcmd -S ${DB_SERVER:-localhost} -U ${DB_USERNAME:-sa} -P "${DB_PASSWORD}" -i /tmp/exec_proc.sql
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ Procedure executada com sucesso${NC}"
        else
            echo -e "${RED}✗ Erro ao executar procedure${NC}"
            return 1
        fi
    else
        echo -e "${RED}sqlcmd não encontrado. Instale o SQL Server Command Line Tools${NC}"
        echo "Para macOS: brew install sqlcmd"
        echo -e "${YELLOW}Alternativa: Execute a procedure manualmente no SQL Server Management Studio${NC}"
        echo -e "${BLUE}EXEC ${proc_name};${NC}"
        return 1
    fi
    
    rm -f /tmp/exec_proc.sql
}

# Execução principal
clear
echo -e "${BLUE}Verificando ambiente...${NC}"
check_python || exit 1
check_env || exit 1

while true; do
    echo
    show_menu
    read -p "Escolha uma opção: " choice
    
    case $choice in
        1)
            echo -e "\n${YELLOW}Executando ETL-001 (Indicators)...${NC}"
            $PYTHON etl_001_indicators.py
            ;;
        2)
            echo -e "\n${YELLOW}Executando ETL-002 (Assignments)...${NC}"
            $PYTHON etl_002_assignments.py
            ;;
        3)
            echo -e "\n${YELLOW}Executando ETL-003 (Targets)...${NC}"
            $PYTHON etl_003_targets.py
            ;;
        4)
            echo -e "\n${YELLOW}Pipeline 001 - Indicators (ETL + Procedure)${NC}"
            $PYTHON run_pipeline.py 001
            ;;
        5)
            echo -e "\n${YELLOW}Pipeline 002 - Assignments (ETL + Procedure)${NC}"
            $PYTHON run_pipeline.py 002
            ;;
        6)
            echo -e "\n${YELLOW}Pipeline 003 - Targets (ETL + Procedure)${NC}"
            $PYTHON run_pipeline.py 003
            ;;
        7)
            echo -e "\n${YELLOW}Executando todos os ETLs...${NC}"
            $PYTHON run_all_etls.py
            ;;
        8)
            echo -e "\n${YELLOW}Executando todos os pipelines (ETLs + Procedures)...${NC}"
            $PYTHON run_full_pipeline.py
            ;;
        9)
            echo -e "\n${YELLOW}Verificando conexão com banco...${NC}"
            $PYTHON tests/test_connection.py
            ;;
        10)
            echo -e "\n${YELLOW}Verificando dados no Bronze...${NC}"
            $PYTHON tests/verify_data.py
            ;;
        11)
            echo -e "\n${YELLOW}Instalando dependências...${NC}"
            $PYTHON -m pip install -r requirements.txt
            ;;
        0)
            echo -e "\n${GREEN}Saindo...${NC}"
            exit 0
            ;;
        *)
            echo -e "\n${RED}Opção inválida!${NC}"
            ;;
    esac
    
    echo -e "\n${YELLOW}Pressione ENTER para continuar...${NC}"
    read
    clear
done