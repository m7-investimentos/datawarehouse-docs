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
    echo "1) Executar ETL-001 (Indicators)"
    echo "2) Executar ETL-002 (Assignments)"
    echo "3) Executar ETL-003 (Targets)"
    echo "4) Executar todos os ETLs"
    echo "5) Executar pipeline completo (ETLs + Procedures)"
    echo "6) Verificar conexão com banco"
    echo "7) Verificar dados no Bronze"
    echo "8) Instalar dependências"
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
            echo -e "\n${YELLOW}Executando todos os ETLs...${NC}"
            $PYTHON run_all_etls.py
            ;;
        5)
            echo -e "\n${YELLOW}Executando pipeline completo...${NC}"
            $PYTHON run_full_pipeline.py
            ;;
        6)
            echo -e "\n${YELLOW}Verificando conexão com banco...${NC}"
            $PYTHON tests/test_connection.py
            ;;
        7)
            echo -e "\n${YELLOW}Verificando dados no Bronze...${NC}"
            $PYTHON tests/verify_data.py
            ;;
        8)
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