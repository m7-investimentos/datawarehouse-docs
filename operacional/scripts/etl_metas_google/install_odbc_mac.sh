#!/bin/bash

echo "========================================="
echo "Instalação do ODBC Driver para SQL Server no macOS"
echo "========================================="

# Verificar se Homebrew está instalado
if ! command -v brew &> /dev/null; then
    echo "❌ Homebrew não encontrado. Instalando..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
else
    echo "✓ Homebrew encontrado"
fi

# Instalar unixODBC
echo ""
echo "Instalando unixODBC..."
brew install unixodbc

# Adicionar tap da Microsoft
echo ""
echo "Adicionando repositório Microsoft..."
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release

# Aceitar EULA e instalar driver
echo ""
echo "Instalando ODBC Driver 17 for SQL Server..."
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql17

# Instalar ferramentas adicionais (opcional)
echo ""
echo "Instalando ferramentas SQL (opcional)..."
HOMEBREW_ACCEPT_EULA=Y brew install mssql-tools

# Verificar instalação
echo ""
echo "Verificando instalação..."

# Listar drivers ODBC instalados
echo ""
echo "Drivers ODBC instalados:"
odbcinst -q -d

# Verificar se o driver foi instalado corretamente
if odbcinst -q -d | grep -q "ODBC Driver 17 for SQL Server"; then
    echo ""
    echo "✅ ODBC Driver 17 for SQL Server instalado com sucesso!"
    
    # Mostrar localização do driver
    echo ""
    echo "Localização do driver:"
    odbcinst -j
    
    # Testar conexão pyodbc
    echo ""
    echo "Testando importação pyodbc..."
    python3 -c "import pyodbc; print('✓ pyodbc importado com sucesso'); print('Drivers disponíveis:', pyodbc.drivers())"
else
    echo ""
    echo "❌ Erro na instalação do driver ODBC"
    echo "Tente executar manualmente:"
    echo "  brew install msodbcsql17"
fi

echo ""
echo "========================================="
echo "Instalação concluída!"
echo ""
echo "Para usar o driver no Python, use uma das opções:"
echo "  driver = 'ODBC Driver 17 for SQL Server'"
echo "  driver = '/usr/local/lib/libmsodbcsql.17.dylib'"
echo "========================================="