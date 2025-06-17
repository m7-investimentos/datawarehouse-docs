#!/usr/bin/env python3
"""
Script para testar conexão com SQL Server e diagnosticar problemas
"""

import os
import sys
from dotenv import load_dotenv
from pathlib import Path

# Diretórios
BASE_DIR = Path(__file__).resolve().parent.parent  # Go up one level from tests directory
CREDENTIALS_DIR = BASE_DIR / 'credentials'

# Carregar variáveis de ambiente do arquivo .env no diretório credentials
load_dotenv(CREDENTIALS_DIR / '.env')

print("="*60)
print("TESTE DE CONEXÃO SQL SERVER")
print("="*60)

# 1. Verificar pyodbc
try:
    import pyodbc
    print("✓ pyodbc instalado com sucesso")
    print(f"  Versão: {pyodbc.version}")
except ImportError:
    print("❌ pyodbc não está instalado")
    print("  Execute: pip install pyodbc")
    sys.exit(1)

# 2. Listar drivers disponíveis
print("\nDrivers ODBC disponíveis:")
drivers = pyodbc.drivers()
if drivers:
    for driver in drivers:
        print(f"  - {driver}")
else:
    print("  ❌ Nenhum driver ODBC encontrado")
    print("  Execute: ./install_odbc_mac.sh")
    sys.exit(1)

# 3. Verificar variáveis de ambiente
print("\nVariáveis de ambiente:")
db_server = os.getenv('DB_SERVER')
db_database = os.getenv('DB_DATABASE')
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')

print(f"  DB_SERVER: {db_server}")
print(f"  DB_DATABASE: {db_database}")
print(f"  DB_USERNAME: {db_username}")
print(f"  DB_PASSWORD: {'*' * len(db_password) if db_password else 'NÃO DEFINIDA'}")
print(f"  DB_DRIVER: {db_driver}")

if not all([db_server, db_database, db_username, db_password]):
    print("\n❌ Variáveis de ambiente incompletas")
    print("  Verifique o arquivo .env")
    sys.exit(1)

# 4. Testar diferentes formatos de conexão
print("\nTestando conexões:")

# Remover chaves extras do driver
driver_clean = db_driver.strip('{}')

# Método 1: Connection string direta
print("\n1. Tentando conexão direta com pyodbc...")
try:
    conn_str = (
        f"DRIVER={{{driver_clean}}};"
        f"SERVER={db_server};"
        f"DATABASE={db_database};"
        f"UID={db_username};"
        f"PWD={db_password};"
        f"TrustServerCertificate=yes"
    )
    print(f"   Connection string: DRIVER={{...}};SERVER={db_server};DATABASE={db_database};...")
    
    conn = pyodbc.connect(conn_str, timeout=10)
    print("   ✓ Conexão estabelecida com sucesso!")
    
    # Testar query simples
    cursor = conn.cursor()
    cursor.execute("SELECT @@VERSION")
    row = cursor.fetchone()
    print(f"   SQL Server: {row[0][:50]}...")
    
    conn.close()
    print("   ✓ Conexão fechada")
    
except Exception as e:
    print(f"   ❌ Erro: {e}")

# Método 2: SQLAlchemy
print("\n2. Tentando conexão via SQLAlchemy...")
try:
    from sqlalchemy import create_engine, text
    
    # Tentar diferentes formatos de connection string
    connection_strings = [
        # Formato 1: Com driver entre chaves
        f"mssql+pyodbc://{db_username}:{db_password}@{db_server}/{db_database}?driver={driver_clean.replace(' ', '+')}",
        # Formato 2: Com TrustServerCertificate
        f"mssql+pyodbc://{db_username}:{db_password}@{db_server}/{db_database}?driver={driver_clean.replace(' ', '+')}&TrustServerCertificate=yes",
        # Formato 3: Connection string completa
        f"mssql+pyodbc:///?odbc_connect=DRIVER={{{driver_clean}}};SERVER={db_server};DATABASE={db_database};UID={db_username};PWD={db_password};TrustServerCertificate=yes"
    ]
    
    for i, conn_str in enumerate(connection_strings, 1):
        print(f"\n   Tentativa {i}:")
        try:
            # Ocultar senha na exibição
            display_str = conn_str.replace(db_password, '*****')
            print(f"   URL: {display_str[:80]}...")
            
            engine = create_engine(conn_str, echo=False)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                print(f"   ✓ Conexão estabelecida! Resultado: {result.scalar()}")
                break
        except Exception as e:
            print(f"   ❌ Erro: {str(e)[:100]}...")
    
except ImportError:
    print("   ❌ SQLAlchemy não está instalado")
    print("   Execute: pip install sqlalchemy")
except Exception as e:
    print(f"   ❌ Erro geral: {e}")

# 5. Sugestões de resolução
print("\n" + "="*60)
print("SUGESTÕES DE RESOLUÇÃO:")
print("="*60)

if not any("ODBC Driver 17 for SQL Server" in d for d in drivers):
    print("\n1. Instalar o driver ODBC:")
    print("   ./install_odbc_mac.sh")
    print("\n2. Ou instalar manualmente:")
    print("   brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release")
    print("   HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql17")

print("\n3. Verificar se o servidor aceita conexões remotas:")
print(f"   telnet {db_server} 1433")

print("\n4. Verificar firewall e configurações de rede")
print("\n5. Para macOS com Apple Silicon (M1/M2), pode ser necessário:")
print("   - Instalar Rosetta 2: softwareupdate --install-rosetta")
print("   - Usar versão x86_64 do Python")

print("\n" + "="*60)