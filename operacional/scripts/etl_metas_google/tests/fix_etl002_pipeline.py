#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para corrigir e executar o pipeline ETL-002
"""

import os
import sys
import pyodbc
from pathlib import Path
from dotenv import load_dotenv
import subprocess

# Configuração
BASE_DIR = Path(__file__).resolve().parent.parent
CREDENTIALS_DIR = BASE_DIR / 'credentials'
load_dotenv(CREDENTIALS_DIR / '.env')

def connect_to_database():
    """Conecta ao banco de dados SQL Server"""
    driver = os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server')
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes"
    )
    
    return pyodbc.connect(conn_str)

def clear_bronze_data():
    """Limpa dados do Bronze para reprocessamento"""
    print("\n1. Limpando dados Bronze existentes...")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Verificar quantos registros existem
        cursor.execute("SELECT COUNT(*) FROM bronze.performance_assignments")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"   Encontrados {count} registros no Bronze")
            cursor.execute("TRUNCATE TABLE bronze.performance_assignments")
            conn.commit()
            print("   ✓ Dados Bronze limpos")
        else:
            print("   ✓ Bronze já está vazio")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"   ✗ Erro ao limpar Bronze: {e}")
        return False
    
    return True

def run_etl():
    """Executa o ETL-002"""
    print("\n2. Executando ETL-002...")
    
    try:
        # Mudar para o diretório do ETL
        os.chdir(BASE_DIR)
        
        # Executar o ETL
        result = subprocess.run(
            [sys.executable, 'etl_002_assignments.py'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("   ✓ ETL executado com sucesso")
            return True
        else:
            print("   ✗ Erro no ETL:")
            print(result.stderr)
            return False
            
    except Exception as e:
        print(f"   ✗ Erro ao executar ETL: {e}")
        return False

def check_bronze_data():
    """Verifica dados carregados no Bronze"""
    print("\n3. Verificando dados no Bronze...")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Contar registros
        cursor.execute("SELECT COUNT(*) FROM bronze.performance_assignments")
        count = cursor.fetchone()[0]
        print(f"   Total de registros: {count}")
        
        if count > 0:
            # Mostrar amostra
            cursor.execute("""
                SELECT TOP 5 
                    crm_id,
                    nome_assessor,
                    indicator_code,
                    indicator_type,
                    weight
                FROM bronze.performance_assignments
                ORDER BY load_id DESC
            """)
            
            print("\n   Amostra dos dados:")
            print("   " + "-"*80)
            print(f"   {'CRM ID':<10} {'Nome':<20} {'Indicador':<15} {'Tipo':<10} {'Peso':<10}")
            print("   " + "-"*80)
            
            for row in cursor.fetchall():
                print(f"   {row[0]:<10} {(row[1] or '')[:20]:<20} {row[2]:<15} {row[3]:<10} {row[4]:<10}")
        
        cursor.close()
        conn.close()
        
        return count > 0
        
    except Exception as e:
        print(f"   ✗ Erro ao verificar Bronze: {e}")
        return False

def run_procedure():
    """Executa a procedure Bronze to Silver"""
    print("\n4. Executando procedure Bronze to Silver...")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Executar procedure
        cursor.execute("EXEC bronze.prc_bronze_to_silver_assignments @debug = 1")
        
        # Capturar mensagens
        while cursor.nextset():
            try:
                rows = cursor.fetchall()
                if rows:
                    for row in rows:
                        print(f"   {row}")
            except:
                pass
        
        conn.commit()
        print("   ✓ Procedure executada")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"   ✗ Erro ao executar procedure: {e}")
        return False

def check_silver_data():
    """Verifica dados na Silver"""
    print("\n5. Verificando dados na Silver...")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Contar registros
        cursor.execute("SELECT COUNT(*) FROM silver.performance_assignments")
        count = cursor.fetchone()[0]
        print(f"   Total de registros: {count}")
        
        if count > 0:
            # Mostrar resumo por tipo
            cursor.execute("""
                SELECT 
                    indicator_type,
                    COUNT(*) as total,
                    COUNT(DISTINCT crm_id) as assessores
                FROM silver.performance_assignments
                WHERE is_active = 1
                GROUP BY indicator_type
                ORDER BY indicator_type
            """)
            
            print("\n   Resumo por tipo de indicador:")
            print("   " + "-"*50)
            print(f"   {'Tipo':<15} {'Total':<10} {'Assessores':<10}")
            print("   " + "-"*50)
            
            for row in cursor.fetchall():
                print(f"   {row[0]:<15} {row[1]:<10} {row[2]:<10}")
        
        cursor.close()
        conn.close()
        
        return count > 0
        
    except Exception as e:
        print(f"   ✗ Erro ao verificar Silver: {e}")
        return False

def main():
    """Executa o pipeline completo"""
    print("="*60)
    print("FIX PIPELINE ETL-002 - Performance Assignments")
    print("="*60)
    
    # 1. Limpar Bronze
    if not clear_bronze_data():
        return
    
    # 2. Executar ETL
    if not run_etl():
        print("\n⚠️  ETL falhou. Verificando possível problema de mapeamento...")
        print("\nSe o erro for relacionado a 'nome_assessor':")
        print("1. Renomeie a coluna 'name_assessor' para 'nome_assessor' no Google Sheets")
        print("2. OU adicione este mapeamento no etl_002_assignments.py após linha 289:")
        print("   if 'name_assessor' in self.data.columns:")
        print("       self.data.rename(columns={'name_assessor': 'nome_assessor'}, inplace=True)")
        return
    
    # 3. Verificar Bronze
    if not check_bronze_data():
        print("\n✗ Nenhum dado foi carregado no Bronze")
        return
    
    # 4. Executar procedure
    if not run_procedure():
        return
    
    # 5. Verificar Silver
    if not check_silver_data():
        print("\n✗ Nenhum dado foi processado para Silver")
        return
    
    print("\n" + "="*60)
    print("✓ PIPELINE CONCLUÍDO COM SUCESSO!")
    print("="*60)

if __name__ == '__main__':
    main()