#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para executar a procedure completa
"""

import os
import pyodbc
from pathlib import Path
from dotenv import load_dotenv

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

def main():
    """Executa procedure completa"""
    print("=== EXECUTANDO PROCEDURE COMPLETA ===\n")
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Executar procedure
        print("Executando bronze.prc_bronze_to_silver_assignments...")
        cursor.execute("EXEC bronze.prc_bronze_to_silver_assignments")
        
        # Capturar mensagens
        messages = []
        while True:
            try:
                if cursor.nextset():
                    try:
                        rows = cursor.fetchall()
                        if rows:
                            for row in rows:
                                messages.append(str(row))
                    except:
                        pass
                else:
                    break
            except:
                break
        
        conn.commit()
        print("✓ Procedure executada com sucesso!\n")
        
        # Mostrar mensagens
        if messages:
            print("Mensagens da procedure:")
            for msg in messages:
                print(f"  {msg}")
        
        # Verificar resultados
        print("\nVerificando resultados na Silver:")
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT crm_id) as assessores,
                COUNT(DISTINCT indicator_id) as indicadores,
                SUM(CASE WHEN indicator_weight > 0 THEN 1 ELSE 0 END) as com_peso
            FROM silver.performance_assignments
            WHERE is_active = 1
        """)
        
        result = cursor.fetchone()
        print(f"  Total de registros ativos: {result[0]}")
        print(f"  Assessores: {result[1]}")
        print(f"  Indicadores: {result[2]}")
        print(f"  Registros com peso > 0: {result[3]}")
        
        # Resumo por tipo
        cursor.execute("""
            SELECT 
                i.category,
                COUNT(*) as total,
                COUNT(DISTINCT a.crm_id) as assessores
            FROM silver.performance_assignments a
            INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
            WHERE a.is_active = 1
            GROUP BY i.category
            ORDER BY i.category
        """)
        
        print("\n  Resumo por categoria:")
        for row in cursor.fetchall():
            print(f"    {row[0]}: {row[1]} registros, {row[2]} assessores")
        
        # Verificar Bronze processado
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processados
            FROM bronze.performance_assignments
        """)
        
        result = cursor.fetchone()
        print(f"\n  Bronze: {result[1]} de {result[0]} registros marcados como processados")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"✗ ERRO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()