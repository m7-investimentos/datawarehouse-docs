#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verificação consolidada de dados após ETL
Substitui: verify_data.py, verify_all_data.py, verify_etl002_complete.py, verify_etl003_data.py
"""

from test_utilities import *
import argparse
from tabulate import tabulate

class ETLDataVerifier:
    """Verifica dados carregados pelo ETL"""
    
    def __init__(self):
        self.db = DatabaseConnection()
        self.db.connect()
        self.logger = setup_logging('etl_verifier')
    
    def verify_indicators(self, show_details=False):
        """Verifica dados do ETL-001 (Indicators)"""
        self.logger.info("\n=== VERIFICAÇÃO ETL-001 (INDICATORS) ===")
        
        # Bronze
        query = """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processed,
                COUNT(DISTINCT indicator_code) as unique_codes,
                COUNT(DISTINCT category) as categories
            FROM bronze.performance_indicators
        """
        bronze_stats = self.db.execute_query(query)[0]
        
        self.logger.info(f"\nBronze:")
        self.logger.info(f"  Total: {bronze_stats[0]} registros")
        self.logger.info(f"  Processados: {bronze_stats[1]}")
        self.logger.info(f"  Códigos únicos: {bronze_stats[2]}")
        self.logger.info(f"  Categorias: {bronze_stats[3]}")
        
        # Silver
        query = "SELECT COUNT(*), COUNT(DISTINCT indicator_code) FROM silver.performance_indicators"
        silver_stats = self.db.execute_query(query)[0]
        
        self.logger.info(f"\nSilver:")
        self.logger.info(f"  Total: {silver_stats[0]} registros")
        self.logger.info(f"  Códigos únicos: {silver_stats[1]}")
        
        # Taxa de sincronização
        if bronze_stats[2] > 0:
            sync_rate = (silver_stats[1] / bronze_stats[2]) * 100
            self.logger.info(f"  Taxa de sincronização: {sync_rate:.1f}%")
        
        # Detalhes se solicitado
        if show_details:
            query = """
                SELECT 
                    i.category,
                    COUNT(*) as count,
                    STRING_AGG(i.indicator_code, ', ') as codes
                FROM silver.performance_indicators i
                GROUP BY i.category
                ORDER BY i.category
            """
            details = self.db.execute_dataframe(query)
            
            self.logger.info("\nDetalhes por categoria:")
            print(tabulate(details, headers='keys', tablefmt='grid'))
        
        return bronze_stats[0] > 0 and silver_stats[0] > 0
    
    def verify_assignments(self, show_details=False):
        """Verifica dados do ETL-002 (Assignments)"""
        self.logger.info("\n=== VERIFICAÇÃO ETL-002 (ASSIGNMENTS) ===")
        
        # Bronze
        query = """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processed,
                COUNT(DISTINCT crm_id) as assessors,
                COUNT(DISTINCT indicator_code) as indicators,
                SUM(CASE WHEN processing_status = 'ERROR' THEN 1 ELSE 0 END) as errors
            FROM bronze.performance_assignments
        """
        bronze_stats = self.db.execute_query(query)[0]
        
        self.logger.info(f"\nBronze:")
        self.logger.info(f"  Total: {bronze_stats[0]} registros")
        self.logger.info(f"  Processados: {bronze_stats[1]}")
        self.logger.info(f"  Assessores: {bronze_stats[2]}")
        self.logger.info(f"  Indicadores: {bronze_stats[3]}")
        if bronze_stats[4] > 0:
            self.logger.warning(f"  ⚠️  Erros: {bronze_stats[4]}")
        
        # Silver
        query = """
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT crm_id) as assessors,
                COUNT(DISTINCT indicator_id) as indicators
            FROM silver.performance_assignments
            WHERE is_active = 1
        """
        silver_stats = self.db.execute_query(query)[0]
        
        self.logger.info(f"\nSilver:")
        self.logger.info(f"  Total: {silver_stats[0]} registros ativos")
        self.logger.info(f"  Assessores: {silver_stats[1]}")
        self.logger.info(f"  Indicadores: {silver_stats[2]}")
        
        # Validação de pesos
        validator = DataValidator(self.db)
        invalid_weights = validator.validate_weight_sum()
        
        if invalid_weights.empty:
            self.logger.info(f"  ✓ Todos os pesos estão válidos (100%)")
        else:
            self.logger.warning(f"  ⚠️  {len(invalid_weights)} assessores com peso inválido")
        
        # Detalhes se solicitado
        if show_details:
            # Top assessores por número de indicadores
            query = """
                SELECT TOP 10
                    a.crm_id,
                    p.nome_pessoa,
                    COUNT(DISTINCT a.indicator_id) as num_indicators,
                    SUM(CASE WHEN i.category = 'CARD' THEN a.indicator_weight ELSE 0 END) as total_weight
                FROM silver.performance_assignments a
                LEFT JOIN silver.dim_pessoas p ON a.crm_id = p.crm_id
                INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
                WHERE a.is_active = 1
                GROUP BY a.crm_id, p.nome_pessoa
                ORDER BY COUNT(DISTINCT a.indicator_id) DESC
            """
            details = self.db.execute_dataframe(query)
            
            self.logger.info("\nTop 10 assessores por número de indicadores:")
            print(tabulate(details, headers='keys', tablefmt='grid', floatfmt=".2f"))
            
            # Distribuição por tipo
            query = """
                SELECT 
                    i.category,
                    COUNT(*) as assignments,
                    COUNT(DISTINCT a.crm_id) as assessors
                FROM silver.performance_assignments a
                INNER JOIN silver.performance_indicators i ON a.indicator_id = i.indicator_id
                WHERE a.is_active = 1
                GROUP BY i.category
                ORDER BY COUNT(*) DESC
            """
            type_dist = self.db.execute_dataframe(query)
            
            self.logger.info("\nDistribuição por categoria:")
            print(tabulate(type_dist, headers='keys', tablefmt='grid'))
        
        return bronze_stats[0] > 0 and silver_stats[0] > 0
    
    def verify_targets(self, show_details=False):
        """Verifica dados do ETL-003 (Targets)"""
        self.logger.info("\n=== VERIFICAÇÃO ETL-003 (TARGETS) ===")
        
        # Verificar se tabela existe
        checker = StructureChecker(self.db)
        if not checker.check_table_exists('bronze', 'performance_targets'):
            self.logger.warning("Tabela bronze.performance_targets não existe")
            return False
        
        # Bronze
        query = """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processed,
                COUNT(DISTINCT crm_id) as assessors,
                COUNT(DISTINCT period_start) as periods
            FROM bronze.performance_targets
        """
        
        try:
            bronze_stats = self.db.execute_query(query)[0]
            
            self.logger.info(f"\nBronze:")
            self.logger.info(f"  Total: {bronze_stats[0]} registros")
            self.logger.info(f"  Processados: {bronze_stats[1]}")
            self.logger.info(f"  Assessores: {bronze_stats[2]}")
            self.logger.info(f"  Períodos: {bronze_stats[3]}")
            
            # Silver
            if checker.check_table_exists('silver', 'performance_targets'):
                query = """
                    SELECT 
                        COUNT(*) as total,
                        COUNT(DISTINCT crm_id) as assessors
                    FROM silver.performance_targets
                    WHERE is_active = 1
                """
                silver_stats = self.db.execute_query(query)[0]
                
                self.logger.info(f"\nSilver:")
                self.logger.info(f"  Total: {silver_stats[0]} registros ativos")
                self.logger.info(f"  Assessores: {silver_stats[1]}")
            
            return bronze_stats[0] > 0
            
        except Exception as e:
            self.logger.error(f"Erro ao verificar targets: {e}")
            return False
    
    def verify_data_quality(self):
        """Verifica qualidade geral dos dados"""
        self.logger.info("\n=== VERIFICAÇÃO DE QUALIDADE DE DADOS ===")
        
        analyzer = DataAnalyzer(self.db)
        
        # Verificar NULLs críticos
        critical_checks = [
            ('bronze', 'performance_assignments', 'crm_id'),
            ('bronze', 'performance_assignments', 'indicator_code'),
            ('silver', 'performance_assignments', 'valid_from')
        ]
        
        issues = []
        
        for schema, table, column in critical_checks:
            try:
                quality = analyzer.check_data_quality(schema, table, column)
                
                null_pct = (quality['null_count'][0] / quality['total'][0] * 100) if quality['total'][0] > 0 else 0
                
                if null_pct > 0:
                    issues.append(f"{schema}.{table}.{column}: {null_pct:.1f}% NULL")
                    self.logger.warning(f"  ⚠️  {schema}.{table}.{column}: {quality['null_count'][0]} NULLs ({null_pct:.1f}%)")
                else:
                    self.logger.info(f"  ✓ {schema}.{table}.{column}: Sem NULLs")
            except:
                pass
        
        # Verificar duplicatas
        dup_check = analyzer.find_duplicates('bronze', 'performance_assignments', 
                                           ['crm_id', 'indicator_code', 'valid_from'])
        
        if not dup_check.empty:
            self.logger.warning(f"\n  ⚠️  {len(dup_check)} combinações duplicadas encontradas")
            issues.append(f"{len(dup_check)} duplicatas em assignments")
        else:
            self.logger.info("\n  ✓ Sem duplicatas em assignments")
        
        return len(issues) == 0
    
    def generate_summary(self):
        """Gera resumo da verificação"""
        self.logger.info("\n" + "="*60)
        self.logger.info("RESUMO DA VERIFICAÇÃO")
        self.logger.info("="*60)
        
        # Status de cada ETL
        etl_status = {
            'ETL-001 (Indicators)': self.verify_indicators(),
            'ETL-002 (Assignments)': self.verify_assignments(),
            'ETL-003 (Targets)': self.verify_targets()
        }
        
        # Qualidade de dados
        quality_ok = self.verify_data_quality()
        
        # Resumo
        self.logger.info("\n📊 STATUS DOS ETLs:")
        for etl, status in etl_status.items():
            icon = "✓" if status else "✗"
            self.logger.info(f"  {icon} {etl}")
        
        self.logger.info(f"\n📈 QUALIDADE DE DADOS: {'✓ OK' if quality_ok else '⚠️  Com problemas'}")
        
        # Recomendações
        all_ok = all(etl_status.values()) and quality_ok
        
        if not all_ok:
            self.logger.info("\n💡 RECOMENDAÇÕES:")
            
            if not etl_status['ETL-001 (Indicators)']:
                self.logger.info("  1. Execute o ETL-001 primeiro (indicadores mestres)")
            
            if not etl_status['ETL-002 (Assignments)']:
                self.logger.info("  2. Verifique se ETL-001 foi executado antes do ETL-002")
                self.logger.info("     Execute: python etl_002_assignments.py")
            
            if not quality_ok:
                self.logger.info("  3. Revise os dados de origem para corrigir problemas de qualidade")
        else:
            self.logger.info("\n✨ Todos os dados foram carregados com sucesso!")
    
    def close(self):
        """Fecha conexão"""
        self.db.close()

def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description='Verificação de dados do ETL')
    parser.add_argument('--etl', choices=['001', '002', '003'], 
                       help='Verificar ETL específico')
    parser.add_argument('--details', action='store_true',
                       help='Mostrar detalhes')
    parser.add_argument('--quality', action='store_true',
                       help='Verificar apenas qualidade de dados')
    
    args = parser.parse_args()
    
    verifier = ETLDataVerifier()
    
    try:
        if args.etl:
            if args.etl == '001':
                verifier.verify_indicators(show_details=args.details)
            elif args.etl == '002':
                verifier.verify_assignments(show_details=args.details)
            elif args.etl == '003':
                verifier.verify_targets(show_details=args.details)
        elif args.quality:
            verifier.verify_data_quality()
        else:
            # Verificação completa
            verifier.generate_summary()
    
    finally:
        verifier.close()

if __name__ == "__main__":
    main()