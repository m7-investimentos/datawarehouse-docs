#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Análise consolidada de todos os dados do ETL
Substitui múltiplos scripts de análise individuais
"""

from test_utilities import *
import argparse

class ComprehensiveAnalyzer:
    """Análise completa do pipeline ETL"""
    
    def __init__(self):
        self.db = DatabaseConnection()
        self.db.connect()
        self.structure = StructureChecker(self.db)
        self.analyzer = DataAnalyzer(self.db)
        self.validator = DataValidator(self.db)
        self.logger = setup_logging('comprehensive_analyzer')
    
    def analyze_bronze_layer(self):
        """Analisa todas as tabelas Bronze"""
        self.logger.info("\n=== ANÁLISE DA CAMADA BRONZE ===")
        
        bronze_tables = [
            'performance_indicators',
            'performance_assignments', 
            'performance_targets'
        ]
        
        results = {}
        
        for table in bronze_tables:
            if self.structure.check_table_exists('bronze', table):
                self.logger.info(f"\nAnalisando bronze.{table}...")
                
                # Estatísticas básicas
                stats = self.analyzer.analyze_bronze_table(table)
                results[table] = stats
                
                self.logger.info(f"  Total de registros: {stats['total_records']}")
                self.logger.info(f"  Processados: {stats['processed_records']}")
                self.logger.info(f"  Não processados: {stats['unprocessed_records']}")
                self.logger.info(f"  Com erro: {stats['error_records']}")
                self.logger.info(f"  Load IDs distintos: {stats['distinct_load_ids']}")
                
                if stats['date_range'][0]:
                    self.logger.info(f"  Período: {stats['date_range'][0]} a {stats['date_range'][1]}")
                
                # Análise específica por tabela
                if table == 'performance_assignments':
                    self._analyze_assignments_details()
                elif table == 'performance_indicators':
                    self._analyze_indicators_details()
                elif table == 'performance_targets':
                    self._analyze_targets_details()
            else:
                self.logger.warning(f"Tabela bronze.{table} não encontrada!")
                results[table] = None
        
        return results
    
    def _analyze_assignments_details(self):
        """Análise detalhada de assignments"""
        # Verificar indicadores não encontrados
        invalid_indicators = self.validator.validate_indicators_exist()
        if not invalid_indicators.empty:
            self.logger.warning(f"  ⚠️  {len(invalid_indicators)} indicadores não encontrados:")
            for _, row in invalid_indicators.iterrows():
                self.logger.warning(f"    - {row['indicator_code']} (CRM: {row['codigo_assessor_crm']})")
        
        # Verificar pesos
        query = """
            SELECT 
                indicator_type,
                COUNT(*) as count,
                AVG(CAST(weight AS FLOAT)) as avg_weight
            FROM bronze.performance_assignments
            WHERE is_processed = 0
            GROUP BY indicator_type
        """
        type_stats = self.db.execute_dataframe(query)
        
        self.logger.info("  Distribuição por tipo:")
        for _, row in type_stats.iterrows():
            self.logger.info(f"    - {row['indicator_type']}: {row['count']} registros, peso médio: {row['avg_weight']:.2f}%")
    
    def _analyze_indicators_details(self):
        """Análise detalhada de indicadores"""
        query = """
            SELECT 
                category,
                COUNT(*) as count
            FROM bronze.performance_indicators
            WHERE is_processed = 0
            GROUP BY category
        """
        category_stats = self.db.execute_dataframe(query)
        
        self.logger.info("  Distribuição por categoria:")
        for _, row in category_stats.iterrows():
            self.logger.info(f"    - {row['category']}: {row['count']} indicadores")
    
    def _analyze_targets_details(self):
        """Análise detalhada de metas"""
        query = """
            SELECT 
                target_type,
                COUNT(*) as count,
                COUNT(DISTINCT codigo_assessor_crm) as assessores
            FROM bronze.performance_targets
            WHERE is_processed = 0
            GROUP BY target_type
        """
        target_stats = self.db.execute_dataframe(query)
        
        if not target_stats.empty:
            self.logger.info("  Distribuição por tipo de meta:")
            for _, row in target_stats.iterrows():
                self.logger.info(f"    - {row['target_type']}: {row['count']} metas, {row['assessores']} assessores")
    
    def analyze_silver_layer(self):
        """Analisa todas as tabelas Silver"""
        self.logger.info("\n=== ANÁLISE DA CAMADA SILVER ===")
        
        silver_tables = [
            'performance_indicators',
            'performance_assignments',
            'performance_targets'
        ]
        
        results = {}
        
        for table in silver_tables:
            if self.structure.check_table_exists('silver', table):
                self.logger.info(f"\nAnalisando silver.{table}...")
                
                row_count = self.structure.get_table_row_count('silver', table)
                results[table] = {'row_count': row_count}
                
                self.logger.info(f"  Total de registros: {row_count}")
                
                # Análises específicas
                if table == 'performance_assignments' and row_count > 0:
                    # Validar pesos
                    invalid_weights = self.validator.validate_weight_sum()
                    if not invalid_weights.empty:
                        self.logger.warning(f"  ⚠️  {len(invalid_weights)} assessores com peso inválido:")
                        for _, row in invalid_weights.head(5).iterrows():
                            self.logger.warning(f"    - CRM {row['codigo_assessor_crm']}: {row['total_weight']:.2f}%")
                    else:
                        self.logger.info("  ✓ Todos os pesos estão válidos (100%)")
            else:
                self.logger.warning(f"Tabela silver.{table} não encontrada!")
                results[table] = None
        
        return results
    
    def analyze_procedures(self):
        """Analisa procedures de transformação"""
        self.logger.info("\n=== ANÁLISE DE PROCEDURES ===")
        
        procedures = [
            ('bronze', 'prc_bronze_to_silver_indicators'),
            ('bronze', 'prc_bronze_to_silver_assignments'),
            ('bronze', 'prc_bronze_to_silver_targets')
        ]
        
        for schema, proc_name in procedures:
            exists = self.structure.check_procedure_exists(schema, proc_name)
            status = "✓ Existe" if exists else "✗ Não encontrada"
            self.logger.info(f"{schema}.{proc_name}: {status}")
            
            if exists:
                # Verificar última execução (se houver log)
                query = """
                    SELECT TOP 1
                        start_time,
                        end_time,
                        rows_read,
                        rows_inserted,
                        status
                    FROM silver.etl_process_log
                    WHERE process_name = ?
                    ORDER BY start_time DESC
                """
                try:
                    result = self.db.execute_query(query, (proc_name,))
                    if result:
                        last_run = result[0]
                        self.logger.info(f"  Última execução: {last_run[0]}")
                        self.logger.info(f"  Status: {last_run[4]}")
                        self.logger.info(f"  Registros: {last_run[2]} lidos, {last_run[3]} inseridos")
                except:
                    pass
    
    def analyze_data_flow(self):
        """Analisa fluxo de dados Bronze → Silver"""
        self.logger.info("\n=== ANÁLISE DO FLUXO DE DADOS ===")
        
        # Verificar sincronização Bronze/Silver
        flows = [
            ('performance_indicators', 'indicator_code'),
            ('performance_assignments', 'codigo_assessor_crm'),
            ('performance_targets', 'codigo_assessor_crm')
        ]
        
        for table, key_col in flows:
            self.logger.info(f"\n{table}:")
            
            # Total no Bronze
            query = f"SELECT COUNT(DISTINCT {key_col}) FROM bronze.{table}"
            bronze_count = self.db.execute_query(query)[0][0]
            
            # Total no Silver
            if self.structure.check_table_exists('silver', table):
                query = f"SELECT COUNT(DISTINCT {key_col}) FROM silver.{table}"
                silver_count = self.db.execute_query(query)[0][0]
                
                sync_rate = (silver_count / bronze_count * 100) if bronze_count > 0 else 0
                
                self.logger.info(f"  Bronze: {bronze_count} {key_col}s distintos")
                self.logger.info(f"  Silver: {silver_count} {key_col}s distintos")
                self.logger.info(f"  Taxa de sincronização: {sync_rate:.1f}%")
                
                if sync_rate < 100:
                    # Identificar gaps
                    query = f"""
                        SELECT TOP 10 b.{key_col}
                        FROM bronze.{table} b
                        WHERE NOT EXISTS (
                            SELECT 1 FROM silver.{table} s
                            WHERE s.{key_col} = b.{key_col}
                        )
                    """
                    missing = self.db.execute_query(query)
                    if missing:
                        self.logger.warning(f"  Exemplos não sincronizados: {[row[0] for row in missing[:5]]}")
    
    def generate_summary_report(self):
        """Gera relatório resumido"""
        self.logger.info("\n" + "="*60)
        self.logger.info("RELATÓRIO RESUMIDO")
        self.logger.info("="*60)
        
        # Status geral
        bronze_results = self.analyze_bronze_layer()
        silver_results = self.analyze_silver_layer()
        
        # Recomendações
        self.logger.info("\n=== RECOMENDAÇÕES ===")
        
        recommendations = []
        
        # Verificar registros não processados
        for table, stats in bronze_results.items():
            if stats and stats['unprocessed_records'] > 0:
                recommendations.append(f"Processar {stats['unprocessed_records']} registros pendentes em bronze.{table}")
        
        # Verificar erros
        for table, stats in bronze_results.items():
            if stats and stats['error_records'] > 0:
                recommendations.append(f"Investigar {stats['error_records']} registros com erro em bronze.{table}")
        
        if recommendations:
            for i, rec in enumerate(recommendations, 1):
                self.logger.info(f"{i}. {rec}")
        else:
            self.logger.info("✓ Nenhuma ação pendente identificada!")
    
    def close(self):
        """Fecha conexões"""
        self.db.close()

def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description='Análise completa do pipeline ETL')
    parser.add_argument('--bronze', action='store_true', help='Analisar apenas Bronze')
    parser.add_argument('--silver', action='store_true', help='Analisar apenas Silver')
    parser.add_argument('--procedures', action='store_true', help='Analisar apenas procedures')
    parser.add_argument('--flow', action='store_true', help='Analisar fluxo de dados')
    parser.add_argument('--summary', action='store_true', help='Gerar apenas resumo')
    
    args = parser.parse_args()
    
    analyzer = ComprehensiveAnalyzer()
    
    try:
        # Se nenhuma opção específica, executar análise completa
        if not any([args.bronze, args.silver, args.procedures, args.flow, args.summary]):
            analyzer.generate_summary_report()
        else:
            if args.bronze:
                analyzer.analyze_bronze_layer()
            if args.silver:
                analyzer.analyze_silver_layer()
            if args.procedures:
                analyzer.analyze_procedures()
            if args.flow:
                analyzer.analyze_data_flow()
            if args.summary:
                analyzer.generate_summary_report()
    
    finally:
        analyzer.close()

if __name__ == "__main__":
    main()