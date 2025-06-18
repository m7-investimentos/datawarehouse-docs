#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnóstico completo de problemas no ETL
Identifica e sugere correções para problemas comuns
"""

from test_utilities import *
import json

class ETLDiagnostics:
    """Diagnóstico automático de problemas no ETL"""
    
    def __init__(self):
        self.db = DatabaseConnection()
        self.issues = []
        self.suggestions = []
        self.logger = setup_logging('etl_diagnostics')
    
    def run_full_diagnostics(self):
        """Executa diagnóstico completo"""
        self.logger.info("=== DIAGNÓSTICO COMPLETO DO ETL ===\n")
        
        # 1. Verificar conexão
        if not self._check_connection():
            return False
        
        # 2. Verificar estruturas
        self._check_table_structures()
        
        # 3. Verificar procedures
        self._check_procedures()
        
        # 4. Verificar dados
        self._check_data_integrity()
        
        # 5. Verificar configurações
        self._check_configurations()
        
        # 6. Gerar relatório
        self._generate_report()
        
        return len(self.issues) == 0
    
    def _check_connection(self):
        """Verifica conexão com banco de dados"""
        self.logger.info("1. Verificando conexão com banco de dados...")
        
        try:
            self.db.connect()
            
            # Verificar versão
            result = self.db.execute_query("SELECT @@VERSION")[0][0]
            self.logger.info(f"   ✓ Conectado ao SQL Server")
            self.logger.info(f"   Versão: {result[:50]}...")
            
            # Verificar database
            result = self.db.execute_query("SELECT DB_NAME()")[0][0]
            if result != 'M7Medallion':
                self.issues.append(f"Database incorreto: {result} (esperado: M7Medallion)")
                self.suggestions.append("Configure DB_DATABASE=M7Medallion no arquivo .env")
            else:
                self.logger.info(f"   ✓ Database: {result}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"   ✗ Erro de conexão: {e}")
            self.issues.append(f"Erro de conexão: {str(e)}")
            
            # Sugestões baseadas no erro
            if "ODBC Driver" in str(e):
                self.suggestions.append("Instale o ODBC Driver 17 ou 18 for SQL Server")
                self.suggestions.append("Mac: brew install msodbcsql17 ou msodbcsql18")
            elif "Login failed" in str(e):
                self.suggestions.append("Verifique as credenciais no arquivo .env")
            elif "Cannot open server" in str(e):
                self.suggestions.append("Verifique se o servidor está acessível")
                self.suggestions.append("Teste: telnet 172.17.0.10 1433")
            
            return False
    
    def _check_table_structures(self):
        """Verifica estrutura das tabelas"""
        self.logger.info("\n2. Verificando estrutura das tabelas...")
        
        checker = StructureChecker(self.db)
        
        # Tabelas esperadas
        expected_tables = {
            'bronze': [
                'performance_indicators',
                'performance_assignments',
                'performance_targets'
            ],
            'silver': [
                'performance_indicators',
                'performance_assignments',
                'performance_targets',
                'dim_pessoas',
                'dim_estruturas'
            ]
        }
        
        for schema, tables in expected_tables.items():
            self.logger.info(f"\n   Schema {schema}:")
            
            for table in tables:
                if checker.check_table_exists(schema, table):
                    # Verificar colunas críticas
                    columns = checker.get_table_columns(schema, table)
                    self.logger.info(f"   ✓ {table}: {len(columns)} colunas")
                    
                    # Verificações específicas
                    if schema == 'bronze' and table == 'performance_assignments':
                        required_cols = ['codigo_assessor_crm', 'indicator_code', 'is_processed', 'load_id']
                        missing = set(required_cols) - set(columns['COLUMN_NAME'].values)
                        if missing:
                            self.issues.append(f"Colunas faltando em bronze.{table}: {missing}")
                            self.suggestions.append(f"Execute o script de criação da tabela bronze.{table}")
                    
                    # Verificar índices
                    indexes = checker.get_indexes(schema, table)
                    if indexes.empty:
                        self.issues.append(f"Tabela {schema}.{table} sem índices")
                        self.suggestions.append(f"Criar índices para melhorar performance em {schema}.{table}")
                
                else:
                    self.logger.warning(f"   ✗ {table}: NÃO ENCONTRADA")
                    self.issues.append(f"Tabela {schema}.{table} não existe")
                    self.suggestions.append(f"Execute o script QRY-XXX-001-create_{schema}_{table}.sql")
    
    def _check_procedures(self):
        """Verifica procedures de ETL"""
        self.logger.info("\n3. Verificando procedures...")
        
        checker = StructureChecker(self.db)
        
        procedures = [
            ('bronze', 'prc_bronze_to_silver_indicators'),
            ('bronze', 'prc_bronze_to_silver_assignments'),
            ('bronze', 'prc_bronze_to_silver_targets')
        ]
        
        for schema, proc_name in procedures:
            if checker.check_procedure_exists(schema, proc_name):
                self.logger.info(f"   ✓ {schema}.{proc_name}")
                
                # Verificar versão da procedure (se houver comentário de versão)
                try:
                    query = f"""
                        SELECT OBJECT_DEFINITION(OBJECT_ID('{schema}.{proc_name}'))
                    """
                    definition = self.db.execute_query(query)[0][0]
                    
                    if "Versão: 2.0.0" in definition and proc_name == 'prc_bronze_to_silver_assignments':
                        self.logger.info(f"     Versão 2.0.0 (corrigida)")
                    elif proc_name == 'prc_bronze_to_silver_assignments' and "Versão: 2.0.0" not in definition:
                        self.issues.append(f"Procedure {proc_name} usando versão antiga (com bug)")
                        self.suggestions.append("Execute o script QRY-ASS-003 atualizado (v2.0.0)")
                except:
                    pass
            else:
                self.logger.warning(f"   ✗ {schema}.{proc_name}: NÃO ENCONTRADA")
                self.issues.append(f"Procedure {schema}.{proc_name} não existe")
                self.suggestions.append(f"Execute o script de criação da procedure")
    
    def _check_data_integrity(self):
        """Verifica integridade dos dados"""
        self.logger.info("\n4. Verificando integridade dos dados...")
        
        analyzer = DataAnalyzer(self.db)
        validator = DataValidator(self.db)
        
        # Verificar Bronze não processado
        bronze_tables = ['performance_indicators', 'performance_assignments', 'performance_targets']
        
        for table in bronze_tables:
            try:
                stats = analyzer.analyze_bronze_table(table)
                
                if stats['unprocessed_records'] > 0:
                    self.logger.warning(f"   ⚠️  {table}: {stats['unprocessed_records']} registros não processados")
                    self.issues.append(f"{stats['unprocessed_records']} registros não processados em bronze.{table}")
                    self.suggestions.append(f"Execute: EXEC bronze.prc_bronze_to_silver_{table.replace('performance_', '')}")
                
                if stats['error_records'] > 0:
                    self.logger.error(f"   ✗ {table}: {stats['error_records']} registros com erro")
                    self.issues.append(f"{stats['error_records']} registros com erro em bronze.{table}")
                    self.suggestions.append(f"Verifique logs em bronze.{table} WHERE processing_status = 'ERROR'")
            except:
                pass
        
        # Verificar indicadores órfãos
        invalid_indicators = validator.validate_indicators_exist()
        if not invalid_indicators.empty:
            count = len(invalid_indicators)
            self.logger.warning(f"\n   ⚠️  {count} códigos de indicador não encontrados")
            self.issues.append(f"{count} indicadores em assignments não existem na tabela mestre")
            self.suggestions.append("Execute ETL-001 primeiro para carregar indicadores")
            self.suggestions.append("Ou verifique se os códigos estão corretos no Google Sheets")
        
        # Verificar pesos inválidos
        invalid_weights = validator.validate_weight_sum()
        if not invalid_weights.empty:
            count = len(invalid_weights)
            self.logger.warning(f"   ⚠️  {count} assessores com soma de pesos != 100%")
            self.issues.append(f"{count} assessores com soma de pesos CARD diferente de 100%")
            self.suggestions.append("Corrija os pesos no Google Sheets")
            self.suggestions.append("Pesos CARD devem somar exatamente 100% por assessor")
    
    def _check_configurations(self):
        """Verifica configurações do ETL"""
        self.logger.info("\n5. Verificando configurações...")
        
        # Verificar arquivos de configuração
        config_files = [
            'config/etl_001_config.json',
            'config/etl_002_config.json',
            'config/etl_003_config.json'
        ]
        
        base_dir = Path(__file__).parent.parent
        
        for config_file in config_files:
            config_path = base_dir / config_file
            
            if config_path.exists():
                self.logger.info(f"   ✓ {config_file}")
                
                # Verificar conteúdo
                try:
                    with open(config_path, 'r') as f:
                        config = json.load(f)
                    
                    # Verificar spreadsheet_id
                    if 'spreadsheet_id' in config:
                        if len(config['spreadsheet_id']) < 20:
                            self.issues.append(f"spreadsheet_id inválido em {config_file}")
                            self.suggestions.append(f"Verifique o ID da planilha Google em {config_file}")
                    
                    # Verificar mapeamentos
                    if 'field_mappings' not in config:
                        self.issues.append(f"field_mappings ausente em {config_file}")
                        
                except Exception as e:
                    self.issues.append(f"Erro ao ler {config_file}: {e}")
            else:
                self.logger.warning(f"   ✗ {config_file}: NÃO ENCONTRADO")
                self.issues.append(f"Arquivo {config_file} não encontrado")
                self.suggestions.append(f"Verifique se o arquivo de configuração existe")
        
        # Verificar credenciais Google
        creds_path = base_dir / 'credentials' / 'google_sheets_api.json'
        if creds_path.exists():
            self.logger.info("   ✓ credentials/google_sheets_api.json")
        else:
            self.logger.warning("   ✗ credentials/google_sheets_api.json: NÃO ENCONTRADO")
            self.issues.append("Credenciais do Google Sheets não encontradas")
            self.suggestions.append("Configure as credenciais da Service Account do Google")
    
    def _generate_report(self):
        """Gera relatório final"""
        self.logger.info("\n" + "="*60)
        self.logger.info("RELATÓRIO DE DIAGNÓSTICO")
        self.logger.info("="*60)
        
        if not self.issues:
            self.logger.info("\n✓ NENHUM PROBLEMA ENCONTRADO!")
            self.logger.info("O ambiente ETL está configurado corretamente.")
        else:
            self.logger.error(f"\n✗ {len(self.issues)} PROBLEMAS ENCONTRADOS:")
            
            for i, issue in enumerate(self.issues, 1):
                self.logger.error(f"\n{i}. {issue}")
            
            self.logger.info(f"\n\n📋 SUGESTÕES DE CORREÇÃO ({len(self.suggestions)} itens):")
            
            for i, suggestion in enumerate(self.suggestions, 1):
                self.logger.info(f"\n{i}. {suggestion}")
        
        # Próximos passos
        self.logger.info("\n\n🚀 PRÓXIMOS PASSOS:")
        
        if self.issues:
            self.logger.info("1. Corrija os problemas identificados acima")
            self.logger.info("2. Execute novamente este diagnóstico")
            self.logger.info("3. Quando tudo estiver ✓, execute o pipeline ETL")
        else:
            self.logger.info("1. Execute o ETL-001 (Indicators)")
            self.logger.info("2. Execute o ETL-002 (Assignments)")
            self.logger.info("3. Execute o ETL-003 (Targets)")
            self.logger.info("4. Ou use ./run_etl.sh para menu interativo")

def quick_check():
    """Verificação rápida do ambiente"""
    logger = setup_logging('quick_check')
    
    logger.info("=== VERIFICAÇÃO RÁPIDA ===\n")
    
    # Teste de conexão
    if test_database_connection():
        logger.info("✓ Conexão com banco: OK")
    else:
        logger.error("✗ Conexão com banco: FALHOU")
        return False
    
    # Verificar tabelas principais
    db = DatabaseConnection()
    db.connect()
    checker = StructureChecker(db)
    
    critical_tables = [
        ('bronze', 'performance_indicators'),
        ('bronze', 'performance_assignments'),
        ('silver', 'performance_indicators')
    ]
    
    all_ok = True
    for schema, table in critical_tables:
        if checker.check_table_exists(schema, table):
            logger.info(f"✓ {schema}.{table}: OK")
        else:
            logger.error(f"✗ {schema}.{table}: NÃO ENCONTRADA")
            all_ok = False
    
    db.close()
    
    return all_ok

def main():
    """Função principal"""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--quick':
        success = quick_check()
    else:
        diagnostics = ETLDiagnostics()
        success = diagnostics.run_full_diagnostics()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()