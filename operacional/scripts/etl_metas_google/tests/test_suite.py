#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Suite de testes automatizados para o pipeline ETL
Executa testes unitários e de integração
"""

import unittest
import sys
from pathlib import Path
from datetime import datetime

# Adicionar diretório pai ao path
sys.path.append(str(Path(__file__).parent.parent))

from test_utilities import *
from etl_001_indicators import process_google_sheets_data as process_indicators
from etl_002_assignments import process_google_sheets_data as process_assignments

class TestDatabaseConnection(unittest.TestCase):
    """Testes de conexão com banco de dados"""
    
    def setUp(self):
        self.db = DatabaseConnection()
    
    def test_connection(self):
        """Testa estabelecimento de conexão"""
        try:
            self.db.connect()
            self.assertIsNotNone(self.db.conn)
            self.db.close()
        except Exception as e:
            self.fail(f"Falha na conexão: {e}")
    
    def test_query_execution(self):
        """Testa execução de query simples"""
        self.db.connect()
        result = self.db.execute_query("SELECT 1 as test")
        self.assertEqual(result[0][0], 1)
        self.db.close()

class TestTableStructures(unittest.TestCase):
    """Testes de estrutura de tabelas"""
    
    @classmethod
    def setUpClass(cls):
        cls.db = DatabaseConnection()
        cls.db.connect()
        cls.checker = StructureChecker(cls.db)
    
    @classmethod
    def tearDownClass(cls):
        cls.db.close()
    
    def test_bronze_tables_exist(self):
        """Verifica se tabelas Bronze existem"""
        tables = ['performance_indicators', 'performance_assignments', 'performance_targets']
        
        for table in tables:
            with self.subTest(table=table):
                exists = self.checker.check_table_exists('bronze', table)
                self.assertTrue(exists, f"Tabela bronze.{table} não existe")
    
    def test_silver_tables_exist(self):
        """Verifica se tabelas Silver existem"""
        tables = ['performance_indicators', 'performance_assignments']
        
        for table in tables:
            with self.subTest(table=table):
                exists = self.checker.check_table_exists('silver', table)
                self.assertTrue(exists, f"Tabela silver.{table} não existe")
    
    def test_required_columns(self):
        """Verifica colunas obrigatórias"""
        # Bronze assignments
        columns = self.checker.get_table_columns('bronze', 'performance_assignments')
        required = ['crm_id', 'indicator_code', 'is_processed', 'load_id']
        
        for col in required:
            with self.subTest(column=col):
                self.assertIn(col, columns['COLUMN_NAME'].values)

class TestDataValidation(unittest.TestCase):
    """Testes de validação de dados"""
    
    @classmethod
    def setUpClass(cls):
        cls.db = DatabaseConnection()
        cls.db.connect()
        cls.validator = DataValidator(cls.db)
    
    @classmethod
    def tearDownClass(cls):
        cls.db.close()
    
    def test_weight_validation(self):
        """Testa validação de pesos"""
        invalid_weights = self.validator.validate_weight_sum()
        
        # Se houver pesos inválidos, verificar se estão sendo detectados
        if not invalid_weights.empty:
            for _, row in invalid_weights.iterrows():
                self.assertNotAlmostEqual(row['total_weight'], 100.0, places=2,
                    msg=f"CRM {row['crm_id']} deveria ter peso inválido")
    
    def test_indicator_validation(self):
        """Testa validação de indicadores"""
        invalid_indicators = self.validator.validate_indicators_exist()
        
        # Verificar se indicadores inválidos têm status correto
        for _, row in invalid_indicators.iterrows():
            self.assertEqual(row['status'], 'NÃO EXISTE')

class TestETLProcess(unittest.TestCase):
    """Testes do processo ETL completo"""
    
    @classmethod
    def setUpClass(cls):
        cls.db = DatabaseConnection()
        cls.db.connect()
    
    @classmethod
    def tearDownClass(cls):
        cls.db.close()
    
    def test_etl_001_config(self):
        """Testa configuração do ETL-001"""
        config_path = Path(__file__).parent.parent / 'config' / 'etl_001_config.json'
        self.assertTrue(config_path.exists(), "Arquivo de configuração ETL-001 não encontrado")
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        self.assertIn('spreadsheet_id', config)
        self.assertIn('field_mappings', config)
        self.assertIn('validation', config)
    
    def test_bronze_processing_flags(self):
        """Testa flags de processamento no Bronze"""
        # Verificar se há registros não processados
        query = """
            SELECT COUNT(*) 
            FROM bronze.performance_assignments 
            WHERE is_processed = 0
        """
        unprocessed = self.db.execute_query(query)[0][0]
        
        # Se houver registros não processados, verificar estrutura
        if unprocessed > 0:
            query = """
                SELECT TOP 1 
                    is_processed,
                    processing_date,
                    processing_status
                FROM bronze.performance_assignments
                WHERE is_processed = 0
            """
            result = self.db.execute_query(query)[0]
            
            self.assertEqual(result[0], 0, "is_processed deve ser 0")
            self.assertIsNone(result[1], "processing_date deve ser NULL")
            self.assertIsNone(result[2], "processing_status deve ser NULL")

class TestProcedures(unittest.TestCase):
    """Testes de procedures"""
    
    @classmethod
    def setUpClass(cls):
        cls.db = DatabaseConnection()
        cls.db.connect()
        cls.checker = StructureChecker(cls.db)
    
    @classmethod
    def tearDownClass(cls):
        cls.db.close()
    
    def test_procedures_exist(self):
        """Verifica se procedures existem"""
        procedures = [
            ('bronze', 'prc_bronze_to_silver_indicators'),
            ('bronze', 'prc_bronze_to_silver_assignments'),
            ('bronze', 'prc_bronze_to_silver_targets')
        ]
        
        for schema, proc_name in procedures:
            with self.subTest(procedure=f"{schema}.{proc_name}"):
                exists = self.checker.check_procedure_exists(schema, proc_name)
                self.assertTrue(exists, f"Procedure {schema}.{proc_name} não existe")
    
    def test_assignments_procedure_version(self):
        """Verifica versão da procedure de assignments"""
        if self.checker.check_procedure_exists('bronze', 'prc_bronze_to_silver_assignments'):
            query = """
                SELECT OBJECT_DEFINITION(OBJECT_ID('bronze.prc_bronze_to_silver_assignments'))
            """
            definition = self.db.execute_query(query)[0][0]
            
            # Verificar se é versão 2.0.0 (corrigida)
            self.assertIn('Versão: 2.0.0', definition,
                "Procedure prc_bronze_to_silver_assignments deve estar na versão 2.0.0")

def run_specific_test(test_name):
    """Executa teste específico"""
    suite = unittest.TestSuite()
    
    if test_name == 'connection':
        suite.addTest(unittest.makeSuite(TestDatabaseConnection))
    elif test_name == 'structure':
        suite.addTest(unittest.makeSuite(TestTableStructures))
    elif test_name == 'validation':
        suite.addTest(unittest.makeSuite(TestDataValidation))
    elif test_name == 'etl':
        suite.addTest(unittest.makeSuite(TestETLProcess))
    elif test_name == 'procedures':
        suite.addTest(unittest.makeSuite(TestProcedures))
    else:
        return False
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()

def run_all_tests():
    """Executa todos os testes"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Adicionar todos os testes
    for test_class in [TestDatabaseConnection, TestTableStructures, 
                       TestDataValidation, TestETLProcess, TestProcedures]:
        suite.addTests(loader.loadTestsFromTestCase(test_class))
    
    # Executar com relatório detalhado
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Gerar resumo
    print("\n" + "="*60)
    print("RESUMO DOS TESTES")
    print("="*60)
    print(f"Testes executados: {result.testsRun}")
    print(f"Sucessos: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Falhas: {len(result.failures)}")
    print(f"Erros: {len(result.errors)}")
    
    if result.failures:
        print("\nFALHAS:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback.split('AssertionError: ')[-1].split('\\n')[0]}")
    
    if result.errors:
        print("\nERROS:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback.split('\\n')[-2]}")
    
    return result.wasSuccessful()

def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Suite de testes do ETL')
    parser.add_argument('--test', choices=['connection', 'structure', 'validation', 'etl', 'procedures'],
                       help='Executar teste específico')
    parser.add_argument('--quick', action='store_true', help='Executar apenas testes rápidos')
    
    args = parser.parse_args()
    
    print(f"=== SUITE DE TESTES ETL ===")
    print(f"Executando em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    if args.test:
        success = run_specific_test(args.test)
    elif args.quick:
        # Testes rápidos apenas
        suite = unittest.TestSuite()
        suite.addTest(TestDatabaseConnection('test_connection'))
        suite.addTest(TestTableStructures('test_bronze_tables_exist'))
        
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        success = result.wasSuccessful()
    else:
        success = run_all_tests()
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()